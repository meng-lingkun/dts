package oracleconnector

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const ttcProtocolMessage byte = 1

// ttcProtocolInfo captures the server capabilities needed by later
// authentication and SQL codecs.  It is deliberately narrower than a generic
// Oracle driver: QMigration only records capabilities that affect migration
// correctness, charset decoding or authentication verifier behavior.
type ttcProtocolInfo struct {
	ServerVersion   byte
	ServerString    string
	ServerCharset   uint16
	ServerNCharset  uint16
	ServerFlags     byte
	CompileTimeCaps []byte
	RuntimeCaps     []byte
}

func buildTTCProtocolRequest(clientName string) []byte {
	clientName = strings.TrimSpace(clientName)
	if clientName == "" {
		clientName = "QMigration"
	}
	// TTC protocol negotiation request: message=1, protocol-level=6, flags=0,
	// followed by a NUL-terminated client identification string.
	out := []byte{ttcProtocolMessage, 6, 0}
	out = append(out, []byte(clientName)...)
	out = append(out, 0)
	return out
}

func parseTTCProtocolResponse(payload []byte) (ttcProtocolInfo, error) {
	var out ttcProtocolInfo
	r := newTTCDecoder(payload)
	code, err := r.byte()
	if err != nil {
		return out, err
	}
	if code != ttcProtocolMessage {
		return out, fmt.Errorf("Oracle TTC protocol response code %d, expected %d", code, ttcProtocolMessage)
	}
	if out.ServerVersion, err = r.byte(); err != nil {
		return out, err
	}
	if out.ServerVersion < 4 || out.ServerVersion > 6 {
		return out, fmt.Errorf("unsupported Oracle TTC server protocol version %d", out.ServerVersion)
	}
	if _, err = r.byte(); err != nil {
		return out, err
	} // compatibility byte

	// The server identification is NUL terminated and historically capped at
	// 50 bytes. Be strict about the cap to avoid consuming charset fields when
	// a corrupt response omits the terminator.
	start := r.off
	end := -1
	for i := start; i < len(payload) && i < start+50; i++ {
		if payload[i] == 0 {
			end = i
			break
		}
	}
	if end < 0 {
		return out, errors.New("Oracle TTC protocol server string is not NUL terminated")
	}
	out.ServerString = string(payload[start:end])
	r.off = end + 1
	cs, err := r.fixedUint(2, true)
	if err != nil {
		return out, err
	}
	out.ServerCharset = uint16(cs)
	if out.ServerFlags, err = r.byte(); err != nil {
		return out, err
	}
	elem, err := r.fixedUint(2, true)
	if err != nil {
		return out, err
	}
	if elem > 0 {
		if _, err = r.take(int(elem) * 5); err != nil {
			return out, err
		}
	}
	n1, err := r.fixedUint(2, true)
	if err != nil {
		return out, err
	}
	numArray, err := r.take(int(n1))
	if err != nil {
		return out, err
	}
	if len(numArray) < 7 {
		return out, errors.New("Oracle TTC protocol numeric capability array too short")
	}
	idx := 6 + int(numArray[5]) + int(numArray[6])
	if idx+5 > len(numArray) {
		return out, errors.New("Oracle TTC protocol national charset offset is invalid")
	}
	out.ServerNCharset = binary.BigEndian.Uint16(numArray[idx+3 : idx+5])
	n2, err := r.byte()
	if err != nil {
		return out, err
	}
	out.CompileTimeCaps, err = r.take(int(n2))
	if err != nil {
		return out, err
	}
	out.CompileTimeCaps = append([]byte(nil), out.CompileTimeCaps...)
	n3, err := r.byte()
	if err != nil {
		return out, err
	}
	out.RuntimeCaps, err = r.take(int(n3))
	if err != nil {
		return out, err
	}
	out.RuntimeCaps = append([]byte(nil), out.RuntimeCaps...)
	return out, nil
}

func (c *Connector) negotiateTTCProtocol(ctx context.Context, accepted *acceptedSession) (ttcProtocolInfo, error) {
	if accepted == nil || accepted.Session == nil {
		return ttcProtocolInfo{}, errors.New("Oracle TTC negotiation requires accepted TNS session")
	}
	if err := accepted.Session.WriteData(ctx, 0, buildTTCProtocolRequest("QMigration")); err != nil {
		return ttcProtocolInfo{}, fmt.Errorf("Oracle TTC protocol request: %w", err)
	}
	flags, payload, err := accepted.Session.ReadData(ctx)
	if err != nil {
		return ttcProtocolInfo{}, fmt.Errorf("Oracle TTC protocol response: %w", err)
	}
	if flags != 0 {
		return ttcProtocolInfo{}, fmt.Errorf("Oracle TTC protocol response DATA flags 0x%x unsupported", flags)
	}
	return parseTTCProtocolResponse(payload)
}
