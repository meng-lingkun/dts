package oracleconnector

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	tnsData              = 6
	tnsMarker            = 12
	tnsMaxDataPayloadLen = 0xffff - 10 // 8-byte TNS header + 2-byte DATA flags
)

// tnsDataSession is the protocol transport used after a listener ACCEPT.
// Oracle authentication and TTC SQL messages are intentionally layered above
// this type so the QMigration Connector can reuse the same TNS framing for
// plaintext TCP and TCPS sockets without vendor client libraries.
type tnsDataSession struct {
	conn net.Conn
}

func sendTNSPacket(w io.Writer, typ byte, body []byte) error {
	if len(body)+8 > 0xffff {
		return fmt.Errorf("TNS packet too large: %d", len(body)+8)
	}
	p := make([]byte, 8+len(body))
	binary.BigEndian.PutUint16(p[0:2], uint16(len(p)))
	p[4] = typ
	copy(p[8:], body)
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

func (s *tnsDataSession) WriteData(ctx context.Context, flags uint16, payload []byte) error {
	if s == nil || s.conn == nil {
		return errors.New("Oracle TNS data session is closed")
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = s.conn.SetWriteDeadline(deadline)
	}
	// TTC messages are byte streams and may legitimately exceed a single TNS
	// DATA packet (large array binds / LOB PL/SQL blocks). Oracle clients split
	// the stream at the negotiated SDU boundary; QMigration uses the protocol's
	// absolute 16-bit packet ceiling here because the listener ACCEPT parser does
	// not yet retain an SDU value. The server reassembles TTC across DATA packets.
	if len(payload) == 0 {
		body := make([]byte, 2)
		binary.BigEndian.PutUint16(body, flags)
		return sendTNSPacket(s.conn, tnsData, body)
	}
	for off := 0; off < len(payload); {
		n := len(payload) - off
		if n > tnsMaxDataPayloadLen {
			n = tnsMaxDataPayloadLen
		}
		body := make([]byte, 2+n)
		binary.BigEndian.PutUint16(body[:2], flags)
		copy(body[2:], payload[off:off+n])
		if err := sendTNSPacket(s.conn, tnsData, body); err != nil {
			return err
		}
		off += n
	}
	return nil
}

func (s *tnsDataSession) ReadData(ctx context.Context) (flags uint16, payload []byte, err error) {
	if s == nil || s.conn == nil {
		return 0, nil, errors.New("Oracle TNS data session is closed")
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = s.conn.SetReadDeadline(deadline)
	}
	for {
		typ, body, err := readTNSPacket(s.conn)
		if err != nil {
			return 0, nil, err
		}
		switch typ {
		case tnsData:
			if len(body) < 2 {
				return 0, nil, errors.New("truncated Oracle TNS DATA packet")
			}
			return binary.BigEndian.Uint16(body[:2]), append([]byte(nil), body[2:]...), nil
		case tnsMarker:
			// Marker/resend semantics are connection-stateful. Do not silently
			// ignore them: the future TTC layer must make an explicit decision.
			return 0, nil, fmt.Errorf("Oracle TNS MARKER received during data exchange: %x", body)
		case tnsRefuse:
			return 0, nil, fmt.Errorf("Oracle TNS session refused: %s", printable(body))
		default:
			return 0, nil, fmt.Errorf("unexpected Oracle TNS session packet type %d", typ)
		}
	}
}

func (s *tnsDataSession) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	return err
}
