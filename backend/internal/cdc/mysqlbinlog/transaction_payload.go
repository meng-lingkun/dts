package mysqlbinlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os/exec"
)

const (
	TransactionCompressionZSTD uint64 = 0
	TransactionCompressionNone uint64 = 255
)

type TransactionPayload struct {
	CompressionType  uint64 `json:"compression_type"`
	UncompressedSize uint64 `json:"uncompressed_size,omitempty"`
	PayloadSize      uint64 `json:"payload_size"`
	Payload          []byte `json:"-"`
}

// ParseTransactionPayload decodes MySQL's length-encoded TLV header used by
// TRANSACTION_PAYLOAD_EVENT. The payload begins immediately after type=0.
func ParseTransactionPayload(e *Event) (*TransactionPayload, error) {
	if e == nil || e.Header.Type != TransactionPayloadEvent {
		return nil, errors.New("not a TRANSACTION_PAYLOAD_EVENT")
	}
	p := e.Payload
	out := &TransactionPayload{}
	var haveCompression, havePayload bool
	off := 0
	for off < len(p) {
		typ, n, err := readLenEnc(p[off:])
		if err != nil {
			return nil, fmt.Errorf("transaction payload field type: %w", err)
		}
		off += n
		if typ == 0 {
			if !haveCompression || !havePayload {
				return nil, errors.New("transaction payload header misses compression type or payload size")
			}
			if out.PayloadSize > uint64(len(p)-off) {
				return nil, fmt.Errorf("transaction payload size %d exceeds remaining %d bytes", out.PayloadSize, len(p)-off)
			}
			out.Payload = append([]byte(nil), p[off:off+int(out.PayloadSize)]...)
			return out, nil
		}
		ln, n, err := readLenEnc(p[off:])
		if err != nil {
			return nil, fmt.Errorf("transaction payload field length: %w", err)
		}
		off += n
		if ln > uint64(len(p)-off) {
			return nil, errors.New("transaction payload field exceeds header")
		}
		field := p[off : off+int(ln)]
		off += int(ln)
		value, consumed, err := readLenEnc(field)
		if err != nil || consumed != len(field) {
			if err == nil {
				err = fmt.Errorf("encoded integer consumed %d of %d bytes", consumed, len(field))
			}
			return nil, fmt.Errorf("transaction payload field %d: %w", typ, err)
		}
		switch typ {
		case 1:
			out.PayloadSize = value
			havePayload = true
		case 2:
			if value != TransactionCompressionZSTD && value != TransactionCompressionNone {
				return nil, fmt.Errorf("unsupported transaction payload compression type %d", value)
			}
			out.CompressionType = value
			haveCompression = true
		case 3:
			out.UncompressedSize = value
		}
	}
	return nil, errors.New("transaction payload header has no end marker")
}

func (p *TransactionPayload) Decompress(zstdBin string) ([]byte, error) {
	if p == nil {
		return nil, errors.New("nil transaction payload")
	}
	var out []byte
	switch p.CompressionType {
	case TransactionCompressionNone:
		out = append([]byte(nil), p.Payload...)
	case TransactionCompressionZSTD:
		if zstdBin == "" {
			zstdBin = "zstd"
		}
		path, err := exec.LookPath(zstdBin)
		if err != nil {
			return nil, fmt.Errorf("ZSTD transaction payload requires %q in PATH or QMIGRATION_ZSTD_BIN: %w", zstdBin, err)
		}
		cmd := exec.Command(path, "-q", "-d", "-c")
		cmd.Stdin = bytes.NewReader(p.Payload)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err = cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("zstd decompress transaction payload: %w: %s", err, stderr.String())
		}
	default:
		return nil, fmt.Errorf("unsupported transaction payload compression type %d", p.CompressionType)
	}
	if p.UncompressedSize > 0 && uint64(len(out)) != p.UncompressedSize {
		return nil, fmt.Errorf("transaction payload uncompressed size mismatch expected=%d actual=%d", p.UncompressedSize, len(out))
	}
	return out, nil
}

// SplitTransactionEvents splits the uncompressed transaction payload into
// complete binlog events using each nested event's 19-byte header EventSize.
func SplitTransactionEvents(data []byte) ([][]byte, error) {
	var out [][]byte
	for off := 0; off < len(data); {
		if len(data)-off < HeaderSize {
			return nil, fmt.Errorf("transaction payload has %d trailing bytes, smaller than event header", len(data)-off)
		}
		sz := int(binary.LittleEndian.Uint32(data[off+9 : off+13]))
		if sz < HeaderSize || off+sz > len(data) {
			return nil, fmt.Errorf("invalid nested binlog event size %d at offset %d", sz, off)
		}
		out = append(out, append([]byte(nil), data[off:off+sz]...))
		off += sz
	}
	return out, nil
}
