package mysqlbinlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os/exec"
)

const (
	TransactionCompressionZSTD uint64 = 0
	TransactionCompressionNone uint64 = 255
	maxTransactionPayloadBytes        = 128 << 20
	maxNestedEvents                   = 100000
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
			if out.PayloadSize == 0 || out.PayloadSize != uint64(len(p)-off) {
				return nil, fmt.Errorf("transaction payload size %d does not match %d remaining bytes", out.PayloadSize, len(p)-off)
			}
			out.Payload = p[off:]
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

func (p *TransactionPayload) Decompress(ctx context.Context, zstdBin string) ([]byte, error) {
	if p == nil {
		return nil, errors.New("nil transaction payload")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.UncompressedSize > maxTransactionPayloadBytes {
		return nil, fmt.Errorf("transaction payload exceeds %d decoded bytes", maxTransactionPayloadBytes)
	}
	limit := int64(maxTransactionPayloadBytes)
	if p.UncompressedSize > 0 {
		limit = int64(p.UncompressedSize)
	}
	var out []byte
	switch p.CompressionType {
	case TransactionCompressionNone:
		out = p.Payload
	case TransactionCompressionZSTD:
		if zstdBin == "" {
			zstdBin = "zstd"
		}
		path, err := exec.LookPath(zstdBin)
		if err != nil {
			return nil, fmt.Errorf("ZSTD transaction payload requires %q in PATH or DTS_ZSTD_BIN: %w", zstdBin, err)
		}
		decodeCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		cmd := exec.CommandContext(decodeCtx, path, "-q", "-d", "-c")
		cmd.Stdin = bytes.NewReader(p.Payload)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		out, err = io.ReadAll(io.LimitReader(stdout, limit+1))
		if err != nil || int64(len(out)) > limit {
			cancel()
		}
		waitErr := cmd.Wait()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			return nil, err
		}
		if int64(len(out)) <= limit && waitErr != nil {
			return nil, fmt.Errorf("zstd decompress transaction payload: %w", waitErr)
		}
	default:
		return nil, fmt.Errorf("unsupported transaction payload compression type %d", p.CompressionType)
	}
	if int64(len(out)) > limit {
		return nil, fmt.Errorf("transaction payload exceeds %d decoded bytes", limit)
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
		if sz < HeaderSize || sz > len(data)-off {
			return nil, fmt.Errorf("invalid nested binlog event size %d at offset %d", sz, off)
		}
		if data[off+4] == TransactionPayloadEvent || len(out) >= maxNestedEvents {
			return nil, errors.New("nested payload container or too many events in transaction payload")
		}
		out = append(out, data[off:off+sz:off+sz])
		off += sz
	}
	return out, nil
}
