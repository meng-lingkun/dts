package oracleconnector

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// ttcEncoder/ttcDecoder implement the compact integer, CLR and key/value
// primitives used by Oracle's TTC negotiation/authentication messages.  Keeping
// these primitives inside QMigration avoids coupling Oracle support to a JDBC
// or external migration runtime and gives auth/query codecs a single audited
// byte boundary.
type ttcEncoder struct{ bytes.Buffer }

func (w *ttcEncoder) byte(v byte) { _ = w.WriteByte(v) }
func (w *ttcEncoder) fixedUint(v uint64, size int, big bool) {
	b := make([]byte, size)
	switch size {
	case 2:
		if big {
			binary.BigEndian.PutUint16(b, uint16(v))
		} else {
			binary.LittleEndian.PutUint16(b, uint16(v))
		}
	case 4:
		if big {
			binary.BigEndian.PutUint32(b, uint32(v))
		} else {
			binary.LittleEndian.PutUint32(b, uint32(v))
		}
	case 8:
		if big {
			binary.BigEndian.PutUint64(b, v)
		} else {
			binary.LittleEndian.PutUint64(b, v)
		}
	default:
		panic("unsupported TTC fixed integer size")
	}
	_, _ = w.Write(b)
}
func (w *ttcEncoder) compactUint(v uint64, max int) {
	if v == 0 {
		w.byte(0)
		return
	}
	var full [8]byte
	binary.BigEndian.PutUint64(full[:], v)
	b := bytes.TrimLeft(full[:], "\x00")
	if len(b) > max {
		panic("TTC integer exceeds field width")
	}
	w.byte(byte(len(b)))
	_, _ = w.Write(b)
}
func (w *ttcEncoder) clr(v []byte) {
	if len(v) == 0 {
		w.byte(0)
		return
	}
	if len(v) <= 0xfc {
		w.byte(byte(len(v)))
		_, _ = w.Write(v)
		return
	}
	w.byte(0xfe)
	for len(v) > 0 {
		n := 0x40
		if n > len(v) {
			n = len(v)
		}
		w.byte(byte(n))
		_, _ = w.Write(v[:n])
		v = v[n:]
	}
	w.byte(0)
}
func (w *ttcEncoder) keyVal(key, val []byte, flag uint32) {
	if len(key) == 0 {
		w.byte(0)
	} else {
		w.compactUint(uint64(len(key)), 4)
		w.clr(key)
	}
	if len(val) == 0 {
		w.byte(0)
	} else {
		w.compactUint(uint64(len(val)), 4)
		w.clr(val)
	}
	w.compactUint(uint64(flag), 4)
}

type ttcDecoder struct {
	b   []byte
	off int
}

func newTTCDecoder(b []byte) *ttcDecoder { return &ttcDecoder{b: b} }
func (r *ttcDecoder) remaining() int     { return len(r.b) - r.off }
func (r *ttcDecoder) take(n int) ([]byte, error) {
	if n < 0 || r.remaining() < n {
		return nil, ioErr("truncated TTC payload")
	}
	out := r.b[r.off : r.off+n]
	r.off += n
	return out, nil
}
func (r *ttcDecoder) byte() (byte, error) {
	b, e := r.take(1)
	if e != nil {
		return 0, e
	}
	return b[0], nil
}
func (r *ttcDecoder) fixedUint(size int, big bool) (uint64, error) {
	b, e := r.take(size)
	if e != nil {
		return 0, e
	}
	switch size {
	case 2:
		if big {
			return uint64(binary.BigEndian.Uint16(b)), nil
		}
		return uint64(binary.LittleEndian.Uint16(b)), nil
	case 4:
		if big {
			return uint64(binary.BigEndian.Uint32(b)), nil
		}
		return uint64(binary.LittleEndian.Uint32(b)), nil
	case 8:
		if big {
			return binary.BigEndian.Uint64(b), nil
		}
		return binary.LittleEndian.Uint64(b), nil
	}
	return 0, fmt.Errorf("unsupported TTC fixed integer size %d", size)
}
func (r *ttcDecoder) compactInt(max int) (int64, error) {
	n, e := r.byte()
	if e != nil {
		return 0, e
	}
	if n == 0 {
		return 0, nil
	}
	negative := n&0x80 != 0
	n &= 0x7f
	if int(n) > max || n > 8 {
		return 0, fmt.Errorf("invalid TTC compact integer length %d", n)
	}
	b, e := r.take(int(n))
	if e != nil {
		return 0, e
	}
	var full [8]byte
	copy(full[8-len(b):], b)
	v := binary.BigEndian.Uint64(full[:])
	if v > uint64(^uint64(0)>>1) {
		return 0, errors.New("TTC compact integer overflows int64")
	}
	if negative {
		return -int64(v), nil
	}
	return int64(v), nil
}
func (r *ttcDecoder) compactUint(max int) (uint64, error) {
	n, e := r.byte()
	if e != nil {
		return 0, e
	}
	if n == 0 {
		return 0, nil
	}
	if n&0x80 != 0 {
		return 0, errors.New("negative TTC compact integer is not valid in this field")
	}
	if int(n) > max || n > 8 {
		return 0, fmt.Errorf("invalid TTC compact integer length %d", n)
	}
	b, e := r.take(int(n))
	if e != nil {
		return 0, e
	}
	var full [8]byte
	copy(full[8-len(b):], b)
	return binary.BigEndian.Uint64(full[:]), nil
}
func (r *ttcDecoder) clr() ([]byte, error) {
	n, e := r.byte()
	if e != nil {
		return nil, e
	}
	if n == 0 {
		return nil, nil
	}
	if n != 0xfe {
		return r.take(int(n))
	}
	var out []byte
	for {
		part, e := r.byte()
		if e != nil {
			return nil, e
		}
		if part == 0 {
			break
		}
		b, e := r.take(int(part))
		if e != nil {
			return nil, e
		}
		out = append(out, b...)
	}
	return out, nil
}
func (r *ttcDecoder) keyVal() (key, val []byte, flag uint32, err error) {
	kl, e := r.compactUint(4)
	if e != nil {
		return nil, nil, 0, e
	}
	if kl > 0 {
		key, e = r.clr()
		if e != nil {
			return nil, nil, 0, e
		}
		if uint64(len(key)) != kl {
			return nil, nil, 0, fmt.Errorf("TTC key length mismatch")
		}
	}
	vl, e := r.compactUint(4)
	if e != nil {
		return nil, nil, 0, e
	}
	if vl > 0 {
		val, e = r.clr()
		if e != nil {
			return nil, nil, 0, e
		}
		if uint64(len(val)) != vl {
			return nil, nil, 0, fmt.Errorf("TTC value length mismatch")
		}
	}
	f, e := r.compactUint(4)
	if e != nil {
		return nil, nil, 0, e
	}
	return key, val, uint32(f), nil
}

var errTTCTruncated = errors.New("truncated TTC payload")

func ioErr(s string) error {
	if s == "truncated TTC payload" {
		return errTTCTruncated
	}
	return errors.New(s)
}
