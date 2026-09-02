package pgoutput

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func cstr(b *bytes.Buffer, s string) { b.WriteString(s); b.WriteByte(0) }
func wrap(plugin []byte, start, end uint64) []byte {
	p := make([]byte, 25+len(plugin))
	p[0] = 'w'
	binary.BigEndian.PutUint64(p[1:9], start)
	binary.BigEndian.PutUint64(p[9:17], end)
	binary.BigEndian.PutUint64(p[17:25], uint64(time.Second.Microseconds()))
	copy(p[25:], plugin)
	return p
}
func tuple(vals ...string) []byte {
	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, uint16(len(vals)))
	for _, v := range vals {
		b.WriteByte('t')
		binary.Write(&b, binary.BigEndian, uint32(len(v)))
		b.WriteString(v)
	}
	return b.Bytes()
}

func TestDecoderTransaction(t *testing.T) {
	d := NewDecoder()
	var r bytes.Buffer
	r.WriteByte('R')
	binary.Write(&r, binary.BigEndian, uint32(7))
	cstr(&r, "public")
	cstr(&r, "orders")
	r.WriteByte('d')
	binary.Write(&r, binary.BigEndian, uint16(2))
	r.WriteByte(1)
	cstr(&r, "id")
	binary.Write(&r, binary.BigEndian, uint32(20))
	binary.Write(&r, binary.BigEndian, int32(-1))
	r.WriteByte(0)
	cstr(&r, "name")
	binary.Write(&r, binary.BigEndian, uint32(25))
	binary.Write(&r, binary.BigEndian, int32(-1))
	if tx, err := d.Push(wrap(r.Bytes(), 1, 2)); err != nil || tx != nil {
		t.Fatalf("relation tx=%v err=%v", tx, err)
	}
	var b bytes.Buffer
	b.WriteByte('B')
	binary.Write(&b, binary.BigEndian, uint64(100))
	binary.Write(&b, binary.BigEndian, uint64(200))
	binary.Write(&b, binary.BigEndian, uint32(42))
	if _, err := d.Push(wrap(b.Bytes(), 2, 3)); err != nil {
		t.Fatal(err)
	}
	var ins bytes.Buffer
	ins.WriteByte('I')
	binary.Write(&ins, binary.BigEndian, uint32(7))
	ins.WriteByte('N')
	ins.Write(tuple("1", "alice"))
	if _, err := d.Push(wrap(ins.Bytes(), 3, 4)); err != nil {
		t.Fatal(err)
	}
	var upd bytes.Buffer
	upd.WriteByte('U')
	binary.Write(&upd, binary.BigEndian, uint32(7))
	upd.WriteByte('K')
	upd.Write(tuple("1"))
	upd.WriteByte('N')
	upd.Write(tuple("1", "bob"))
	if _, err := d.Push(wrap(upd.Bytes(), 4, 5)); err != nil {
		t.Fatal(err)
	}
	var del bytes.Buffer
	del.WriteByte('D')
	binary.Write(&del, binary.BigEndian, uint32(7))
	del.WriteByte('K')
	del.Write(tuple("1"))
	if _, err := d.Push(wrap(del.Bytes(), 5, 6)); err != nil {
		t.Fatal(err)
	}
	var c bytes.Buffer
	c.WriteByte('C')
	c.WriteByte(0)
	binary.Write(&c, binary.BigEndian, uint64(0x100))
	binary.Write(&c, binary.BigEndian, uint64(0x120))
	binary.Write(&c, binary.BigEndian, uint64(300))
	tx, err := d.Push(wrap(c.Bytes(), 6, 7))
	if err != nil {
		t.Fatal(err)
	}
	if tx == nil || tx.XID != 42 || len(tx.Events) != 3 || tx.Events[0].SourceTable != "orders" || tx.Events[2].Operation != "DELETE" {
		t.Fatalf("bad tx %+v", tx)
	}
	if tx.Events[2].PositionValue != "0/120" {
		t.Fatalf("bad checkpoint %s", tx.Events[2].PositionValue)
	}
}

func TestLSNRoundTrip(t *testing.T) {
	v, err := ParseLSN("16/B374D848")
	if err != nil {
		t.Fatal(err)
	}
	if FormatLSN(v) != "16/B374D848" {
		t.Fatal(FormatLSN(v))
	}
}
