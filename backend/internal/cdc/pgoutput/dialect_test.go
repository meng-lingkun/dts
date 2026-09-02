package pgoutput

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestDecoderKingbaseDialectKeepsIndependentPositionIdentity(t *testing.T) {
	d := NewDecoderWithDialect("KINGBASE_LSN", "kingbase")
	var r bytes.Buffer
	r.WriteByte('R')
	binary.Write(&r, binary.BigEndian, uint32(7))
	cstr(&r, "public")
	cstr(&r, "orders")
	r.WriteByte('d')
	binary.Write(&r, binary.BigEndian, uint16(1))
	r.WriteByte(1)
	cstr(&r, "id")
	binary.Write(&r, binary.BigEndian, uint32(20))
	binary.Write(&r, binary.BigEndian, int32(-1))
	if tx, err := d.Push(wrap(r.Bytes(), 1, 2)); err != nil || tx != nil {
		t.Fatalf("relation tx=%v err=%v", tx, err)
	}

	var b bytes.Buffer
	b.WriteByte('B')
	binary.Write(&b, binary.BigEndian, uint64(100))
	binary.Write(&b, binary.BigEndian, uint64(200))
	binary.Write(&b, binary.BigEndian, uint32(9))
	if _, err := d.Push(wrap(b.Bytes(), 2, 3)); err != nil {
		t.Fatal(err)
	}
	var ins bytes.Buffer
	ins.WriteByte('I')
	binary.Write(&ins, binary.BigEndian, uint32(7))
	ins.WriteByte('N')
	ins.Write(tuple("1"))
	if _, err := d.Push(wrap(ins.Bytes(), 3, 4)); err != nil {
		t.Fatal(err)
	}
	var c bytes.Buffer
	c.WriteByte('C')
	c.WriteByte(0)
	binary.Write(&c, binary.BigEndian, uint64(0x100))
	binary.Write(&c, binary.BigEndian, uint64(0x120))
	binary.Write(&c, binary.BigEndian, uint64(300))
	tx, err := d.Push(wrap(c.Bytes(), 4, 5))
	if err != nil {
		t.Fatal(err)
	}
	if tx == nil || len(tx.Events) != 1 {
		t.Fatalf("tx=%+v", tx)
	}
	ev := tx.Events[0]
	if ev.PositionType != "KINGBASE_LSN" || ev.PositionValue != "0/120" || !strings.HasPrefix(ev.ID, "kingbase:") {
		t.Fatalf("dialect identity not preserved: %+v", ev)
	}
}
