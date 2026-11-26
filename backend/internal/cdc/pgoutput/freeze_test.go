package pgoutput

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestFreezeMessageProducesCommittedEmptyTransaction(t *testing.T) {
	d := NewDecoder()
	var begin bytes.Buffer
	begin.WriteByte('B')
	_ = binary.Write(&begin, binary.BigEndian, uint64(100))
	_ = binary.Write(&begin, binary.BigEndian, uint64(1))
	_ = binary.Write(&begin, binary.BigEndian, uint32(7))
	if _, err := d.Push(wrap(begin.Bytes(), 90, 100)); err != nil {
		t.Fatal(err)
	}
	var message bytes.Buffer
	message.WriteByte('M')
	message.WriteByte(1)
	_ = binary.Write(&message, binary.BigEndian, uint64(95))
	cstr(&message, "dts.freeze")
	_ = binary.Write(&message, binary.BigEndian, uint32(0))
	if tx, err := d.Push(wrap(message.Bytes(), 95, 100)); err != nil || tx != nil {
		t.Fatalf("marker advanced before commit: %+v %v", tx, err)
	}
	var commit bytes.Buffer
	commit.WriteByte('C')
	commit.WriteByte(0)
	for _, n := range []uint64{100, 110, 1} {
		_ = binary.Write(&commit, binary.BigEndian, n)
	}
	tx, err := d.Push(wrap(commit.Bytes(), 100, 110))
	if err != nil || tx == nil || tx.EndLSN != 110 || len(tx.Events) != 0 {
		t.Fatalf("missing committed barrier: %+v %v", tx, err)
	}
}

func TestTruncateCannotBeSilentlyAcknowledged(t *testing.T) {
	if _, err := NewDecoder().Push(wrap([]byte{'T', 0, 0, 0, 1, 0, 0, 0, 0, 7}, 1, 2)); err == nil {
		t.Fatal("unapplied truncate accepted")
	}
}
