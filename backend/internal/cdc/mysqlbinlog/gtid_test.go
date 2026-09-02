package mysqlbinlog

import (
	"encoding/binary"
	"testing"
)

func TestGTIDSetParseAddEncode(t *testing.T) {
	set, err := ParseGTIDSet("24BC7850-2C16-11E6-A073-0242AC110002:1-3:5,aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:7")
	if err != nil {
		t.Fatal(err)
	}
	if err := set.Add("24bc7850-2c16-11e6-a073-0242ac110002:4"); err != nil {
		t.Fatal(err)
	}
	got := set.String()
	want := "24bc7850-2c16-11e6-a073-0242ac110002:1-5,aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:7"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	b, err := set.EncodeSIDBlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 8+2*(16+8)+2*16 {
		t.Fatalf("unexpected sid block size %d", len(b))
	}
	if n := binary.LittleEndian.Uint64(b[:8]); n != 2 {
		t.Fatalf("n_sids=%d", n)
	}
}

func TestParseGTIDEvent(t *testing.T) {
	payload := make([]byte, 25)
	payload[0] = 1
	copy(payload[1:17], []byte{0x24, 0xbc, 0x78, 0x50, 0x2c, 0x16, 0x11, 0xe6, 0xa0, 0x73, 0x02, 0x42, 0xac, 0x11, 0x00, 0x02})
	binary.LittleEndian.PutUint64(payload[17:25], 42)
	ev := &Event{Header: Header{Type: GTIDEvent}, Payload: payload}
	g, err := ParseGTIDEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := g.String(), "24bc7850-2c16-11e6-a073-0242ac110002:42"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestAssemblerCarriesGTID(t *testing.T) {
	a := &Assembler{}
	a.SetFile("mysql-bin.000001")
	sid := []byte{0x24, 0xbc, 0x78, 0x50, 0x2c, 0x16, 0x11, 0xe6, 0xa0, 0x73, 0x02, 0x42, 0xac, 0x11, 0x00, 0x02}
	p := make([]byte, 25)
	copy(p[1:17], sid)
	binary.LittleEndian.PutUint64(p[17:], 9)
	if _, err := a.Push(&Event{Header: Header{Type: GTIDEvent}, Payload: p}); err != nil {
		t.Fatal(err)
	}
	row := &Event{Header: Header{Type: WriteRowsEventV2, LogPos: 100}}
	if _, err := a.Push(row); err != nil {
		t.Fatal(err)
	}
	xidp := make([]byte, 8)
	binary.LittleEndian.PutUint64(xidp, 1)
	tx, err := a.Push(&Event{Header: Header{Type: XIDEvent, LogPos: 120}, Payload: xidp})
	if err != nil {
		t.Fatal(err)
	}
	if tx == nil || tx.GTID != "24bc7850-2c16-11e6-a073-0242ac110002:9" {
		t.Fatalf("unexpected tx %+v", tx)
	}
}
