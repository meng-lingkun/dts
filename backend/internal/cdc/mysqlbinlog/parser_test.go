package mysqlbinlog

import (
	"bytes"
	"encoding/binary"
	"qmigration/backend/internal/domain"
	"testing"
)

func rawEvent(t byte, pos uint32, payload []byte) []byte {
	b := make([]byte, HeaderSize+len(payload))
	binary.LittleEndian.PutUint32(b[0:4], 123)
	b[4] = t
	binary.LittleEndian.PutUint32(b[5:9], 7)
	binary.LittleEndian.PutUint32(b[9:13], uint32(len(b)))
	binary.LittleEndian.PutUint32(b[13:17], pos)
	copy(b[HeaderSize:], payload)
	return b
}

func TestParserRotateTableMapRowsAndTransaction(t *testing.T) {
	parser := Parser{}
	rotPayload := make([]byte, 8+16)
	binary.LittleEndian.PutUint64(rotPayload[:8], 4)
	copy(rotPayload[8:], []byte("mysql-bin.000002"))
	rot, err := parser.Parse(rawEvent(RotateEvent, 0, rotPayload))
	if err != nil {
		t.Fatal(err)
	}
	r, err := ParseRotate(rot)
	if err != nil || r.File != "mysql-bin.000002" {
		t.Fatalf("rotate=%+v err=%v", r, err)
	}

	// table id=9, flags=0, schema app, table orders, 2 columns (LONG, VARCHAR), metadata length 2, null bitmap 1.
	tmPayload := []byte{9, 0, 0, 0, 0, 0, 0, 0, 3, 'a', 'p', 'p', 0, 6, 'o', 'r', 'd', 'e', 'r', 's', 0, 2, 3, 15, 2, 0, 32, 0}
	tmEvent, err := parser.Parse(rawEvent(TableMapEvent, 100, tmPayload))
	if err != nil {
		t.Fatal(err)
	}
	tm, err := ParseTableMap(tmEvent)
	if err != nil {
		t.Fatal(err)
	}
	if tm.TableID != 9 || tm.Schema != "app" || tm.Table != "orders" || len(tm.ColumnTypes) != 2 {
		t.Fatalf("bad table map %+v", tm)
	}

	rowsPayload := []byte{9, 0, 0, 0, 0, 0, 0, 0, 2, 0, 2, 0x03, 0x00, 1, 2, 3}
	rowsEvent, err := parser.Parse(rawEvent(WriteRowsEventV2, 120, rowsPayload))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := ParseRowsV2(rowsEvent)
	if err != nil {
		t.Fatal(err)
	}
	if rows.TableID != 9 || rows.ColumnCount != 2 || len(rows.RowData) != 4 {
		t.Fatalf("bad rows %+v data=%x", rows, rows.RowData)
	}

	// Raw file reader framing.
	var file bytes.Buffer
	file.Write(FileMagic)
	file.Write(rawEvent(RotateEvent, 0, rotPayload))
	rd := NewReader(&file, 0)
	if err := rd.ReadMagic(); err != nil {
		t.Fatal(err)
	}
	if _, err := rd.Next(); err != nil {
		t.Fatal(err)
	}

	a := &Assembler{}
	if _, err := a.Push(rot); err != nil {
		t.Fatal(err)
	}
	beginPayload := make([]byte, 13+1+5)
	// zero-length schema; NUL at offset 13, query begins at 14.
	copy(beginPayload[14:], []byte("BEGIN"))
	begin, _ := parser.Parse(rawEvent(QueryEvent, 90, beginPayload))
	if _, err := a.Push(begin); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Push(rowsEvent); err != nil {
		t.Fatal(err)
	}
	xp := make([]byte, 8)
	binary.LittleEndian.PutUint64(xp, 77)
	xid, _ := parser.Parse(rawEvent(XIDEvent, 140, xp))
	tx, err := a.Push(xid)
	if err != nil {
		t.Fatal(err)
	}
	if tx == nil || tx.XID != 77 || tx.Position() != "mysql-bin.000002:140" || len(tx.Events) != 1 {
		t.Fatalf("bad tx %+v", tx)
	}
}

func TestDecodeRowsCommonValues(t *testing.T) {
	tm := &TableMap{
		TableID:     9,
		Schema:      "app",
		Table:       "orders",
		ColumnTypes: []byte{TypeLongLong, TypeVarchar, TypeNewDecimal},
		// VARCHAR(64) is little endian, DECIMAL(5,2) is precision then scale.
		ColumnMeta: []byte{64, 0, 5, 2},
	}
	cols := []domain.ColumnInfo{
		{Name: "id", DataType: "bigint", ColumnType: "bigint"},
		{Name: "name", DataType: "varchar", ColumnType: "varchar(64)"},
		{Name: "amount", DataType: "decimal", ColumnType: "decimal(5,2)"},
	}
	row := []byte{0x00} // null bitmap for three present columns
	id := make([]byte, 8)
	binary.LittleEndian.PutUint64(id, 42)
	row = append(row, id...)
	row = append(row, 3, 'a', 'b', 'c')
	// Positive DECIMAL(5,2) 123.45: sign bit + two integer bytes + one fraction byte.
	row = append(row, 0x80, 0x7b, 0x2d)
	rows := &Rows{EventType: WriteRowsEventV2, TableID: 9, ColumnCount: 3, BeforeBitmap: []byte{0x07}, RowData: row}
	changes, err := DecodeRows(tm, rows, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || len(changes[0].After) != 3 {
		t.Fatalf("unexpected decoded rows: %+v", changes)
	}
	got := changes[0].After
	if got[0].Value != "42" || got[1].Value != "abc" || got[2].Value != "123.45" {
		t.Fatalf("unexpected values: %+v", got)
	}
}

func TestDecodeRowsUpdateAndNull(t *testing.T) {
	tm := &TableMap{TableID: 3, Schema: "app", Table: "t", ColumnTypes: []byte{TypeLong, TypeVarchar}, ColumnMeta: []byte{32, 0}}
	cols := []domain.ColumnInfo{{Name: "id", DataType: "int", ColumnType: "int"}, {Name: "name", DataType: "varchar", ColumnType: "varchar(32)"}}
	before := []byte{0x00, 1, 0, 0, 0, 3, 'o', 'l', 'd'}
	// After-image has name=NULL: second bit in the null bitmap is set.
	after := []byte{0x02, 1, 0, 0, 0}
	data := append(before, after...)
	rows := &Rows{EventType: UpdateRowsEventV2, TableID: 3, ColumnCount: 2, BeforeBitmap: []byte{0x03}, AfterBitmap: []byte{0x03}, RowData: data, Update: true}
	changes, err := DecodeRows(tm, rows, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Before[1].Value != "old" || !changes[0].After[1].Null {
		t.Fatalf("unexpected update: %+v", changes)
	}
}
