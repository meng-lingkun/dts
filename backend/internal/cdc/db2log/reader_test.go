package db2log

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"

	"qmigration/backend/internal/domain"
)

type fakeAgent struct {
	boot  BootstrapResponse
	reads []*ReadResponse
	i     int
}

func (f *fakeAgent) Position(context.Context) (*PositionResponse, error) {
	return &PositionResponse{Recoverable: true, NextStartLRI: LRI{Type: 1, Part2: 1}, ByteOrder: "little"}, nil
}
func (f *fakeAgent) Bootstrap(context.Context, BootstrapRequest) (*BootstrapResponse, error) {
	return &f.boot, nil
}
func (f *fakeAgent) Read(context.Context, LRI, int, int) (*ReadResponse, error) {
	if f.i >= len(f.reads) {
		return &ReadResponse{ReadToCurrent: true}, nil
	}
	v := f.reads[f.i]
	f.i++
	return v, nil
}

func rawHeader(typ uint16, tid string, payload []byte) []byte {
	b := make([]byte, 40+len(payload))
	binary.LittleEndian.PutUint32(b[0:4], uint32(len(b)))
	binary.LittleEndian.PutUint16(b[4:6], typ)
	x, _ := hex.DecodeString(tid)
	copy(b[32:38], x)
	copy(b[40:], payload)
	return b
}
func envRec(lri, next uint64, typ uint16, tid string, payload []byte) RecordEnvelope {
	return RecordEnvelope{LRI: LRI{Type: 1, Part2: lri}, NextLRI: LRI{Type: 1, Part2: next}, LogType: typ, TID: tid, ByteOrder: "little", RawBase64: base64.StdEncoding.EncodeToString(rawHeader(typ, tid, payload))}
}
func descriptorRecord(tid string) RecordEnvelope {
	desc := make([]byte, 4+16)
	desc[0] = 0
	binary.LittleEndian.PutUint16(desc[2:4], 2)
	binary.LittleEndian.PutUint16(desc[4:6], FieldInteger)
	binary.LittleEndian.PutUint16(desc[6:8], 4)
	binary.LittleEndian.PutUint16(desc[8:10], FieldFlagNoNulls)
	binary.LittleEndian.PutUint16(desc[10:12], 0)
	binary.LittleEndian.PutUint16(desc[12:14], FieldVarchar)
	binary.LittleEndian.PutUint16(desc[14:16], 32)
	binary.LittleEndian.PutUint16(desc[16:18], FieldFlagNoNulls)
	binary.LittleEndian.PutUint16(desc[18:20], 4)
	dms := make([]byte, 92+len(desc))
	dms[0] = 1
	dms[1] = DMSInitializeTable
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	binary.LittleEndian.PutUint32(dms[88:92], uint32(len(desc)))
	copy(dms[92:], desc)
	return envRec(1, 2, LogTypeNormal, tid, dms)
}
func insertRecord(tid string, id int32, name string) RecordEnvelope {
	data := make([]byte, 8+len(name))
	binary.LittleEndian.PutUint32(data[0:4], uint32(id))
	binary.LittleEndian.PutUint16(data[4:6], 8)
	binary.LittleEndian.PutUint16(data[6:8], uint16(len(name)))
	copy(data[8:], name)
	rec := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint16(rec[2:4], uint16(len(rec)))
	copy(rec[4:], data)
	dms := make([]byte, 20+len(rec))
	dms[0] = 1
	dms[1] = DMSInsert
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	copy(dms[20:], rec)
	return envRec(10, 11, LogTypeNormal, tid, dms)
}
func selection() Selection {
	return Selection{Schema: "APP", Table: "T", TablespaceID: 7, TableID: 9, PrimaryKeys: []string{"ID"}, Columns: []domain.ColumnInfo{{Name: "ID", DataType: "integer"}, {Name: "NAME", DataType: "varchar"}}}
}

func TestReaderGroupsCommittedTransactionAndAck(t *testing.T) {
	tid := "010203040506"
	boot := descriptorRecord(tid)
	ins := insertRecord(tid, 42, "alpha")
	commit := envRec(11, 12, LogTypeCommit, tid, nil)
	f := &fakeAgent{boot: BootstrapResponse{Records: []RecordEnvelope{boot}}, reads: []*ReadResponse{{Records: []RecordEnvelope{ins, commit}, NextStartLRI: LRI{Type: 1, Part2: 12}}}}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCInsert {
		t.Fatalf("events=%+v", tx.Events)
	}
	if tx.Events[0].After[0].Value != "42" || tx.Events[0].After[1].Value != "alpha" {
		t.Fatalf("row=%+v", tx.Events[0].After)
	}
	if tx.Checkpoint.PositionValue != "1:0000000000000000:000000000000000c" {
		t.Fatalf("checkpoint=%s", tx.Checkpoint.PositionValue)
	}
	if r.Acknowledged().Part2 != 10 {
		t.Fatalf("source advanced before apply/ack")
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if r.Acknowledged().Part2 != 12 {
		t.Fatalf("ack=%s", r.Acknowledged())
	}
}

func TestReaderAbortDropsTransaction(t *testing.T) {
	tid := "010203040506"
	f := &fakeAgent{boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}}, reads: []*ReadResponse{{Records: []RecordEnvelope{insertRecord(tid, 1, "x"), envRec(11, 12, LogTypeAbort, tid, nil)}, NextStartLRI: LRI{Type: 1, Part2: 12}, ReadToCurrent: true}, {Records: nil, NextStartLRI: LRI{Type: 1, Part2: 12}, ReadToCurrent: true}}}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCCheckpoint {
		t.Fatalf("abort must yield only safe progress checkpoint: %+v", tx.Events)
	}
}

func TestReaderSubtransactionMergesIntoParent(t *testing.T) {
	parent := "010203040506"
	child := "111213141516"
	subPayload, _ := hex.DecodeString(child)
	sub := envRec(9, 10, LogTypeSubtransaction, parent, subPayload)
	ins := insertRecord(child, 7, "sub")
	commit := envRec(11, 12, LogTypeCommit, parent, nil)
	f := &fakeAgent{boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(parent)}}, reads: []*ReadResponse{{Records: []RecordEnvelope{sub, ins, commit}, NextStartLRI: LRI{Type: 1, Part2: 12}}}}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 9}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].After[0].Value != "7" {
		t.Fatalf("subtransaction was not merged: %+v", tx.Events)
	}
}

func TestParserFailsClosedOnValueCompression(t *testing.T) {
	tid := "010203040506"
	e := insertRecord(tid, 1, "x")
	raw, _ := base64.StdEncoding.DecodeString(e.RawBase64)
	raw[40+20] |= 0x04
	e.RawBase64 = base64.StdEncoding.EncodeToString(raw)
	td := &TableDescriptor{Fields: []DescriptorField{{Type: FieldInteger, Length: 4, Flags: FieldFlagNoNulls, Offset: 0}, {Type: FieldVarchar, Length: 32, Flags: FieldFlagNoNulls, Offset: 4}}}
	s := selection()
	if _, err := ParseDataManager(e, &s, td); err == nil {
		t.Fatal("value-compressed row must fail closed")
	}
}

func TestParserRejectsEnvelopeHeaderMismatch(t *testing.T) {
	tid := "010203040506"
	e := insertRecord(tid, 1, "x")
	e.LogType = LogTypeCommit
	if _, err := decodeEnvelopeRaw(e); err == nil || !strings.Contains(err.Error(), "type mismatch") {
		t.Fatalf("type/header mismatch must fail closed, err=%v", err)
	}

	e = insertRecord(tid, 1, "x")
	e.TID = "111213141516"
	if _, err := decodeEnvelopeRaw(e); err == nil || !strings.Contains(err.Error(), "TID mismatch") {
		t.Fatalf("TID/header mismatch must fail closed, err=%v", err)
	}

	e = insertRecord(tid, 1, "x")
	raw, _ := base64.StdEncoding.DecodeString(e.RawBase64)
	binary.LittleEndian.PutUint32(raw[0:4], uint32(len(raw)+1))
	e.RawBase64 = base64.StdEncoding.EncodeToString(raw)
	if _, err := decodeEnvelopeRaw(e); err == nil || !strings.Contains(err.Error(), "length mismatch") {
		t.Fatalf("length/header mismatch must fail closed, err=%v", err)
	}
}

func compressedInnerRow(offsetBase int) ([]byte, *TableDescriptor, []domain.ColumnInfo) {
	td := &TableDescriptor{Fields: []DescriptorField{
		{Type: FieldInteger, Length: 4, Flags: FieldFlagNoNulls},
		{Type: FieldVarchar, Length: 32},
		{Type: FieldInteger, Length: 4, Flags: FieldFlagNoNulls | FieldFlagSystemDefaultCompression},
		{Type: FieldVarchar, Length: 32, Flags: FieldFlagNoNulls},
	}}
	cols := []domain.ColumnInfo{{Name: "ID"}, {Name: "NOTE"}, {Name: "ZERO"}, {Name: "NAME"}}
	data := make([]byte, 0, 11)
	var id [4]byte
	binary.LittleEndian.PutUint32(id[:], 42)
	data = append(data, id[:]...)
	data = append(data, 0x01) // NULL attribute
	data = append(data, 0x80) // compressed system default
	data = append(data, []byte("alpha")...)
	n := len(td.Fields)
	arrBytes := (n + 1) * 2
	inner := make([]byte, 4+arrBytes+len(data))
	inner[0] = 0x02
	binary.LittleEndian.PutUint16(inner[2:4], uint16(n))
	offs := []uint16{uint16(offsetBase), uint16(offsetBase+4) | 0x8000, uint16(offsetBase+5) | 0x8000, uint16(offsetBase + 6), uint16(offsetBase + 11)}
	for i, off := range offs {
		binary.LittleEndian.PutUint16(inner[4+i*2:4+i*2+2], off)
	}
	copy(inner[4+arrBytes:], data)
	return inner, td, cols
}

func TestValueCompressedRowDecodesRegularNullAndSystemDefault(t *testing.T) {
	inner, td, cols := compressedInnerRow(0)
	row, err := decodeValueCompressedRow(inner, td, cols, binary.LittleEndian)
	if err != nil {
		t.Fatal(err)
	}
	if len(row) != 4 || row[0].Value != "42" || !row[1].Null || row[2].Value != "0" || row[3].Value != "alpha" {
		t.Fatalf("compressed row=%+v", row)
	}
}

func TestValueCompressedRowAcceptsFormattedRelativeOffsets(t *testing.T) {
	// Offset array contains n+1 uint16 values, so the formatted-record-relative
	// first data offset is 10 for this four-column row.
	inner, td, cols := compressedInnerRow(10)
	row, err := decodeValueCompressedRow(inner, td, cols, binary.LittleEndian)
	if err != nil {
		t.Fatal(err)
	}
	if row[0].Value != "42" || row[3].Value != "alpha" {
		t.Fatalf("compressed row=%+v", row)
	}
}

func TestValueCompressedRowRejectsPartialImage(t *testing.T) {
	inner, td, cols := compressedInnerRow(0)
	binary.LittleEndian.PutUint16(inner[2:4], 3)
	if _, err := decodeValueCompressedRow(inner, td, cols, binary.LittleEndian); err == nil || !strings.Contains(err.Error(), "partial row") {
		t.Fatalf("partial compressed row must fail closed: %v", err)
	}
}

func TestValueCompressedSystemDefaultRequiresDescriptorFlag(t *testing.T) {
	inner, td, cols := compressedInnerRow(0)
	td.Fields[2].Flags &^= FieldFlagSystemDefaultCompression
	if _, err := decodeValueCompressedRow(inner, td, cols, binary.LittleEndian); err == nil || !strings.Contains(err.Error(), "COMPRESS SYSTEM DEFAULT") {
		t.Fatalf("unqualified compressed default must fail closed: %v", err)
	}
}

func TestValueCompressedOutOfRowDescriptorStillFailsClosed(t *testing.T) {
	td := &TableDescriptor{Fields: []DescriptorField{{Type: FieldVarchar, Length: 200, Flags: FieldFlagNoNulls}}}
	cols := []domain.ColumnInfo{{Name: "V"}}
	arrBytes := 4
	inner := make([]byte, 4+arrBytes+24)
	inner[0] = 0x02
	binary.LittleEndian.PutUint16(inner[2:4], 1)
	binary.LittleEndian.PutUint16(inner[4:6], 0x8000)
	binary.LittleEndian.PutUint16(inner[6:8], 24)
	if _, err := decodeValueCompressedRow(inner, td, cols, binary.LittleEndian); err == nil || !strings.Contains(err.Error(), "out-of-row") {
		t.Fatalf("out-of-row compressed descriptor must fail closed: %v", err)
	}
}

func lobSelection() Selection {
	return Selection{Schema: "APP", Table: "LOB_T", TablespaceID: 7, TableID: 9, PrimaryKeys: []string{"ID"}, Columns: []domain.ColumnInfo{{Name: "ID", DataType: "integer"}, {Name: "PAYLOAD", DataType: "blob"}}}
}

func lobDescriptorRecord(tid string) RecordEnvelope {
	desc := make([]byte, 4+16)
	desc[0] = 0
	binary.LittleEndian.PutUint16(desc[2:4], 2)
	binary.LittleEndian.PutUint16(desc[4:6], FieldInteger)
	binary.LittleEndian.PutUint16(desc[6:8], 4)
	binary.LittleEndian.PutUint16(desc[8:10], FieldFlagNoNulls)
	binary.LittleEndian.PutUint16(desc[10:12], 0)
	binary.LittleEndian.PutUint16(desc[12:14], FieldBlob)
	binary.LittleEndian.PutUint16(desc[14:16], 0)
	binary.LittleEndian.PutUint16(desc[16:18], FieldFlagNoNulls)
	binary.LittleEndian.PutUint16(desc[18:20], 4)
	dms := make([]byte, 92+len(desc))
	dms[0] = 1
	dms[1] = DMSInitializeTable
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	binary.LittleEndian.PutUint32(dms[88:92], uint32(len(desc)))
	copy(dms[92:], desc)
	return envRec(1, 2, LogTypeNormal, tid, dms)
}

func outOfRowStartRecord(tid string, lri, next uint64) RecordEnvelope {
	dms := make([]byte, 6)
	dms[0] = 1
	dms[1] = DMSStartOutOfRow
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func lobAddRecord(tid string, lri, next, offset uint64, col uint16, op byte, data []byte, amountOnly bool) RecordEnvelope {
	body := make([]byte, 32+len(data))
	body[0] = 5
	if amountOnly {
		body[1] = LOBAddAmount
	} else {
		body[1] = LOBAddData
	}
	binary.LittleEndian.PutUint16(body[2:4], 20)
	binary.LittleEndian.PutUint16(body[4:6], 30)
	binary.LittleEndian.PutUint16(body[6:8], 7)
	binary.LittleEndian.PutUint16(body[8:10], 9)
	binary.LittleEndian.PutUint32(body[12:16], uint32(len(data)))
	binary.LittleEndian.PutUint64(body[16:24], offset)
	body[25] = op
	binary.LittleEndian.PutUint16(body[26:28], col)
	copy(body[32:], data)
	return envRec(lri, next, LogTypeNormal, tid, body)
}

func outOfRowBlobInsertRecord(tid string, lri, next uint64, id int32) RecordEnvelope {
	data := make([]byte, 32)
	binary.LittleEndian.PutUint32(data[0:4], uint32(id))
	binary.LittleEndian.PutUint16(data[4:6], 0x8008)
	binary.LittleEndian.PutUint16(data[6:8], 24)
	// The following 24 bytes are the documented varying-data descriptor. Its
	// internal fields are intentionally opaque to QMigration; the LOB manager
	// records carry the replicated bytes.
	rec := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint16(rec[2:4], uint16(len(rec)))
	copy(rec[4:], data)
	dms := make([]byte, 20+len(rec))
	dms[0] = 1
	dms[1] = DMSInsert
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	copy(dms[20:], rec)
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func TestReaderReconstructsChunkedOutOfRowBLOB(t *testing.T) {
	tid := "010203040506"
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{lobDescriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			outOfRowStartRecord(tid, 10, 11),
			lobAddRecord(tid, 11, 12, 0, 1, 1, []byte("hello "), false),
			lobAddRecord(tid, 12, 13, 6, 1, 1, []byte("world"), false),
			outOfRowBlobInsertRecord(tid, 13, 14, 42),
			envRec(14, 15, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 15}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{lobSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 {
		t.Fatalf("events=%+v", tx.Events)
	}
	got := tx.Events[0].After
	if len(got) != 2 || got[0].Value != "42" || got[1].Encoding != "base64" {
		t.Fatalf("after=%+v", got)
	}
	b, err := base64.StdEncoding.DecodeString(got[1].Value)
	if err != nil || string(b) != "hello world" {
		t.Fatalf("BLOB=%q err=%v", b, err)
	}
}

func TestReaderFailsClosedOnNotLoggedOutOfRowBLOB(t *testing.T) {
	tid := "010203040506"
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{lobDescriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			outOfRowStartRecord(tid, 10, 11),
			lobAddRecord(tid, 11, 12, 0, 1, 1, nil, true),
			outOfRowBlobInsertRecord(tid, 12, 13, 1),
		}, NextStartLRI: LRI{Type: 1, Part2: 13}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{lobSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "NOT LOGGED") {
		t.Fatalf("NOT LOGGED LOB must fail closed: %v", err)
	}
}

func TestSplitConsolidatedVarying(t *testing.T) {
	// Header + 3 offsets for a two-column table, then only column 1 has data.
	data := []byte("outside")
	raw := make([]byte, 4+12+len(data))
	raw[0] = 0x12
	total := len(raw)
	raw[1], raw[2], raw[3] = byte(total>>16), byte(total>>8), byte(total)
	binary.LittleEndian.PutUint32(raw[4:8], 0)
	binary.LittleEndian.PutUint32(raw[8:12], 0)
	binary.LittleEndian.PutUint32(raw[12:16], uint32(len(data)))
	copy(raw[16:], data)
	parts, err := splitConsolidatedVarying(raw, 2, binary.LittleEndian)
	if err != nil {
		t.Fatal(err)
	}
	if string(parts[1]) != "outside" || len(parts[0]) != 0 {
		t.Fatalf("parts=%v", parts)
	}
}

func outOfRowBlobRow(id int32) []byte {
	data := make([]byte, 32)
	binary.LittleEndian.PutUint32(data[0:4], uint32(id))
	binary.LittleEndian.PutUint16(data[4:6], 0x8008)
	binary.LittleEndian.PutUint16(data[6:8], 24)
	rec := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint16(rec[2:4], uint16(len(rec)))
	copy(rec[4:], data)
	return rec
}

func outOfRowBlobUpdateRecord(tid string, lri, next uint64, oldID, newID int32) RecordEnvelope {
	oldRec, newRec := outOfRowBlobRow(oldID), outOfRowBlobRow(newID)
	second := make([]byte, 20+len(newRec))
	second[0] = 1
	second[1] = DMSUpdate
	binary.LittleEndian.PutUint16(second[2:4], 7)
	binary.LittleEndian.PutUint16(second[4:6], 9)
	copy(second[20:], newRec)
	dms := make([]byte, 20+len(oldRec)+len(second))
	dms[0] = 1
	dms[1] = DMSUpdate
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	copy(dms[20:], oldRec)
	copy(dms[20+len(oldRec):], second)
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func TestReaderReconstructsOutOfRowBLOBUpdateAfterImage(t *testing.T) {
	tid := "010203040506"
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{lobDescriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			outOfRowStartRecord(tid, 10, 11),
			lobAddRecord(tid, 11, 12, 0, 1, 4, []byte("new-value"), false),
			outOfRowBlobUpdateRecord(tid, 12, 13, 42, 42),
			envRec(13, 14, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 14}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{lobSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCUpdate {
		t.Fatalf("events=%+v", tx.Events)
	}
	if len(tx.Events[0].Before) != 1 || tx.Events[0].Before[0].Column != "ID" {
		t.Fatalf("before should omit unresolved non-key LOB: %+v", tx.Events[0].Before)
	}
	if len(tx.Events[0].After) != 2 {
		t.Fatalf("after=%+v", tx.Events[0].After)
	}
	b, err := base64.StdEncoding.DecodeString(tx.Events[0].After[1].Value)
	if err != nil || string(b) != "new-value" {
		t.Fatalf("after BLOB=%q err=%v", b, err)
	}
}

func consolidatedLOBPayload(columns int, values map[int][]byte) []byte {
	offs := make([]uint32, columns+1)
	var data []byte
	for i := 0; i < columns; i++ {
		offs[i] = uint32(len(data))
		data = append(data, values[i]...)
	}
	offs[columns] = uint32(len(data))
	raw := make([]byte, 4+(columns+1)*4+len(data))
	raw[0] = 0x12
	total := len(raw)
	raw[1], raw[2], raw[3] = byte(total>>16), byte(total>>8), byte(total)
	for i, off := range offs {
		binary.LittleEndian.PutUint32(raw[4+i*4:8+i*4], off)
	}
	copy(raw[4+(columns+1)*4:], data)
	return raw
}

func varcharOutOfRowSelection() Selection {
	return Selection{Schema: "APP", Table: "V_T", TablespaceID: 7, TableID: 9, PrimaryKeys: []string{"ID"}, Columns: []domain.ColumnInfo{{Name: "ID", DataType: "integer"}, {Name: "V", DataType: "varchar"}}}
}

func varcharOutOfRowDescriptor(tid string) RecordEnvelope {
	desc := make([]byte, 4+16)
	desc[0] = 0
	binary.LittleEndian.PutUint16(desc[2:4], 2)
	binary.LittleEndian.PutUint16(desc[4:6], FieldInteger)
	binary.LittleEndian.PutUint16(desc[6:8], 4)
	binary.LittleEndian.PutUint16(desc[8:10], FieldFlagNoNulls)
	binary.LittleEndian.PutUint16(desc[10:12], 0)
	binary.LittleEndian.PutUint16(desc[12:14], FieldVarchar)
	binary.LittleEndian.PutUint16(desc[14:16], 32767)
	binary.LittleEndian.PutUint16(desc[16:18], FieldFlagNoNulls)
	binary.LittleEndian.PutUint16(desc[18:20], 4)
	dms := make([]byte, 92+len(desc))
	dms[0], dms[1] = 1, DMSInitializeTable
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	binary.LittleEndian.PutUint32(dms[88:92], uint32(len(desc)))
	copy(dms[92:], desc)
	return envRec(1, 2, LogTypeNormal, tid, dms)
}

func TestReaderReconstructsConsolidatedOutOfRowVarchar(t *testing.T) {
	tid := "010203040506"
	payload := consolidatedLOBPayload(2, map[int][]byte{1: []byte("consolidated-value")})
	f := &fakeAgent{boot: BootstrapResponse{Records: []RecordEnvelope{varcharOutOfRowDescriptor(tid)}}, reads: []*ReadResponse{{Records: []RecordEnvelope{
		outOfRowStartRecord(tid, 10, 11),
		lobAddRecord(tid, 11, 12, 0, LOBColumnConsolidated, 1, payload[:8], false),
		lobAddRecord(tid, 12, 13, 8, LOBColumnConsolidated, 1, payload[8:], false),
		outOfRowBlobInsertRecord(tid, 13, 14, 5),
		envRec(14, 15, LogTypeCommit, tid, nil),
	}, NextStartLRI: LRI{Type: 1, Part2: 15}}}}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{varcharOutOfRowSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := tx.Events[0].After[1].Value; got != "consolidated-value" {
		t.Fatalf("V=%q", got)
	}
}

func xmlSelection() Selection {
	return Selection{Schema: "APP", Table: "XML_T", TablespaceID: 7, TableID: 9, PrimaryKeys: []string{"ID"}, Columns: []domain.ColumnInfo{{Name: "ID", DataType: "integer"}, {Name: "DOC", DataType: "xml", ColumnType: "XML"}}}
}

func xmlDescriptorRecord(tid string) RecordEnvelope {
	desc := make([]byte, 4+16)
	desc[0] = 0
	binary.LittleEndian.PutUint16(desc[2:4], 2)
	binary.LittleEndian.PutUint16(desc[4:6], FieldInteger)
	binary.LittleEndian.PutUint16(desc[6:8], 4)
	binary.LittleEndian.PutUint16(desc[8:10], FieldFlagNoNulls)
	binary.LittleEndian.PutUint16(desc[10:12], 0)
	binary.LittleEndian.PutUint16(desc[12:14], FieldXML)
	binary.LittleEndian.PutUint16(desc[14:16], 0)
	binary.LittleEndian.PutUint16(desc[16:18], FieldFlagNoNulls)
	binary.LittleEndian.PutUint16(desc[18:20], 4)
	dms := make([]byte, 92+len(desc))
	dms[0], dms[1] = 1, DMSInitializeTable
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	binary.LittleEndian.PutUint32(dms[88:92], uint32(len(desc)))
	copy(dms[92:], desc)
	return envRec(1, 2, LogTypeNormal, tid, dms)
}

func xmlSerializedRecord(tid string, lri, next uint64, col uint16, data []byte) RecordEnvelope {
	body := make([]byte, 24+len(data))
	body[0] = CSLComponentXML
	body[1] = CSLXMLSerialized
	binary.LittleEndian.PutUint16(body[2:4], 31)
	binary.LittleEndian.PutUint16(body[4:6], 32)
	binary.LittleEndian.PutUint16(body[6:8], 7)
	binary.LittleEndian.PutUint16(body[8:10], 9)
	body[10] = CSLObjectTypeXML
	binary.LittleEndian.PutUint32(body[12:16], uint32(len(data)))
	binary.LittleEndian.PutUint16(body[16:18], col)
	copy(body[24:], data)
	return envRec(lri, next, LogTypeInformational, tid, body)
}

func TestReaderReconstructsSerializedXMLInsert(t *testing.T) {
	tid := "010203040506"
	f := &fakeAgent{boot: BootstrapResponse{Records: []RecordEnvelope{xmlDescriptorRecord(tid)}}, reads: []*ReadResponse{{Records: []RecordEnvelope{
		outOfRowStartRecord(tid, 10, 11),
		xmlSerializedRecord(tid, 11, 12, 1, []byte("<root><v>")),
		xmlSerializedRecord(tid, 12, 13, 1, []byte("中文</v></root>")),
		outOfRowBlobInsertRecord(tid, 13, 14, 77),
		envRec(14, 15, LogTypeCommit, tid, nil),
	}, NextStartLRI: LRI{Type: 1, Part2: 15}}}}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{xmlSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCInsert {
		t.Fatalf("events=%+v", tx.Events)
	}
	if len(tx.Events[0].After) != 2 || tx.Events[0].After[1].Value != "<root><v>中文</v></root>" || tx.Events[0].After[1].Encoding != "" {
		t.Fatalf("after=%+v", tx.Events[0].After)
	}
}

func TestReaderReconstructsSerializedXMLUpdateAfterImage(t *testing.T) {
	tid := "010203040506"
	f := &fakeAgent{boot: BootstrapResponse{Records: []RecordEnvelope{xmlDescriptorRecord(tid)}}, reads: []*ReadResponse{{Records: []RecordEnvelope{
		outOfRowStartRecord(tid, 20, 21),
		xmlSerializedRecord(tid, 21, 22, 1, []byte("<new>")),
		xmlSerializedRecord(tid, 22, 23, 1, []byte("value</new>")),
		outOfRowBlobUpdateRecord(tid, 23, 24, 88, 88),
		envRec(24, 25, LogTypeCommit, tid, nil),
	}, NextStartLRI: LRI{Type: 1, Part2: 25}}}}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 20}, []Selection{xmlSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCUpdate {
		t.Fatalf("events=%+v", tx.Events)
	}
	if len(tx.Events[0].Before) != 1 || tx.Events[0].Before[0].Column != "ID" {
		t.Fatalf("before=%+v", tx.Events[0].Before)
	}
	if len(tx.Events[0].After) != 2 || tx.Events[0].After[1].Value != "<new>value</new>" {
		t.Fatalf("after=%+v", tx.Events[0].After)
	}
}

func TestReaderFailsClosedOnXMLWithoutStartMarker(t *testing.T) {
	tid := "010203040506"
	f := &fakeAgent{boot: BootstrapResponse{Records: []RecordEnvelope{xmlDescriptorRecord(tid)}}, reads: []*ReadResponse{{Records: []RecordEnvelope{
		xmlSerializedRecord(tid, 30, 31, 1, []byte("<x/>")),
	}, NextStartLRI: LRI{Type: 1, Part2: 31}}}}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 30}, []Selection{xmlSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "without start-of-out-of-row") {
		t.Fatalf("XML without marker must fail closed: %v", err)
	}
}

func TestLOBGroupRejectsConflictingXMLReplay(t *testing.T) {
	g := newLOBGroup(uint32(7)<<16|9, "little")
	r := &XMLManagerRecord{ColumnID: 1, Data: []byte("<a/>"), LRI: LRI{Type: 1, Part2: 10}}
	if err := g.addXML(r); err != nil {
		t.Fatal(err)
	}
	if err := g.addXML(&XMLManagerRecord{ColumnID: 1, Data: []byte("<b/>"), LRI: r.LRI}); err == nil {
		t.Fatal("same XML LRI with different bytes must fail closed")
	}
}

func rawCompensationHeader(tid string, payload []byte) []byte {
	b := make([]byte, 64+len(payload))
	binary.LittleEndian.PutUint32(b[0:4], uint32(len(b)))
	binary.LittleEndian.PutUint16(b[4:6], LogTypeCompensation)
	binary.LittleEndian.PutUint16(b[6:8], LogFlagPropagatable)
	x, _ := hex.DecodeString(tid)
	copy(b[32:38], x)
	copy(b[64:], payload)
	return b
}

func compensationRec(lri, next uint64, tid string, payload []byte) RecordEnvelope {
	return RecordEnvelope{
		LRI:       LRI{Type: 1, Part2: lri},
		NextLRI:   LRI{Type: 1, Part2: next},
		LogType:   LogTypeCompensation,
		Flags:     LogFlagPropagatable,
		TID:       tid,
		ByteOrder: "little",
		RawBase64: base64.StdEncoding.EncodeToString(rawCompensationHeader(tid, payload)),
	}
}

func testRID(page uint32, slot uint16) []byte {
	b := make([]byte, 6)
	binary.LittleEndian.PutUint32(b[0:4], page)
	binary.LittleEndian.PutUint16(b[4:6], slot)
	return b
}

func testRowRecord(id int32, name string) []byte {
	data := make([]byte, 8+len(name))
	binary.LittleEndian.PutUint32(data[0:4], uint32(id))
	binary.LittleEndian.PutUint16(data[4:6], 8)
	binary.LittleEndian.PutUint16(data[6:8], uint16(len(name)))
	copy(data[8:], name)
	rec := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint16(rec[2:4], uint16(len(rec)))
	copy(rec[4:], data)
	return rec
}

func insertRecordWithRID(lri, next uint64, tid string, rid []byte, id int32, name string) RecordEnvelope {
	rec := testRowRecord(id, name)
	dms := make([]byte, 20+len(rec))
	dms[0], dms[1] = 1, DMSInsert
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	copy(dms[12:18], rid)
	copy(dms[20:], rec)
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func multiInsertRecord(lri, next uint64, tid string, rows ...struct {
	rid  []byte
	id   int32
	name string
}) RecordEnvelope {
	parts := make([][]byte, 0, len(rows))
	rowBytes := 0
	variableBytes := 0
	for _, row := range rows {
		rec := testRowRecord(row.id, row.name)
		part := make([]byte, 8+len(rec))
		copy(part[0:6], row.rid)
		copy(part[8:], rec)
		parts = append(parts, part)
		rowBytes += len(rec)
		variableBytes += len(part)
	}
	dms := make([]byte, 20+variableBytes)
	dms[0], dms[1] = 1, DMSMultiInsert
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	binary.LittleEndian.PutUint16(dms[8:10], uint16(len(rows)))
	binary.LittleEndian.PutUint16(dms[12:14], uint16(rowBytes))
	binary.LittleEndian.PutUint16(dms[14:16], uint16(variableBytes))
	pos := 20
	for _, part := range parts {
		copy(dms[pos:], part)
		pos += len(part)
	}
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func undoInsertRecord(lri, next uint64, tid string, rid []byte) RecordEnvelope {
	dms := make([]byte, 20)
	dms[0], dms[1] = 1, DMSUndoInsert
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	copy(dms[12:18], rid)
	return compensationRec(lri, next, tid, dms)
}

func undoMultiInsertRecord(lri, next uint64, tid string, rids ...[]byte) RecordEnvelope {
	dms := make([]byte, 20+8*len(rids))
	dms[0], dms[1] = 1, DMSUndoMultiInsert
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	binary.LittleEndian.PutUint16(dms[8:10], uint16(len(rids)))
	pos := 20
	for _, rid := range rids {
		copy(dms[pos:pos+6], rid)
		pos += 8
	}
	return compensationRec(lri, next, tid, dms)
}

func TestReaderDecodesMultiInsertAsOneCommittedTransaction(t *testing.T) {
	tid := "010203040506"
	rid1, rid2 := testRID(100, 1), testRID(100, 2)
	multi := multiInsertRecord(10, 11, tid,
		struct {
			rid  []byte
			id   int32
			name string
		}{rid1, 1, "one"},
		struct {
			rid  []byte
			id   int32
			name string
		}{rid2, 2, "two"},
	)
	f := &fakeAgent{
		boot:  BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{multi, envRec(11, 12, LogTypeCommit, tid, nil)}, NextStartLRI: LRI{Type: 1, Part2: 12}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 2 {
		t.Fatalf("multi-insert events=%+v", tx.Events)
	}
	if tx.Events[0].Operation != domain.CDCInsert || tx.Events[0].After[0].Value != "1" || tx.Events[0].After[1].Value != "one" {
		t.Fatalf("first row=%+v", tx.Events[0])
	}
	if tx.Events[1].Operation != domain.CDCInsert || tx.Events[1].After[0].Value != "2" || tx.Events[1].After[1].Value != "two" {
		t.Fatalf("second row=%+v", tx.Events[1])
	}
	if tx.Events[0].ID == tx.Events[1].ID {
		t.Fatalf("multi-insert row event IDs must be distinct: %q", tx.Events[0].ID)
	}
}

func TestReaderSavepointCompensationRemovesOnlyRolledBackInsert(t *testing.T) {
	tid := "010203040506"
	rid1, rid2 := testRID(200, 1), testRID(200, 2)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			insertRecordWithRID(10, 11, tid, rid1, 1, "keep"),
			insertRecordWithRID(11, 12, tid, rid2, 2, "rollback"),
			undoInsertRecord(12, 13, tid, rid2),
			envRec(13, 14, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 14}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].After[0].Value != "1" || tx.Events[0].After[1].Value != "keep" {
		t.Fatalf("savepoint rollback net effect=%+v", tx.Events)
	}
}

func TestReaderUndoMultiInsertRemovesAllMatchingRows(t *testing.T) {
	tid := "010203040506"
	rid1, rid2, rid3 := testRID(300, 1), testRID(300, 2), testRID(301, 1)
	multi := multiInsertRecord(10, 11, tid,
		struct {
			rid  []byte
			id   int32
			name string
		}{rid1, 1, "one"},
		struct {
			rid  []byte
			id   int32
			name string
		}{rid2, 2, "two"},
	)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			multi,
			undoMultiInsertRecord(11, 12, tid, rid1, rid2),
			insertRecordWithRID(12, 13, tid, rid3, 3, "survivor"),
			envRec(13, 14, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 14}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].After[0].Value != "3" || tx.Events[0].After[1].Value != "survivor" {
		t.Fatalf("undo-multi-insert net effect=%+v", tx.Events)
	}
}

func TestReaderCompensationFailsClosedWhenRIDCannotBeMatched(t *testing.T) {
	tid := "010203040506"
	rid1, missing := testRID(400, 1), testRID(999, 9)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			insertRecordWithRID(10, 11, tid, rid1, 1, "one"),
			undoInsertRecord(11, 12, tid, missing),
			envRec(12, 13, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 13}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "could not match prior selected-table change") {
		t.Fatalf("unmatched compensation must fail closed: %v", err)
	}
}

func TestParserRejectsTruncatedPropagatableCompensationHeader(t *testing.T) {
	tid := "010203040506"
	raw := rawHeader(LogTypeCompensation, tid, nil)
	binary.LittleEndian.PutUint16(raw[6:8], LogFlagPropagatable)
	e := RecordEnvelope{
		LRI:       LRI{Type: 1, Part2: 10},
		NextLRI:   LRI{Type: 1, Part2: 11},
		LogType:   LogTypeCompensation,
		Flags:     LogFlagPropagatable,
		TID:       tid,
		ByteOrder: "little",
		RawBase64: base64.StdEncoding.EncodeToString(raw),
	}
	if _, err := ParseDataManager(e, nil, nil); err == nil || !strings.Contains(err.Error(), "64-byte manager header") {
		t.Fatalf("truncated propagatable compensation header must fail closed: %v", err)
	}
}

func deleteRecordWithRID(lri, next uint64, tid string, rid []byte, id int32, name string) RecordEnvelope {
	rec := testRowRecord(id, name)
	dms := make([]byte, 20+len(rec))
	dms[0], dms[1] = 1, DMSDelete
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	copy(dms[12:18], rid)
	copy(dms[20:], rec)
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func updateRecordWithRID(lri, next uint64, tid string, rid []byte, oldID int32, oldName string, newID int32, newName string) RecordEnvelope {
	oldRec, newRec := testRowRecord(oldID, oldName), testRowRecord(newID, newName)
	second := make([]byte, 20+len(newRec))
	second[0], second[1] = 1, DMSUpdate
	binary.LittleEndian.PutUint16(second[2:4], 7)
	binary.LittleEndian.PutUint16(second[4:6], 9)
	copy(second[12:18], rid)
	copy(second[20:], newRec)
	dms := make([]byte, 20+len(oldRec)+len(second))
	dms[0], dms[1] = 1, DMSUpdate
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	copy(dms[12:18], rid)
	copy(dms[20:], oldRec)
	copy(dms[20+len(oldRec):], second)
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func undoRowRecord(lri, next uint64, tid string, function byte, rid []byte, id int32, name string) RecordEnvelope {
	rec := testRowRecord(id, name)
	dms := make([]byte, 20+len(rec))
	dms[0], dms[1] = 1, function
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	copy(dms[12:18], rid)
	copy(dms[20:], rec)
	return compensationRec(lri, next, tid, dms)
}

func TestReaderRollbackDeleteCompensationCancelsDelete(t *testing.T) {
	tid := "010203040506"
	ridDelete, ridKeep := testRID(500, 1), testRID(501, 1)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			deleteRecordWithRID(10, 11, tid, ridDelete, 10, "existing"),
			undoRowRecord(11, 12, tid, DMSUndoDelete, ridDelete, 10, "existing"),
			insertRecordWithRID(12, 13, tid, ridKeep, 11, "survivor"),
			envRec(13, 14, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 14}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCInsert || tx.Events[0].After[0].Value != "11" {
		t.Fatalf("rollback-delete net effect=%+v", tx.Events)
	}
}

func TestReaderRollbackUpdateCompensationCancelsUpdate(t *testing.T) {
	tid := "010203040506"
	ridUpdate, ridKeep := testRID(600, 1), testRID(601, 1)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			updateRecordWithRID(10, 11, tid, ridUpdate, 20, "before", 20, "after"),
			undoRowRecord(11, 12, tid, DMSUndoUpdate, ridUpdate, 20, "before"),
			insertRecordWithRID(12, 13, tid, ridKeep, 21, "survivor"),
			envRec(13, 14, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 14}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCInsert || tx.Events[0].After[0].Value != "21" {
		t.Fatalf("rollback-update net effect=%+v", tx.Events)
	}
}

func TestParserFailsClosedOnIncompleteOuterUpdateImage(t *testing.T) {
	tid := "010203040506"
	e := insertRecord(tid, 1, "x")
	raw, _ := base64.StdEncoding.DecodeString(e.RawBase64)
	// The DMS row starts after the 40-byte log manager header + 20-byte DMS
	// insert header. Outer 0x02 is an incomplete/indirect image in documented
	// update relocation paths and must never be decoded as a complete row.
	raw[60] = 0x02
	e.RawBase64 = base64.StdEncoding.EncodeToString(raw)
	td := &TableDescriptor{Fields: []DescriptorField{{Type: FieldInteger, Length: 4, Flags: FieldFlagNoNulls, Offset: 0}, {Type: FieldVarchar, Length: 32, Flags: FieldFlagNoNulls, Offset: 4}}}
	s := selection()
	if _, err := ParseDataManager(e, &s, td); err == nil || !strings.Contains(err.Error(), "not a complete user image") {
		t.Fatalf("incomplete outer row image must fail closed: %v", err)
	}
}

func completeOuterRowRecord(id int32, name string) []byte {
	base := testRowRecord(id, name)
	payload := append([]byte(nil), base[4:]...)
	inner := make([]byte, 4+len(payload))
	inner[0] = 0x01
	copy(inner[4:], payload)
	rec := make([]byte, 4+len(inner))
	rec[0] = 0x04
	binary.LittleEndian.PutUint16(rec[2:4], uint16(len(rec)))
	copy(rec[4:], inner)
	return rec
}

func completeInsertRecordWithRID(lri, next uint64, tid string, rid []byte, id int32, name string) RecordEnvelope {
	rec := completeOuterRowRecord(id, name)
	dms := make([]byte, 20+len(rec))
	dms[0], dms[1] = 1, DMSInsert
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	copy(dms[12:18], rid)
	copy(dms[20:], rec)
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func indirectUpdateRecordWithRID(lri, next uint64, tid string, oldRID, newRID []byte, oldID int32, oldName string) RecordEnvelope {
	oldRec := testRowRecord(oldID, oldName)
	indirect := make([]byte, 4)
	indirect[0] = 0x02
	binary.LittleEndian.PutUint16(indirect[2:4], uint16(len(indirect)))
	second := make([]byte, 20+len(indirect))
	second[0], second[1] = 1, DMSUpdate
	binary.LittleEndian.PutUint16(second[2:4], 7)
	binary.LittleEndian.PutUint16(second[4:6], 9)
	copy(second[12:18], newRID)
	copy(second[20:], indirect)
	dms := make([]byte, 20+len(oldRec)+len(second))
	dms[0], dms[1] = 1, DMSUpdate
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	copy(dms[12:18], oldRID)
	copy(dms[20:], oldRec)
	copy(dms[20+len(oldRec):], second)
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func decomposedDeleteRecord(lri, next uint64, tid string, rid []byte, id int32, name string) RecordEnvelope {
	e := deleteRecordWithRID(lri, next, tid, rid, id, name)
	raw, _ := base64.StdEncoding.DecodeString(e.RawBase64)
	binary.LittleEndian.PutUint16(raw[40+6:40+8], IUDFlagDecomposedUpdate)
	e.RawBase64 = base64.StdEncoding.EncodeToString(raw)
	return e
}

func decomposedInsertRecord(lri, next uint64, tid string, rid []byte, id int32, name string) RecordEnvelope {
	e := insertRecordWithRID(lri, next, tid, rid, id, name)
	raw, _ := base64.StdEncoding.DecodeString(e.RawBase64)
	binary.LittleEndian.PutUint16(raw[40+6:40+8], IUDFlagDecomposedUpdate)
	e.RawBase64 = base64.StdEncoding.EncodeToString(raw)
	return e
}

func TestReaderLinksIndirectUpdateToPrecedingInsertAfterImage(t *testing.T) {
	tid := "010203040506"
	oldRID, newRID := testRID(700, 1), testRID(701, 2)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			completeInsertRecordWithRID(10, 11, tid, newRID, 42, "after"),
			indirectUpdateRecordWithRID(11, 12, tid, oldRID, newRID, 42, "before"),
			envRec(12, 13, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 13}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCUpdate {
		t.Fatalf("indirect update events=%+v", tx.Events)
	}
	if tx.Events[0].Before[1].Value != "before" || tx.Events[0].After[1].Value != "after" {
		t.Fatalf("indirect update before/after=%+v/%+v", tx.Events[0].Before, tx.Events[0].After)
	}
	if tx.Events[0].ID != "db2:1:0000000000000000:000000000000000b" {
		t.Fatalf("logical update should use update LRI identity, got %s", tx.Events[0].ID)
	}
}

func TestReaderIndirectUpdateFailsClosedWithoutPrecedingInsert(t *testing.T) {
	tid := "010203040506"
	oldRID, newRID := testRID(710, 1), testRID(711, 1)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			indirectUpdateRecordWithRID(10, 11, tid, oldRID, newRID, 1, "before"),
		}, NextStartLRI: LRI{Type: 1, Part2: 11}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "no preceding 0x04 INSERT") {
		t.Fatalf("missing relocation candidate must fail closed: %v", err)
	}
}

func TestReaderOrdinaryCompleteInsertIsNotStolenByExplicitUpdate(t *testing.T) {
	tid := "010203040506"
	insertRID, updateRID := testRID(720, 1), testRID(721, 1)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			completeInsertRecordWithRID(10, 11, tid, insertRID, 1, "insert"),
			updateRecordWithRID(11, 12, tid, updateRID, 2, "before", 2, "after"),
			envRec(12, 13, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 13}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 2 || tx.Events[0].Operation != domain.CDCInsert || tx.Events[1].Operation != domain.CDCUpdate {
		t.Fatalf("complete insert + explicit update events=%+v", tx.Events)
	}
}

func TestReaderCombinesDecomposedDeleteInsertIntoUpdate(t *testing.T) {
	tid := "010203040506"
	oldRID, newRID := testRID(730, 1), testRID(731, 1)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			decomposedDeleteRecord(10, 11, tid, oldRID, 7, "before"),
			decomposedInsertRecord(11, 12, tid, newRID, 7, "after"),
			envRec(12, 13, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 13}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCUpdate {
		t.Fatalf("decomposed update events=%+v", tx.Events)
	}
	if tx.Events[0].Before[1].Value != "before" || tx.Events[0].After[1].Value != "after" {
		t.Fatalf("decomposed update before/after=%+v/%+v", tx.Events[0].Before, tx.Events[0].After)
	}
}

func TestReaderDecomposedUpdateFailsClosedWhenPairInterrupted(t *testing.T) {
	tid := "010203040506"
	oldRID, unrelatedRID := testRID(740, 1), testRID(741, 1)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			decomposedDeleteRecord(10, 11, tid, oldRID, 1, "before"),
			insertRecordWithRID(11, 12, tid, unrelatedRID, 2, "other"),
		}, NextStartLRI: LRI{Type: 1, Part2: 12}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "interrupted an incomplete decomposed-update") {
		t.Fatalf("interrupted decomposed update must fail closed: %v", err)
	}
}

func unselectedInsertRecord(lri, next uint64, tid string) RecordEnvelope {
	dms := make([]byte, 24)
	dms[0], dms[1] = 1, DMSInsert
	binary.LittleEndian.PutUint16(dms[2:4], 8)
	binary.LittleEndian.PutUint16(dms[4:6], 10)
	binary.LittleEndian.PutUint16(dms[22:24], 4)
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func TestReaderIndirectUpdateRejectsCandidateAcrossUnselectedMutation(t *testing.T) {
	tid := "010203040506"
	oldRID, newRID := testRID(750, 1), testRID(751, 1)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			completeInsertRecordWithRID(10, 11, tid, newRID, 9, "after"),
			unselectedInsertRecord(11, 12, tid),
			indirectUpdateRecordWithRID(12, 13, tid, oldRID, newRID, 9, "before"),
		}, NextStartLRI: LRI{Type: 1, Part2: 13}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "no preceding 0x04 INSERT") {
		t.Fatalf("intervening unselected mutation must invalidate relocation candidate: %v", err)
	}
}

func TestReaderRollbackIndirectUpdateCompensationCancelsLinkedUpdate(t *testing.T) {
	tid := "010203040506"
	oldRID, newRID, keepRID := testRID(760, 1), testRID(761, 1), testRID(762, 1)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{descriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			completeInsertRecordWithRID(10, 11, tid, newRID, 12, "after"),
			indirectUpdateRecordWithRID(11, 12, tid, oldRID, newRID, 12, "before"),
			undoRowRecord(12, 13, tid, DMSUndoUpdate, oldRID, 12, "before"),
			insertRecordWithRID(13, 14, tid, keepRID, 13, "keep"),
			envRec(14, 15, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 15}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{selection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCInsert || tx.Events[0].After[1].Value != "keep" {
		t.Fatalf("indirect update compensation net effect=%+v", tx.Events)
	}
}

func blobRowRecord(id int32, payload []byte, external bool) []byte {
	if external {
		return outOfRowBlobRow(id)
	}
	data := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(data[0:4], uint32(id))
	binary.LittleEndian.PutUint16(data[4:6], 8)
	binary.LittleEndian.PutUint16(data[6:8], uint16(len(payload)))
	copy(data[8:], payload)
	rec := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint16(rec[2:4], uint16(len(rec)))
	copy(rec[4:], data)
	return rec
}

func multiInsertBlobRecord(lri, next uint64, tid string, rows ...struct {
	rid      []byte
	id       int32
	payload  []byte
	external bool
}) RecordEnvelope {
	parts := make([][]byte, 0, len(rows))
	rowBytes := 0
	variableBytes := 0
	for _, row := range rows {
		rec := blobRowRecord(row.id, row.payload, row.external)
		part := make([]byte, 8+len(rec))
		copy(part[0:6], row.rid)
		copy(part[8:], rec)
		parts = append(parts, part)
		rowBytes += len(rec)
		variableBytes += len(part)
	}
	dms := make([]byte, 20+variableBytes)
	dms[0], dms[1] = 1, DMSMultiInsert
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	binary.LittleEndian.PutUint16(dms[8:10], uint16(len(rows)))
	binary.LittleEndian.PutUint16(dms[12:14], uint16(rowBytes))
	binary.LittleEndian.PutUint16(dms[14:16], uint16(variableBytes))
	pos := 20
	for _, part := range parts {
		copy(dms[pos:], part)
		pos += len(part)
	}
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func TestReaderReconstructsUnambiguousOutOfRowMultiInsert(t *testing.T) {
	tid := "010203040506"
	rid1, rid2 := testRID(700, 1), testRID(700, 2)
	multi := multiInsertBlobRecord(12, 13, tid,
		struct {
			rid      []byte
			id       int32
			payload  []byte
			external bool
		}{rid1, 1, []byte("plain"), false},
		struct {
			rid      []byte
			id       int32
			payload  []byte
			external bool
		}{rid2, 2, nil, true},
	)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{lobDescriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			outOfRowStartRecord(tid, 10, 11),
			lobAddRecord(tid, 11, 12, 0, 1, 1, []byte("outside"), false),
			multi,
			envRec(13, 14, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 14}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{lobSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 2 {
		t.Fatalf("events=%+v", tx.Events)
	}
	b0, err := base64.StdEncoding.DecodeString(tx.Events[0].After[1].Value)
	if err != nil || string(b0) != "plain" {
		t.Fatalf("plain blob=%q err=%v", b0, err)
	}
	b1, err := base64.StdEncoding.DecodeString(tx.Events[1].After[1].Value)
	if err != nil || string(b1) != "outside" {
		t.Fatalf("external blob=%q err=%v", b1, err)
	}
}

func TestReaderFailsClosedOnAmbiguousOutOfRowMultiInsert(t *testing.T) {
	tid := "010203040506"
	rid1, rid2 := testRID(701, 1), testRID(701, 2)
	multi := multiInsertBlobRecord(12, 13, tid,
		struct {
			rid      []byte
			id       int32
			payload  []byte
			external bool
		}{rid1, 1, nil, true},
		struct {
			rid      []byte
			id       int32
			payload  []byte
			external bool
		}{rid2, 2, nil, true},
	)
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{lobDescriptorRecord(tid)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			outOfRowStartRecord(tid, 10, 11),
			lobAddRecord(tid, 11, 12, 0, 1, 1, []byte("ambiguous"), false),
			multi,
		}, NextStartLRI: LRI{Type: 1, Part2: 13}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{lobSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "2 rows requiring out-of-row data") {
		t.Fatalf("ambiguous multi-insert must fail closed: %v", err)
	}
}

func vectorSelection() Selection {
	return Selection{Schema: "APP", Table: "VECTOR_T", TablespaceID: 7, TableID: 9, PrimaryKeys: []string{"ID"}, Columns: []domain.ColumnInfo{{Name: "ID", DataType: "integer"}, {Name: "EMBEDDING", DataType: "vector", ColumnType: "VECTOR(2,INT8)"}}}
}

func vectorDescriptorRecord(tid string, nullable bool) RecordEnvelope {
	desc := make([]byte, 4+16)
	desc[0] = 0
	binary.LittleEndian.PutUint16(desc[2:4], 2)
	binary.LittleEndian.PutUint16(desc[4:6], FieldInteger)
	binary.LittleEndian.PutUint16(desc[6:8], 4)
	binary.LittleEndian.PutUint16(desc[8:10], FieldFlagNoNulls)
	binary.LittleEndian.PutUint16(desc[10:12], 0)
	binary.LittleEndian.PutUint16(desc[12:14], FieldStruct)
	binary.LittleEndian.PutUint16(desc[14:16], 2)
	flags := uint16(FieldFlagNoNulls)
	if nullable {
		flags = FieldFlagIsNull
	}
	binary.LittleEndian.PutUint16(desc[16:18], flags)
	binary.LittleEndian.PutUint16(desc[18:20], 4)
	dms := make([]byte, 92+len(desc))
	dms[0], dms[1] = 1, DMSInitializeTable
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	binary.LittleEndian.PutUint32(dms[88:92], uint32(len(desc)))
	copy(dms[92:], desc)
	return envRec(1, 2, LogTypeNormal, tid, dms)
}

func vectorDataRecord(tid string, lri, next uint64, col uint16, serialized string) RecordEnvelope {
	b := []byte(serialized)
	dms := make([]byte, 16+len(b))
	dms[0], dms[1] = 1, DMSVectorData
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	binary.LittleEndian.PutUint16(dms[6:8], col)
	binary.LittleEndian.PutUint32(dms[8:12], uint32(len(b)))
	copy(dms[16:], b)
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func vectorInsertRecord(tid string, lri, next uint64, id int32, nullable, isNull bool) RecordEnvelope {
	dataLen := 6
	if nullable {
		dataLen++
	}
	data := make([]byte, dataLen)
	binary.LittleEndian.PutUint32(data[0:4], uint32(id))
	// Two opaque bytes model the internal fixed VECTOR representation. QMigration
	// never consumes these bytes; function 213 supplies the serialized value.
	data[4], data[5] = 0xaa, 0xbb
	if nullable && isNull {
		data[6] = 0x01
	}
	rec := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint16(rec[2:4], uint16(len(rec)))
	copy(rec[4:], data)
	dms := make([]byte, 20+len(rec))
	dms[0], dms[1] = 1, DMSInsert
	binary.LittleEndian.PutUint16(dms[2:4], 7)
	binary.LittleEndian.PutUint16(dms[4:6], 9)
	copy(dms[12:18], testRID(800, uint16(id)))
	copy(dms[20:], rec)
	return envRec(lri, next, LogTypeNormal, tid, dms)
}

func TestReaderReconstructsDB2Vector213Insert(t *testing.T) {
	tid := "010203040506"
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{vectorDescriptorRecord(tid, false)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			outOfRowStartRecord(tid, 10, 11),
			vectorDataRecord(tid, 11, 12, 1, "[1,-2]"),
			vectorInsertRecord(tid, 12, 13, 7, false, false),
			envRec(13, 14, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 14}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{vectorSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || len(tx.Events[0].After) != 2 {
		t.Fatalf("events=%+v", tx.Events)
	}
	if tx.Events[0].After[0].Value != "7" || tx.Events[0].After[1].Value != "[1,-2]" {
		t.Fatalf("after=%+v", tx.Events[0].After)
	}
}

func TestReaderVectorNullDoesNotRequire213Record(t *testing.T) {
	tid := "010203040506"
	f := &fakeAgent{
		boot: BootstrapResponse{Records: []RecordEnvelope{vectorDescriptorRecord(tid, true)}},
		reads: []*ReadResponse{{Records: []RecordEnvelope{
			vectorInsertRecord(tid, 10, 11, 8, true, true),
			envRec(11, 12, LogTypeCommit, tid, nil),
		}, NextStartLRI: LRI{Type: 1, Part2: 12}}},
	}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{vectorSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || len(tx.Events[0].After) != 2 || !tx.Events[0].After[1].Null {
		t.Fatalf("NULL VECTOR after=%+v", tx.Events)
	}
}

func TestReaderVector213WithoutStartFailsClosed(t *testing.T) {
	tid := "010203040506"
	f := &fakeAgent{boot: BootstrapResponse{Records: []RecordEnvelope{vectorDescriptorRecord(tid, false)}}, reads: []*ReadResponse{{Records: []RecordEnvelope{
		vectorDataRecord(tid, 10, 11, 1, "[1,2]"),
	}, NextStartLRI: LRI{Type: 1, Part2: 11}}}}
	r, err := NewReader(context.Background(), f, LRI{Type: 1, Part2: 10}, []Selection{vectorSelection()}, "agent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "VECTOR record") || !strings.Contains(err.Error(), "without start-of-out-of-row") {
		t.Fatalf("VECTOR without marker must fail closed: %v", err)
	}
}

func TestParseVectorManagerRejectsMalformedSerializedValue(t *testing.T) {
	tid := "010203040506"
	e := vectorDataRecord(tid, 10, 11, 1, "not-a-vector")
	if _, err := ParseVectorManager(e); err == nil || !strings.Contains(err.Error(), "bracket-delimited") {
		t.Fatalf("malformed VECTOR must fail closed: %v", err)
	}
}
