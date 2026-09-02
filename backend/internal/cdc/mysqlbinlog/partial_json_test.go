package mysqlbinlog

import (
	"encoding/binary"
	"qmigration/backend/internal/domain"
	"testing"
)

func jsonDiffReplace(path string, binaryValue []byte) []byte {
	out := []byte{byte(JSONDiffReplace), byte(len(path))}
	out = append(out, []byte(path)...)
	out = append(out, byte(len(binaryValue)))
	out = append(out, binaryValue...)
	return out
}

func jsonDiffInsert(path string, binaryValue []byte) []byte {
	out := []byte{byte(JSONDiffInsert), byte(len(path))}
	out = append(out, []byte(path)...)
	out = append(out, byte(len(binaryValue)))
	out = append(out, binaryValue...)
	return out
}

func jsonDiffRemove(path string) []byte {
	out := []byte{byte(JSONDiffRemove), byte(len(path))}
	out = append(out, []byte(path)...)
	return out
}

func jsonDiffVector(diffs ...[]byte) []byte {
	body := []byte{}
	for _, diff := range diffs {
		body = append(body, diff...)
	}
	out := make([]byte, 4, 4+len(body))
	binary.LittleEndian.PutUint32(out, uint32(len(body)))
	out = append(out, body...)
	return out
}

func TestParseAndApplyJSONDiff(t *testing.T) {
	diffBytes := jsonDiffReplace("$.a", []byte{jsonInt16, 0x02, 0x00})
	diff, err := ParseJSONDiff(diffBytes)
	if err != nil {
		t.Fatal(err)
	}
	if diff.Operation != JSONDiffReplace || diff.Path != "$.a" || diff.Value != "2" {
		t.Fatalf("unexpected diff: %+v", diff)
	}
	got, err := ApplyJSONDiff(`{"a":1,"b":[true,"x"]}`, diff)
	if err != nil {
		t.Fatal(err)
	}
	if got != `{"a":2,"b":[true,"x"]}` {
		t.Fatalf("unexpected rebuilt JSON: %s", got)
	}
}

func TestApplyJSONDiffInsertAndRemove(t *testing.T) {
	insert := &JSONDiff{Operation: JSONDiffInsert, Path: "$.b[1]", Value: `2`}
	got, err := ApplyJSONDiff(`{"a":1,"b":[1,3]}`, insert)
	if err != nil {
		t.Fatal(err)
	}
	if got != `{"a":1,"b":[1,2,3]}` {
		t.Fatalf("unexpected insert result: %s", got)
	}
	removed, err := ApplyJSONDiff(got, &JSONDiff{Operation: JSONDiffRemove, Path: "$.a"})
	if err != nil {
		t.Fatal(err)
	}
	if removed != `{"b":[1,2,3]}` {
		t.Fatalf("unexpected remove result: %s", removed)
	}
}

func TestParseAndApplyJSONDiffVector(t *testing.T) {
	vector := jsonDiffVector(
		jsonDiffReplace("$.a", []byte{jsonInt16, 0x02, 0x00}),
		jsonDiffInsert("$.b[1]", []byte{jsonInt16, 0x02, 0x00}),
		jsonDiffRemove("$.c"),
	)
	diffs, err := ParseJSONDiffVector(vector)
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 3 {
		t.Fatalf("expected three diffs, got %+v", diffs)
	}
	got, err := ApplyJSONDiffVector(`{"a":1,"b":[1,3],"c":true}`, diffs)
	if err != nil {
		t.Fatal(err)
	}
	if got != `{"a":2,"b":[1,2,3]}` {
		t.Fatalf("unexpected rebuilt JSON: %s", got)
	}
}

func TestPartialJSONDiffSupportsOpaqueDecimal(t *testing.T) {
	vector := jsonDiffVector(
		jsonDiffReplace("$.amount", opaqueJSON(TypeNewDecimal, []byte{5, 2, 0x80, 0x7b, 0x2d})),
	)
	diffs, err := ParseJSONDiffVector(vector)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ApplyJSONDiffVector(`{"amount":1,"status":"open"}`, diffs)
	if err != nil {
		t.Fatal(err)
	}
	if got != `{"amount":123.45,"status":"open"}` {
		t.Fatalf("unexpected rebuilt JSON with OPAQUE decimal: %s", got)
	}
}

func TestJSONDiffVectorRejectsCorruptLength(t *testing.T) {
	vector := jsonDiffVector(jsonDiffRemove("$.a"))
	binary.LittleEndian.PutUint32(vector[:4], uint32(len(vector)))
	if _, err := ParseJSONDiffVector(vector); err == nil {
		t.Fatal("expected corrupt vector length to fail")
	}
}

func TestDecodePartialUpdateRowsRebuildsFullJSON(t *testing.T) {
	beforeJSON := []byte{
		jsonSmallObject,
		0x02, 0x00, // count
		0x16, 0x00, // size=22
		0x12, 0x00, 0x01, 0x00,
		0x13, 0x00, 0x01, 0x00,
		jsonInt16, 0x01, 0x00,
		jsonString, 0x14, 0x00,
		'a', 'b',
		0x01, 'x',
	}
	diff := jsonDiffVector(jsonDiffReplace("$.a", []byte{jsonInt16, 0x02, 0x00}))
	rowData := []byte{0x00} // before null bitmap
	id := make([]byte, 4)
	binary.LittleEndian.PutUint32(id, 1)
	rowData = append(rowData, id...)
	rowData = append(rowData, byte(len(beforeJSON)))
	rowData = append(rowData, beforeJSON...)
	rowData = append(rowData,
		0x01, // binlog_row_value_options = PARTIAL_JSON
		0x01, // first JSON column is partial
		0x00, // after null bitmap
	)
	rowData = append(rowData, id...)
	rowData = append(rowData, byte(len(diff)))
	rowData = append(rowData, diff...)

	tm := &TableMap{TableID: 7, Schema: "app", Table: "orders", ColumnTypes: []byte{TypeLong, TypeJSON}, ColumnMeta: []byte{1}}
	rows := &Rows{EventType: PartialUpdateRowsEvent, TableID: 7, ColumnCount: 2, BeforeBitmap: []byte{0x03}, AfterBitmap: []byte{0x03}, RowData: rowData, Update: true}
	cols := []domain.ColumnInfo{{Name: "id", DataType: "int", ColumnType: "int"}, {Name: "doc", DataType: "json", ColumnType: "json"}}
	changes, err := DecodeRows(tm, rows, cols)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || len(changes[0].After) != 2 {
		t.Fatalf("unexpected decoded changes: %+v", changes)
	}
	doc, ok := findCDCField(changes[0].After, "doc")
	if !ok || doc.Encoding != "json" || doc.Value != `{"a":2,"b":"x"}` {
		t.Fatalf("partial JSON was not rebuilt: %+v", doc)
	}
}

func TestPartialJSONRequiresFullBeforeImage(t *testing.T) {
	diff := jsonDiffVector(jsonDiffReplace("$.a", []byte{jsonInt16, 0x02, 0x00}))
	data := []byte{0x01, 0x01, 0x00, byte(len(diff))}
	data = append(data, diff...)
	tm := &TableMap{TableID: 1, ColumnTypes: []byte{TypeJSON}, ColumnMeta: []byte{1}}
	cols := []domain.ColumnInfo{{Name: "doc", DataType: "json", ColumnType: "json"}}
	metas, err := ColumnMetadata(tm)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodePartialUpdateAfterImage(data, []byte{0x01}, tm, cols, metas, nil); err == nil {
		t.Fatal("expected missing full before-image to fail safe")
	}
}
