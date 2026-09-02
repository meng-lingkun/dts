package main

import (
	"encoding/binary"
	"qmigration/backend/internal/cdc/mysqlbinlog"
	"qmigration/backend/internal/domain"
	"testing"
)

func testJSONDiffReplace(path string, binaryValue []byte) []byte {
	out := []byte{0, byte(len(path))}
	out = append(out, []byte(path)...)
	out = append(out, byte(len(binaryValue)))
	out = append(out, binaryValue...)
	return out
}

func testJSONDiffVector(diffs ...[]byte) []byte {
	body := []byte{}
	for _, diff := range diffs {
		body = append(body, diff...)
	}
	out := make([]byte, 4, 4+len(body))
	binary.LittleEndian.PutUint32(out, uint32(len(body)))
	return append(out, body...)
}

func TestDecodeTransactionPartialJSONCarriesCheckpoint(t *testing.T) {
	// Binary JSON {"a":1,"b":"x"}.
	beforeJSON := []byte{
		0x00,
		0x02, 0x00,
		0x16, 0x00,
		0x12, 0x00, 0x01, 0x00,
		0x13, 0x00, 0x01, 0x00,
		0x05, 0x01, 0x00,
		0x0c, 0x14, 0x00,
		'a', 'b',
		0x01, 'x',
	}
	// Replace $.a with binary JSON INT16(2). The partial field itself contains
	// a Json_diff_vector (4-byte byte length + one or more diffs).
	diff := testJSONDiffVector(testJSONDiffReplace("$.a", []byte{0x05, 0x02, 0x00}))

	rowData := []byte{0x00} // before-image NULL bitmap
	id := make([]byte, 4)
	binary.LittleEndian.PutUint32(id, 7)
	rowData = append(rowData, id...)
	rowData = append(rowData, byte(len(beforeJSON)))
	rowData = append(rowData, beforeJSON...)
	rowData = append(rowData,
		0x01, // binlog_row_value_options = PARTIAL_JSON
		0x01, // partial bitmap: JSON column #0 uses a diff vector
		0x00, // after-image NULL bitmap
	)
	rowData = append(rowData, id...)
	rowData = append(rowData, byte(len(diff)))
	rowData = append(rowData, diff...)

	// Build a complete UPDATE_ROWS_EVENT payload as ParseRows expects it.
	payload := []byte{9, 0, 0, 0, 0, 0, 0, 0} // table-id + flags
	payload = append(payload, 2, 0)           // v2 extra-data length=2
	payload = append(payload, 2)              // column count
	payload = append(payload, 0x03, 0x03)     // before/after bitmaps
	payload = append(payload, rowData...)
	ev := &mysqlbinlog.Event{Header: mysqlbinlog.Header{Type: mysqlbinlog.PartialUpdateRowsEvent, Timestamp: 123, LogPos: 456}, Payload: payload}
	tx := &mysqlbinlog.Transaction{File: "mysql-bin.000007", EndPos: 456, GTID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:9", Events: []*mysqlbinlog.Event{ev}}
	tm := &mysqlbinlog.TableMap{TableID: 9, Schema: "app", Table: "orders", ColumnTypes: []byte{3, 245}, ColumnMeta: []byte{1}}
	md := &domain.TableMetadata{Schema: "app", Name: "orders", Columns: []domain.ColumnInfo{
		{Name: "id", DataType: "int", ColumnType: "int", PrimaryKey: true, Ordinal: 1},
		{Name: "doc", DataType: "json", ColumnType: "json", Ordinal: 2},
	}, PrimaryKeys: []string{"id"}, PrimaryKey: "id"}

	events, err := decodeTransaction(tx, map[uint64]*mysqlbinlog.TableMap{9: tm}, map[uint64]*domain.TableMetadata{9: md}, selectedSet("app.orders"))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Operation != domain.CDCUpdate {
		t.Fatalf("unexpected CDC events: %+v", events)
	}
	if events[0].PositionType != "BINLOG" || events[0].PositionValue != "mysql-bin.000007:456" {
		t.Fatalf("checkpoint not attached to rebuilt partial JSON event: %+v", events[0])
	}
	var doc string
	for _, f := range events[0].After {
		if f.Column == "doc" {
			doc = f.Value
		}
	}
	if doc != `{"a":2,"b":"x"}` {
		t.Fatalf("partial JSON was not reconstructed: %s", doc)
	}
}

func TestReplicationEndpoints(t *testing.T) {
	eps, err := replicationEndpoints("odp1", 2883, "odp2:2883,odp3:2883,odp2:2883")
	if err != nil {
		t.Fatal(err)
	}
	if len(eps) != 3 || eps[0].String() != "odp1:2883" || eps[2].String() != "odp3:2883" {
		t.Fatalf("unexpected endpoints: %#v", eps)
	}
	if _, err := replicationEndpoints("odp1", 2883, "bad"); err == nil {
		t.Fatal("invalid fallback endpoint must fail")
	}
}
