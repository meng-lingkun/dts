package sqlserverconnector

import (
	"encoding/base64"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"testing"
)

func TestChangesToTransactionsPairsUpdateAndBinary(t *testing.T) {
	cols := []domain.ColumnInfo{{Name: "id", DataType: "int"}, {Name: "payload", DataType: "varbinary"}}
	lsn := "0x00000000000000000001"
	seq := "0x00000000000000000002"
	changes := []CDCChange{
		{StartLSN: lsn, SeqVal: seq, Operation: 3, Schema: "dbo", Table: "t", Capture: "dbo_t", Columns: cols, Values: []connector.Value{{Raw: []byte("1")}, {Raw: []byte{1, 2}}}},
		{StartLSN: lsn, SeqVal: seq, Operation: 4, Schema: "dbo", Table: "t", Capture: "dbo_t", Columns: cols, Values: []connector.Value{{Raw: []byte("1")}, {Raw: []byte{3, 4}}}},
	}
	txs, err := ChangesToTransactions(changes)
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 1 || len(txs[0].Events) != 1 {
		t.Fatalf("txs=%+v", txs)
	}
	e := txs[0].Events[0]
	if e.Operation != domain.CDCUpdate || len(e.Before) != 2 || len(e.After) != 2 {
		t.Fatalf("event=%+v", e)
	}
	if e.Before[1].Encoding != "base64" || e.Before[1].Value != base64.StdEncoding.EncodeToString([]byte{1, 2}) {
		t.Fatalf("before binary=%+v", e.Before[1])
	}
	if e.After[1].Value != base64.StdEncoding.EncodeToString([]byte{3, 4}) {
		t.Fatalf("after binary=%+v", e.After[1])
	}
}

func TestNormalizeSQLServerLSN(t *testing.T) {
	if v, err := normalizeLSN("0x0000000000000000000a"); err != nil || v != "0x0000000000000000000A" {
		t.Fatalf("v=%s err=%v", v, err)
	}
	for _, bad := range []string{"10", "0xz1", "0x123"} {
		if _, err := normalizeLSN(bad); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestCompareLSN(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{{"0x01", "0x02", -1}, {"0x0A", "0x09", 1}, {"0x0001", "0x01", 0}}
	for _, tc := range cases {
		got, err := compareLSN(tc.a, tc.b)
		if err != nil || got != tc.want {
			t.Fatalf("compare %s %s=%d err=%v", tc.a, tc.b, got, err)
		}
	}
}
