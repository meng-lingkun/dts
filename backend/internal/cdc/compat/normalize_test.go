package compat

import (
	"testing"

	"qmigration/backend/internal/domain"
)

func TestNormalizeDebeziumMySQLUpdate(t *testing.T) {
	raw := []byte(`{"payload":{"before":{"id":1,"name":"old"},"after":{"id":1,"name":"new"},"source":{"db":"app","table":"users","file":"mysql-bin.000007","pos":1234,"gtid":"uuid:1-9"},"op":"u","ts_ms":1720000000123,"transaction":{"id":"tx-9"}}}`)
	events, err := NormalizeDebezium(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events=%d", len(events))
	}
	e := events[0]
	if e.Operation != domain.CDCUpdate || e.SourceSchema != "app" || e.SourceTable != "users" {
		t.Fatalf("event=%+v", e)
	}
	if e.PositionType != "GTID" || e.PositionValue != "uuid:1-9" || e.Resource != "mysql-bin.000007" {
		t.Fatalf("position=%+v", e)
	}
	if len(e.Before) != 2 || len(e.After) != 2 {
		t.Fatalf("images before=%v after=%v", e.Before, e.After)
	}
}

func TestNormalizeDebeziumPostgresLSN(t *testing.T) {
	raw := []byte(`{"before":null,"after":{"id":"8f5a","payload":{"k":"v"}},"source":{"schema":"public","table":"events","lsn":268435456,"ts_us":1720000000123456},"op":"c","ts_us":1720000000123456}`)
	events, err := NormalizeDebezium(raw)
	if err != nil {
		t.Fatal(err)
	}
	e := events[0]
	if e.PositionType != "LSN" || e.PositionValue != "268435456" {
		t.Fatalf("position=%+v", e)
	}
	if e.SourceTimestampMS != 1720000000123 {
		t.Fatalf("timestamp=%d", e.SourceTimestampMS)
	}
}

func TestNormalizeDebeziumRejectsUpdateWithoutBefore(t *testing.T) {
	_, err := NormalizeDebezium([]byte(`{"payload":{"before":null,"after":{"id":1},"source":{"db":"app","table":"t","file":"bin.1","pos":9},"op":"u"}}`))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNormalizeCanalUpdateReconstructsBefore(t *testing.T) {
	raw := []byte(`{"data":[{"id":"1","name":"new","age":"20"}],"database":"app","es":1720000000999,"id":42,"isDdl":false,"old":[{"name":"old"}],"table":"users","type":"UPDATE"}`)
	events, err := NormalizeCanal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events=%d", len(events))
	}
	e := events[0]
	if e.Operation != domain.CDCUpdate || e.PositionType != "CANAL_ID" || e.PositionValue != "42@1720000000999" {
		t.Fatalf("event=%+v", e)
	}
	before := map[string]string{}
	for _, f := range e.Before {
		before[f.Column] = f.Value
	}
	if before["name"] != "old" || before["age"] != "20" {
		t.Fatalf("before=%v", before)
	}
}

func TestNormalizeCanalMultiRowPositionOnFinalEvent(t *testing.T) {
	events, err := NormalizeCanal([]byte(`{"data":[{"id":"1"},{"id":"2"}],"database":"app","es":1000,"id":9,"table":"t","type":"INSERT"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events=%d", len(events))
	}
	if events[0].PositionValue != "" || events[1].PositionValue != "9@1000" {
		t.Fatalf("positions=%q,%q", events[0].PositionValue, events[1].PositionValue)
	}
}

func TestNormalizeCanalDDL(t *testing.T) {
	events, err := NormalizeCanal([]byte(`{"database":"app","es":1000,"id":10,"isDdl":true,"sql":"ALTER TABLE t ADD c INT","table":"t","type":"ALTER"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Operation != domain.CDCDDL || events[0].SQL == "" {
		t.Fatalf("events=%+v", events)
	}
}
