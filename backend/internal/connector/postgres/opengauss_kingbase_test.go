package postgresconnector

import (
	"strings"
	"testing"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func TestOpenGaussAndKingbaseCDCQualificationGates(t *testing.T) {
	f := NewFactory()
	t.Setenv("QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC", "")
	t.Setenv("QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC", "")
	for _, typ := range []domain.DataSourceType{domain.DataSourceOpenGauss, domain.DataSourceKingbase} {
		d := f.Capabilities(typ)
		if !d.Has(connector.CapabilityFullRead) || !d.Has(connector.CapabilityFullWrite) || d.Has(connector.CapabilityCDCRead) {
			t.Fatalf("default %s capability mismatch: %+v", typ, d)
		}
		if d.Maturity != connector.MaturityNativeFullOnly {
			t.Fatalf("default %s maturity=%s", typ, d.Maturity)
		}
	}

	t.Setenv("QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC", "1")
	og := f.Capabilities(domain.DataSourceOpenGauss)
	for _, cap := range []connector.Capability{connector.CapabilityCDCPosition, connector.CapabilityCDCCheckpoint, connector.CapabilityCDCRead} {
		if !og.Has(cap) {
			t.Fatalf("openGauss gate missing %s: %+v", cap, og)
		}
	}
	if og.Maturity != connector.MaturityExperimental || !og.QualificationRequired {
		t.Fatalf("unexpected openGauss descriptor: %+v", og)
	}

	t.Setenv("QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC", "1")
	kb := f.Capabilities(domain.DataSourceKingbase)
	for _, cap := range []connector.Capability{connector.CapabilityCDCPosition, connector.CapabilityCDCCheckpoint, connector.CapabilityCDCRead} {
		if !kb.Has(cap) {
			t.Fatalf("Kingbase gate missing %s: %+v", cap, kb)
		}
	}
	if kb.Maturity != connector.MaturityExperimental || !kb.QualificationRequired {
		t.Fatalf("unexpected Kingbase descriptor: %+v", kb)
	}
}

func TestOpenGaussLogicalDecodeQueryAndTransaction(t *testing.T) {
	q, err := openGaussDecodeQuery("pg_logical_slot_peek_changes", "qmigration_og", 4096, "0/16B6C50", []string{"public.orders", "sales.items"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"pg_logical_slot_peek_changes", "qmigration_og", "0/16B6C50", "skip-empty-xacts", "include-xids", "white-table-list", "public.orders,sales.items"} {
		if !strings.Contains(q, want) {
			t.Fatalf("query missing %q: %s", want, q)
		}
	}
	if _, err := openGaussDecodeQuery("pg_logical_slot_peek_changes", "bad slot", 1, "", []string{"public.t"}); err == nil {
		t.Fatal("invalid slot accepted")
	}
	if _, err := openGaussDecodeQuery("bad_func", "slot", 1, "", []string{"public.t"}); err == nil {
		t.Fatal("invalid function accepted")
	}

	rows := &RawRows{Rows: [][][]byte{
		{gv("0/101"), gv("42"), gv("BEGIN 42")},
		{gv("0/102"), gv("42"), gv(`{"table_name":"public.orders","op_type":"INSERT","columns_name":["id","name"],"columns_type":["integer","text"],"columns_val":["1","alice"],"old_keys_name":[],"old_keys_type":[],"old_keys_val":[]}`)},
		{gv("0/103"), gv("42"), gv(`{"table_name":"public.orders","op_type":"UPDATE","columns_name":["id","name"],"columns_type":["integer","text"],"columns_val":["1","bob"],"old_keys_name":["id"],"old_keys_type":["integer"],"old_keys_val":["1"]}`)},
		{gv("0/104"), gv("42"), gv("COMMIT 42")},
	}}
	txs, err := ParseOpenGaussLogicalRows(rows, "qmigration_og")
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 1 || txs[0].CommitLSN != "0/104" || len(txs[0].Events) != 2 {
		t.Fatalf("txs=%+v", txs)
	}
	for _, ev := range txs[0].Events {
		if ev.PositionType != "OPENGAUSS_LSN" || ev.PositionValue != "0/104" || ev.Resource != "qmigration_og" || !strings.HasPrefix(ev.ID, "opengauss:42:0/104:") {
			t.Fatalf("bad openGauss event identity: %+v", ev)
		}
	}
}

func TestOpenGaussLogicalDecodeFailsClosed(t *testing.T) {
	binaryRows := &RawRows{Rows: [][][]byte{
		{gv("0/1"), gv("2"), gv("BEGIN 2")},
		{gv("0/2"), gv("2"), gv(`{"table_name":"s.t","op_type":"INSERT","columns_name":["b"],"columns_type":["bytea"],"columns_val":["abc"],"old_keys_name":[],"old_keys_type":[],"old_keys_val":[]}`)},
		{gv("0/3"), gv("2"), gv("COMMIT 2")},
	}}
	if _, err := ParseOpenGaussLogicalRows(binaryRows, "slot"); err == nil || !strings.Contains(err.Error(), "not qualified for binary") {
		t.Fatalf("binary JSON path must fail closed, err=%v", err)
	}
	partial := &RawRows{Rows: [][][]byte{{gv("0/1"), gv("3"), gv("BEGIN 3")}}}
	if _, err := ParseOpenGaussLogicalRows(partial, "slot"); err == nil || !strings.Contains(err.Error(), "before COMMIT") {
		t.Fatalf("partial transaction must fail closed, err=%v", err)
	}
}
