package postgresconnector

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func gv(s string) []byte { return []byte(s) }

func TestGaussDBCapabilitiesAreQualificationGated(t *testing.T) {
	f := NewFactory()
	t.Setenv("QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE", "")
	t.Setenv("QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC", "")
	d := f.Capabilities(domain.DataSourceGaussDB)
	if !d.Has(connector.CapabilityProtocolProbe) || d.Has(connector.CapabilityFullRead) || d.Has(connector.CapabilityCDCRead) {
		t.Fatalf("default GaussDB descriptor must remain probe only: %+v", d)
	}
	if d.Maturity != connector.MaturityProbeOnly || !d.QualificationRequired {
		t.Fatalf("unexpected default GaussDB maturity: %+v", d)
	}

	t.Setenv("QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE", "1")
	d = f.Capabilities(domain.DataSourceGaussDB)
	for _, cap := range []connector.Capability{connector.CapabilityMetadata, connector.CapabilityFullRead, connector.CapabilityFullWrite, connector.CapabilityCDCApply, connector.CapabilityCDCTransactional} {
		if !d.Has(cap) {
			t.Fatalf("native gate missing %s: %+v", cap, d)
		}
	}
	if d.Has(connector.CapabilityCDCRead) || d.Has(connector.CapabilityCDCPosition) {
		t.Fatalf("CDC must require the second gate: %+v", d)
	}
	if d.Maturity != connector.MaturityExperimental || !d.QualificationRequired {
		t.Fatalf("unexpected gated GaussDB maturity: %+v", d)
	}

	t.Setenv("QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC", "1")
	d = f.Capabilities(domain.DataSourceGaussDB)
	for _, cap := range []connector.Capability{connector.CapabilityCDCRead, connector.CapabilityCDCPosition, connector.CapabilityCDCCheckpoint} {
		if !d.Has(cap) {
			t.Fatalf("CDC gate missing %s: %+v", cap, d)
		}
	}
	if !domain.DataSourceGaussDB.IsPostgreSQLWireCompatible() || domain.DataSourceGaussDB.IsExternalJDBC() {
		t.Fatalf("GaussDB classification not switched to native PostgreSQL-wire path")
	}
}

func TestParseGaussDBLogicalTransaction(t *testing.T) {
	rows := &RawRows{Rows: [][][]byte{
		{gv("0/101"), gv("42"), gv("BEGIN 42")},
		{gv("0/102"), gv("42"), gv(`{"table_name":"public.orders","op_type":"INSERT","columns_name":["id","name"],"columns_type":["integer","text"],"columns_val":["1","alice"],"old_keys_name":[],"old_keys_type":[],"old_keys_val":[]}`)},
		{gv("0/103"), gv("42"), gv(`{"table_name":"public.orders","op_type":"UPDATE","columns_name":["id","name"],"columns_type":["integer","text"],"columns_val":["1","bob"],"old_keys_name":["id"],"old_keys_type":["integer"],"old_keys_val":["1"]}`)},
		{gv("0/104"), gv("42"), gv(`{"table_name":"public.orders","op_type":"DELETE","columns_name":[],"columns_type":[],"columns_val":[],"old_keys_name":["id"],"old_keys_type":["integer"],"old_keys_val":["1"]}`)},
		{gv("0/105"), gv("42"), gv("COMMIT 42")},
	}}
	txs, err := ParseGaussDBLogicalRows(rows, "qmigration_slot")
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 1 || txs[0].XID != "42" || txs[0].CommitLSN != "0/105" || len(txs[0].Events) != 3 {
		t.Fatalf("txs=%+v", txs)
	}
	if txs[0].Events[0].Operation != domain.CDCInsert || txs[0].Events[1].Operation != domain.CDCUpdate || txs[0].Events[2].Operation != domain.CDCDelete {
		t.Fatalf("ops=%+v", txs[0].Events)
	}
	upd := txs[0].Events[1]
	if len(upd.Before) != 1 || upd.Before[0].Column != "id" || upd.Before[0].Value != "1" || len(upd.After) != 2 || upd.After[1].Value != "bob" {
		t.Fatalf("update=%+v", upd)
	}
	for i, ev := range txs[0].Events {
		if ev.PositionType != "GAUSSDB_LSN" || ev.PositionValue != "0/105" || ev.Resource != "qmigration_slot" || !strings.HasPrefix(ev.ID, "gaussdb:42:0/105:") {
			t.Fatalf("event %d checkpoint identity=%+v", i, ev)
		}
	}
}

func TestParseGaussDBLogicalArrayAndSafetyFailures(t *testing.T) {
	arrayRows := &RawRows{Rows: [][][]byte{
		{gv("0/201"), gv("7"), gv("BEGIN 7")},
		{gv("0/202"), gv("7"), gv(`[{"table_name":"s.t","op_type":"INSERT","columns_name":["id"],"columns_type":["integer"],"columns_val":["9"],"old_keys_name":[],"old_keys_type":[],"old_keys_val":[]}]`)},
		{gv("0/203"), gv("7"), gv("COMMIT 7")},
	}}
	txs, err := ParseGaussDBLogicalRows(arrayRows, "slot")
	if err != nil || len(txs) != 1 || len(txs[0].Events) != 1 {
		t.Fatalf("array txs=%+v err=%v", txs, err)
	}

	partial := &RawRows{Rows: [][][]byte{{gv("0/1"), gv("1"), gv("BEGIN 1")}, {gv("0/2"), gv("1"), gv(`{"table_name":"s.t","op_type":"INSERT","columns_name":["id"],"columns_type":["integer"],"columns_val":["1"]}`)}}}
	if _, err := ParseGaussDBLogicalRows(partial, "slot"); err == nil || !strings.Contains(err.Error(), "before COMMIT") {
		t.Fatalf("partial transaction must fail closed, err=%v", err)
	}

	binaryRows := &RawRows{Rows: [][][]byte{
		{gv("0/1"), gv("2"), gv("BEGIN 2")},
		{gv("0/2"), gv("2"), gv(`{"table_name":"s.t","op_type":"INSERT","columns_name":["b"],"columns_type":["bytea"],"columns_val":["abc"]}`)},
		{gv("0/3"), gv("2"), gv("COMMIT 2")},
	}}
	if _, err := ParseGaussDBLogicalRows(binaryRows, "slot"); err == nil || !strings.Contains(err.Error(), "not qualified for binary") {
		t.Fatalf("binary JSON logical decoding must fail closed, err=%v", err)
	}

	mismatch := &RawRows{Rows: [][][]byte{{gv("0/1"), gv("3"), gv("BEGIN 3")}, {gv("0/2"), gv("4"), gv(`{"table_name":"s.t","op_type":"INSERT","columns_name":["id"],"columns_type":["integer"],"columns_val":["1"]}`)}, {gv("0/3"), gv("3"), gv("COMMIT 3")}}}
	if _, err := ParseGaussDBLogicalRows(mismatch, "slot"); err == nil || !strings.Contains(err.Error(), "xid changed") {
		t.Fatalf("xid mismatch must fail closed, err=%v", err)
	}
}

func TestGaussDBCreateSlotForcesLSNOrder(t *testing.T) {
	q, err := gaussDBCreateSlotQuery("qmigration_slot")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "'mppdb_decoding',0") {
		t.Fatalf("GaussDB slot must explicitly use LSN output_order=0: %s", q)
	}
	if _, err := gaussDBCreateSlotQuery("Bad Slot"); err == nil {
		t.Fatal("invalid GaussDB slot accepted")
	}
}

func TestGaussDBDecodeQueryValidation(t *testing.T) {
	q, err := gaussDBDecodeQuery("pg_logical_slot_peek_changes", "slot_1", 123, "0/16B6C50", []string{"public.orders", "sales.items"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"pg_logical_slot_peek_changes", "slot_1", "0/16B6C50", "white-table-list", "public.orders,sales.items"} {
		if !strings.Contains(q, want) {
			t.Fatalf("query missing %q: %s", want, q)
		}
	}
	if _, err := gaussDBDecodeQuery("pg_logical_slot_peek_changes", "bad slot", 1, "", []string{"public.t"}); err == nil {
		t.Fatal("invalid slot accepted")
	}
	if _, err := gaussDBDecodeQuery("pg_logical_slot_peek_changes", "slot", 1, "", []string{"bad-table"}); err == nil {
		t.Fatal("invalid table accepted")
	}
	if _, err := gaussDBDecodeQuery("pg_logical_slot_peek_changes", "slot", 1, "not-lsn", []string{"public.t"}); err == nil {
		t.Fatal("invalid LSN accepted")
	}
}

func gbU16(b *bytes.Buffer, v uint16) { _ = binary.Write(b, binary.BigEndian, v) }
func gbU32(b *bytes.Buffer, v uint32) { _ = binary.Write(b, binary.BigEndian, v) }
func gbU64(b *bytes.Buffer, v uint64) { _ = binary.Write(b, binary.BigEndian, v) }

func gbLSN(t *testing.T, s string) uint64 {
	t.Helper()
	v, err := parseReplicationLSN(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

type gbCol struct {
	name string
	oid  uint32
	val  []byte
	null bool
}

func gbTuple(cols ...gbCol) []byte {
	var b bytes.Buffer
	gbU16(&b, uint16(len(cols)))
	for _, c := range cols {
		gbU16(&b, uint16(len(c.name)))
		b.WriteString(c.name)
		gbU32(&b, c.oid)
		if c.null {
			gbU32(&b, 0xffffffff)
			continue
		}
		gbU32(&b, uint32(len(c.val)))
		b.Write(c.val)
	}
	return b.Bytes()
}

func gbFrame(t *testing.T, lsn string, kind byte, payload []byte) []byte {
	t.Helper()
	var frame bytes.Buffer
	gbU64(&frame, gbLSN(t, lsn))
	frame.WriteByte(kind)
	frame.Write(payload)
	var out bytes.Buffer
	gbU32(&out, uint32(frame.Len()))
	out.Write(frame.Bytes())
	out.WriteByte('F')
	return out.Bytes()
}

func gbBegin(t *testing.T, lsn string) []byte {
	var p bytes.Buffer
	gbU64(&p, 99) // CSN
	gbU64(&p, gbLSN(t, lsn))
	return gbFrame(t, lsn, 'B', p.Bytes())
}

func gbCommit(t *testing.T, lsn string, xid uint64) []byte {
	var p bytes.Buffer
	p.WriteByte('X')
	gbU64(&p, xid)
	return gbFrame(t, lsn, 'C', p.Bytes())
}

func gbDML(t *testing.T, lsn string, kind byte, schema, table string, newCols, oldCols []gbCol) []byte {
	var p bytes.Buffer
	gbU16(&p, uint16(len(schema)))
	p.WriteString(schema)
	gbU16(&p, uint16(len(table)))
	p.WriteString(table)
	if newCols != nil {
		p.WriteByte('N')
		p.Write(gbTuple(newCols...))
	}
	if oldCols != nil {
		p.WriteByte('O')
		p.Write(gbTuple(oldCols...))
	}
	return gbFrame(t, lsn, kind, p.Bytes())
}

func gbRow(t *testing.T, lsn, xid string, msg []byte) [][]byte {
	return [][]byte{gv(lsn), gv(xid), gv(hex.EncodeToString(msg))}
}

func TestParseGaussDBBinaryTransactionAndBinaryValues(t *testing.T) {
	rows := &RawRows{}
	rows.Rows = append(rows.Rows, gbRow(t, "0/301", "42", gbBegin(t, "0/301")))
	rows.Rows = append(rows.Rows, gbRow(t, "0/302", "42", gbDML(t, "0/302", 'I', "public", "orders", []gbCol{
		{name: "id", oid: 23, val: []byte("1")},
		{name: "payload", oid: 99999, val: []byte{'a', 0, 0xff, 'z'}},
		{name: "empty", oid: 25, val: []byte{}},
		{name: "nullable", oid: 25, null: true},
		{name: "raw_bytes", oid: 17, val: []byte(`\x00ff41`)},
	}, nil)))
	rows.Rows = append(rows.Rows, gbRow(t, "0/303", "42", gbDML(t, "0/303", 'U', "public", "orders",
		[]gbCol{{name: "id", oid: 23, val: []byte("1")}, {name: "name", oid: 25, val: []byte("bob")}},
		[]gbCol{{name: "id", oid: 23, val: []byte("1")}},
	)))
	rows.Rows = append(rows.Rows, gbRow(t, "0/304", "42", gbDML(t, "0/304", 'D', "public", "orders", nil,
		[]gbCol{{name: "id", oid: 23, val: []byte("1")}},
	)))
	rows.Rows = append(rows.Rows, gbRow(t, "0/305", "42", gbCommit(t, "0/305", 42)))

	txs, err := ParseGaussDBBinaryRows(rows, "slot_bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 1 || txs[0].CommitLSN != "0/305" || len(txs[0].Events) != 3 {
		t.Fatalf("txs=%+v", txs)
	}
	ins := txs[0].Events[0]
	if ins.Operation != domain.CDCInsert || len(ins.After) != 5 {
		t.Fatalf("insert=%+v", ins)
	}
	if ins.After[1].Encoding != "base64" || ins.After[1].Value != "YQD/eg==" {
		t.Fatalf("NUL/non-UTF8 payload not preserved: %+v", ins.After[1])
	}
	if ins.After[2].Null || ins.After[2].Value != "" {
		t.Fatalf("empty non-NULL changed: %+v", ins.After[2])
	}
	if !ins.After[3].Null {
		t.Fatalf("NULL lost: %+v", ins.After[3])
	}
	if ins.After[4].Encoding != "base64" || ins.After[4].Value != "AP9B" {
		t.Fatalf("bytea hex was not restored to raw bytes: %+v", ins.After[4])
	}
	upd := txs[0].Events[1]
	if upd.Operation != domain.CDCUpdate || len(upd.Before) != 1 || len(upd.After) != 2 || upd.After[1].Value != "bob" {
		t.Fatalf("update=%+v", upd)
	}
	if txs[0].Events[2].Operation != domain.CDCDelete {
		t.Fatalf("delete=%+v", txs[0].Events[2])
	}
	for _, ev := range txs[0].Events {
		if ev.PositionType != "GAUSSDB_LSN" || ev.PositionValue != "0/305" || ev.Resource != "slot_bin" {
			t.Fatalf("checkpoint identity=%+v", ev)
		}
	}
}

func TestGaussDBBinaryParserFailClosed(t *testing.T) {
	good := gbBegin(t, "0/401")
	badDelimiter := append([]byte(nil), good...)
	badDelimiter[len(badDelimiter)-1] = 'Q'
	if _, err := decodeGaussBinaryCell([]byte(hex.EncodeToString(badDelimiter))); err == nil || !strings.Contains(err.Error(), "delimiter") {
		t.Fatalf("bad delimiter accepted: %v", err)
	}

	rows := &RawRows{Rows: [][][]byte{gbRow(t, "0/999", "1", gbBegin(t, "0/401"))}}
	if _, err := ParseGaussDBBinaryRows(rows, "slot"); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("LSN mismatch accepted: %v", err)
	}

	bytea := gaussDBBinaryColumn{Name: "b", TypeOID: 17, Value: []byte("escape-not-qualified")}
	if _, err := gaussDBBinaryField(bytea); err == nil || !strings.Contains(err.Error(), "hex output") {
		t.Fatalf("non-hex bytea accepted: %v", err)
	}

	partial := &RawRows{}
	partial.Rows = append(partial.Rows, gbRow(t, "0/501", "9", gbBegin(t, "0/501")))
	partial.Rows = append(partial.Rows, gbRow(t, "0/502", "9", gbDML(t, "0/502", 'I', "s", "t", []gbCol{{name: "id", oid: 23, val: []byte("1")}}, nil)))
	if _, err := ParseGaussDBBinaryRows(partial, "slot"); err == nil || !strings.Contains(err.Error(), "before COMMIT") {
		t.Fatalf("partial binary transaction accepted: %v", err)
	}
}

func TestGaussDBBinaryDecodeQueryValidation(t *testing.T) {
	q, err := gaussDBBinaryDecodeQuery("pg_logical_slot_peek_binary_changes", "slot_1", 123, "0/16B6C50", []string{"public.orders"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"pg_logical_slot_peek_binary_changes", "encode(data,'hex')", "0/16B6C50", "white-table-list", "public.orders", "enable-ddl-decoding", "false"} {
		if !strings.Contains(q, want) {
			t.Fatalf("binary query missing %q: %s", want, q)
		}
	}
	if _, err := gaussDBBinaryDecodeQuery("pg_logical_slot_peek_changes", "slot", 1, "", []string{"public.t"}); err == nil {
		t.Fatal("text logical function accepted by binary query builder")
	}
}

func TestGaussDBDDLDecodeQueryAndSafeSubset(t *testing.T) {
	q, err := gaussDBDDLDecodeQuery("pg_logical_slot_peek_changes", "slot_ddl", 64, "0/600", []string{"public.orders"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"pg_logical_slot_peek_changes", "enable-ddl-decoding", "true", "enable-ddl-json-format", "false", "public.orders", "0/600"} {
		if !strings.Contains(q, want) {
			t.Fatalf("DDL query missing %q: %s", want, q)
		}
	}
	for _, ddl := range []string{
		"ALTER TABLE public.orders ADD COLUMN note varchar(32)",
		"ALTER TABLE IF EXISTS public.orders DROP COLUMN note",
		"TRUNCATE TABLE public.orders",
		"CREATE INDEX idx_orders_note ON public.orders(note)",
		"CREATE UNIQUE INDEX idx_orders_id ON public.orders(id)",
	} {
		if err := gaussDBSafeDDL(ddl, []string{"public.orders"}); err != nil {
			t.Fatalf("safe DDL rejected %q: %v", ddl, err)
		}
	}
	for _, ddl := range []string{
		"DROP TABLE public.orders",
		"ALTER TABLE public.other ADD COLUMN x int",
		"CREATE INDEX idx_other ON public.other(id)",
		"CREATE INDEX CONCURRENTLY idx_orders ON public.orders(id)",
		"CREATE SEQUENCE public.seq1",
	} {
		if err := gaussDBSafeDDL(ddl, []string{"public.orders"}); err == nil {
			t.Fatalf("unsafe DDL accepted %q", ddl)
		}
	}
}

func TestParseGaussDBDDLOnlyTransaction(t *testing.T) {
	rows := &RawRows{Rows: [][][]byte{
		{gv("0/601"), gv("88"), gv("BEGIN 88")},
		{gv("0/602"), gv("88"), gv(`{"TDDL":"ALTER TABLE public.orders ADD COLUMN note pg_catalog.varchar(32)"}`)},
		{gv("0/603"), gv("88"), gv("COMMIT 88")},
	}}
	txs, err := parseGaussDBDDLRows(rows, "slot_ddl", []string{"public.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 1 || txs[0].XID != "88" || txs[0].CommitLSN != "0/603" || txs[0].HasDML || len(txs[0].DDL) != 1 {
		t.Fatalf("summaries=%+v", txs)
	}
	ev := txs[0].DDL[0]
	if ev.Operation != domain.CDCDDL || ev.SQL == "" || ev.PositionType != "GAUSSDB_LSN" || ev.PositionValue != "0/603" || ev.Resource != "slot_ddl" {
		t.Fatalf("ddl event=%+v", ev)
	}
}

func TestParseGaussDBDDLMixedTransactionPreservesTemplate(t *testing.T) {
	rows := &RawRows{Rows: [][][]byte{
		{gv("0/611"), gv("89"), gv("BEGIN 89")},
		{gv("0/612"), gv("89"), gv(`{"table_name":"public.orders","op_type":"INSERT"}`)},
		{gv("0/613"), gv("89"), gv(`{"TDDL":"ALTER TABLE public.orders ADD COLUMN note pg_catalog.varchar(32)"}`)},
		{gv("0/614"), gv("89"), gv("COMMIT 89")},
	}}
	got, err := parseGaussDBDDLRows(rows, "slot_ddl", []string{"public.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].DMLCount != 1 || len(got[0].Sequence) != 2 || got[0].Sequence[0] != -1 || got[0].Sequence[1] != 0 {
		t.Fatalf("template=%+v", got)
	}
}

func TestMergeGaussDBDDLAndBinaryKeepsCommitOrder(t *testing.T) {
	summaries := []gaussDBDDLTxnSummary{
		{XID: "90", CommitLSN: "0/620", DDL: []domain.CDCEvent{{Operation: domain.CDCDDL, SQL: "TRUNCATE TABLE public.orders"}}},
		{XID: "91", CommitLSN: "0/630", HasDML: true, DMLCount: 1, Sequence: []int{-1}},
	}
	binaryTx := []GaussDBTransaction{{XID: "91", CommitLSN: "0/630", Events: []domain.CDCEvent{{Operation: domain.CDCInsert, SourceSchema: "public", SourceTable: "orders"}}}}
	out, err := mergeGaussDBDDLAndBinary(summaries, binaryTx)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || !gaussDBTransactionHasDDL(out[0]) || out[1].Events[0].Operation != domain.CDCInsert {
		t.Fatalf("merged=%+v", out)
	}
	if _, err := mergeGaussDBDDLAndBinary(summaries, nil); err == nil || !strings.Contains(err.Error(), "did not return DML transaction") {
		t.Fatalf("missing binary transaction accepted: %v", err)
	}
}

func TestGaussDBHybridDDLAndDMLReconstruction(t *testing.T) {
	s := gaussDBDDLTxnSummary{XID: "42", CommitLSN: "0/20", DDL: []domain.CDCEvent{{Operation: domain.CDCDDL, SQL: "ALTER TABLE public.t ADD COLUMN c int"}}, Sequence: []int{0, -1}, DMLCount: 1, HasDML: true}
	b := GaussDBTransaction{XID: "42", CSN: 99, CommitLSN: "0/20", Events: []domain.CDCEvent{{Operation: domain.CDCUpdate, SourceSchema: "public", SourceTable: "t"}}}
	got, err := mergeGaussDBDDLAndBinary([]gaussDBDDLTxnSummary{s}, []GaussDBTransaction{b})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].CSN != 99 || len(got[0].Events) != 2 || got[0].Events[0].Operation != domain.CDCDDL || got[0].Events[1].Operation != domain.CDCUpdate {
		t.Fatalf("unexpected %#v", got)
	}
	s.DMLCount = 2
	if _, err := mergeGaussDBDDLAndBinary([]gaussDBDDLTxnSummary{s}, []GaussDBTransaction{b}); err == nil {
		t.Fatal("expected cardinality mismatch")
	}
}
