package gbase8scdc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"qmigration/backend/internal/domain"
)

var testCaptureLineage = strings.Repeat("a", CaptureLineageHexLength)

func testStartPosition() string {
	return "restart=10;commit=10;capture=" + testCaptureLineage
}

type fakeAgent struct {
	reads      []*ReadResponse
	i          int
	omitFences bool
}

func (f *fakeAgent) Health(context.Context) error { return nil }
func (f *fakeAgent) Checkpoint(_ context.Context, req CheckpointRequest) (*CheckpointResponse, error) {
	out := &CheckpointResponse{Sequence: "10", CaptureLineage: testCaptureLineage}
	if selectionsRequireSmartLOB(req.Tables) {
		out.SmartLOBImageContract = SmartLOBImageContract
	}
	if !f.omitFences {
		out.SchemaFences = SchemaFencesForSelections(req.Tables)
	}
	return out, nil
}
func (f *fakeAgent) Read(_ context.Context, req ReadRequest) (*ReadResponse, error) {
	if f.i >= len(f.reads) {
		out := &ReadResponse{CaptureLineage: testCaptureLineage}
		if selectionsRequireSmartLOB(req.Tables) {
			out.SmartLOBImageContract = SmartLOBImageContract
		}
		if !f.omitFences {
			out.SchemaFences = SchemaFencesForSelections(req.Tables)
		}
		return out, nil
	}
	r := f.reads[f.i]
	f.i++
	if strings.TrimSpace(r.CaptureLineage) == "" {
		r.CaptureLineage = testCaptureLineage
	}
	if selectionsRequireSmartLOB(req.Tables) && strings.TrimSpace(r.SmartLOBImageContract) == "" {
		r.SmartLOBImageContract = SmartLOBImageContract
	}
	if !f.omitFences && r.SchemaFences == nil {
		r.SchemaFences = SchemaFencesForSelections(req.Tables)
	}
	return r, nil
}
func fld(c, v string) domain.CDCField { return domain.CDCField{Column: c, Value: v} }
func sel() []TableSelection {
	s, err := BuildTableSelection("app", "orders", []domain.ColumnInfo{
		{Name: "id", DataType: "integer", ColumnType: "INTEGER", Nullable: false, Ordinal: 1, PrimaryKey: true},
		{Name: "v", DataType: "varchar", ColumnType: "VARCHAR(64)", Nullable: true, Ordinal: 2},
	}, []string{"id"})
	if err != nil {
		panic(err)
	}
	return []TableSelection{s}
}

func lobSel() []TableSelection {
	s, err := BuildTableSelection("app", "lob_orders", []domain.ColumnInfo{
		{Name: "id", DataType: "integer", ColumnType: "INTEGER", Nullable: false, Ordinal: 1, PrimaryKey: true},
		{Name: "payload", DataType: "blob", ColumnType: "BLOB", Nullable: true, Ordinal: 2},
		{Name: "note", DataType: "clob", ColumnType: "CLOB", Nullable: true, Ordinal: 3},
	}, []string{"id"})
	if err != nil {
		panic(err)
	}
	return []TableSelection{s}
}

func lobProof(column, kind string, b []byte) SmartLOBImageProof {
	return SmartLOBImageProof{Column: column, Kind: kind, ByteLength: int64(len(b)), SHA256: fmt.Sprintf("%x", sha256.Sum256(b)), Acquisition: SmartLOBImageContract}
}

func TestReaderLongTransactionRestartWatermark(t *testing.T) {
	id := StableTableID("app", "orders")
	a := &fakeAgent{reads: []*ReadResponse{{NextSequence: "121", Records: []RecordEnvelope{
		{Kind: KindBegin, Sequence: "50", TransactionID: 1},
		{Kind: KindInsert, Sequence: "60", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("id", "1"), fld("v", "long")}},
		{Kind: KindBegin, Sequence: "80", TransactionID: 2},
		{Kind: KindInsert, Sequence: "90", TransactionID: 2, TableID: id, Fields: []domain.CDCField{fld("id", "2"), fld("v", "short")}},
		{Kind: KindCommit, Sequence: "100", TransactionID: 2},
		{Kind: KindCommit, Sequence: "120", TransactionID: 1},
	}}}}
	r, err := NewReader(a, "app", testStartPosition(), sel(), "agent")
	if err != nil {
		t.Fatal(err)
	}
	r.SetPolling(time.Millisecond, 0, 0)
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := tx.Checkpoint.PositionValue; got != "restart=50;commit=100;capture="+testCaptureLineage {
		t.Fatalf("checkpoint=%s", got)
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	tx, err = r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := tx.Checkpoint.PositionValue; got != "restart=120;commit=120;capture="+testCaptureLineage {
		t.Fatalf("checkpoint2=%s", got)
	}
}

func TestReaderUpdateDiscardRollbackAndBinaryField(t *testing.T) {
	id := StableTableID("app", "orders")
	a := &fakeAgent{reads: []*ReadResponse{{NextSequence: "50", Records: []RecordEnvelope{
		{Kind: KindBegin, Sequence: "11", TransactionID: 7},
		{Kind: KindInsert, Sequence: "12", TransactionID: 7, TableID: id, Fields: []domain.CDCField{fld("id", "1"), {Column: "v", Value: "AAEC", Encoding: "base64"}}},
		{Kind: KindUpdateBefore, Sequence: "13", TransactionID: 7, TableID: id, Fields: []domain.CDCField{fld("id", "1"), fld("v", "a")}},
		{Kind: KindUpdateAfter, Sequence: "14", TransactionID: 7, TableID: id, Fields: []domain.CDCField{fld("id", "1"), fld("v", "b")}},
		{Kind: KindDiscard, Sequence: "14", TransactionID: 7},
		{Kind: KindCommit, Sequence: "20", TransactionID: 7},
	}}}}
	r, _ := NewReader(a, "app", testStartPosition(), sel(), "agent")
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCInsert || tx.Events[0].After[1].Encoding != "base64" {
		t.Fatalf("events=%+v", tx.Events)
	}
}

func TestReaderEmitsTransactionalTruncate(t *testing.T) {
	id := StableTableID("app", "orders")
	a := &fakeAgent{reads: []*ReadResponse{{NextSequence: "20", Records: []RecordEnvelope{
		{Kind: KindBegin, Sequence: "11", TransactionID: 1},
		{Kind: KindInsert, Sequence: "12", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("id", "1"), fld("v", "before-truncate")}},
		{Kind: KindTruncate, Sequence: "13", TransactionID: 1, TableID: id},
		{Kind: KindCommit, Sequence: "14", TransactionID: 1},
	}}}}
	r, _ := NewReader(a, "app", testStartPosition(), sel(), "agent")
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 2 || tx.Events[0].Operation != domain.CDCInsert || tx.Events[1].Operation != domain.CDCTruncate {
		t.Fatalf("events=%+v", tx.Events)
	}
	if tx.Events[1].SourceSchema != "app" || tx.Events[1].SourceTable != "orders" || tx.Events[1].PositionValue != "restart=14;commit=14;capture="+testCaptureLineage {
		t.Fatalf("truncate=%+v", tx.Events[1])
	}
}

func TestReaderRejectsRecordsAfterTruncateAndMalformedTruncate(t *testing.T) {
	id := StableTableID("app", "orders")
	cases := [][]RecordEnvelope{
		{{Kind: KindBegin, Sequence: "11", TransactionID: 1}, {Kind: KindTruncate, Sequence: "12", TransactionID: 1, TableID: id}, {Kind: KindInsert, Sequence: "13", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("id", "1"), fld("v", "x")}}},
		{{Kind: KindBegin, Sequence: "11", TransactionID: 1}, {Kind: KindTruncate, Sequence: "12", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("id", "1")}}},
		{{Kind: KindBegin, Sequence: "11", TransactionID: 1}, {Kind: KindTruncate, Sequence: "12", TransactionID: 1, TableID: id}, {Kind: KindTruncate, Sequence: "13", TransactionID: 1, TableID: id}},
		{{Kind: KindBegin, Sequence: "11", TransactionID: 1}, {Kind: KindUpdateAfter, Sequence: "12", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("id", "1")}}},
	}
	for i, records := range cases {
		a := &fakeAgent{reads: []*ReadResponse{{NextSequence: "20", Records: records}}}
		r, _ := NewReader(a, "app", testStartPosition(), sel(), "agent")
		if _, err := r.Next(context.Background()); err == nil {
			t.Fatalf("case %d expected fail closed", i)
		}
	}
}

func TestReaderRequiresProviderNextSequence(t *testing.T) {
	a := &fakeAgent{reads: []*ReadResponse{{Records: []RecordEnvelope{{Kind: KindBegin, Sequence: "11", TransactionID: 1}}}}}
	r, _ := NewReader(a, "app", testStartPosition(), sel(), "agent")
	_, err := r.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "next_sequence") {
		t.Fatalf("err=%v", err)
	}
}

func TestStableTableID(t *testing.T) {
	if StableTableID("APP", "Orders") != StableTableID("app", "orders") || StableTableID("app", "orders") == StableTableID("app", "other") {
		t.Fatal("unstable table id")
	}
}

func TestReaderEmitsCheckpointForEmptyCommittedTransaction(t *testing.T) {
	id := StableTableID("app", "orders")
	a := &fakeAgent{reads: []*ReadResponse{{NextSequence: "31", Records: []RecordEnvelope{
		{Kind: KindBegin, Sequence: "20", TransactionID: 11},
		{Kind: KindCommit, Sequence: "30", TransactionID: 11},
		{Kind: KindBegin, Sequence: "31", TransactionID: 12},
		{Kind: KindInsert, Sequence: "32", TransactionID: 12, TableID: id, Fields: []domain.CDCField{fld("id", "1"), fld("v", "x")}},
		{Kind: KindCommit, Sequence: "40", TransactionID: 12},
	}}}}
	r, err := NewReader(a, "app", testStartPosition(), sel(), "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCCheckpoint || tx.Checkpoint.PositionValue != "restart=30;commit=30;capture="+testCaptureLineage {
		t.Fatalf("checkpoint tx=%+v", tx)
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
}

func TestReaderValidatesProviderFullRowFields(t *testing.T) {
	id := StableTableID("app", "orders")
	cases := []RecordEnvelope{
		{Kind: KindInsert, Sequence: "12", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("id", "1")}},
		{Kind: KindInsert, Sequence: "12", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("v", "x"), fld("id", "1")}},
		{Kind: KindInsert, Sequence: "12", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("id", "1"), {Column: "v", Value: "%%%", Encoding: "base64"}}},
		{Kind: KindInsert, Sequence: "12", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("id", "1"), {Column: "v", Null: true, Value: "x"}}},
	}
	for _, bad := range cases {
		a := &fakeAgent{reads: []*ReadResponse{{NextSequence: "20", Records: []RecordEnvelope{{Kind: KindBegin, Sequence: "11", TransactionID: 1}, bad}}}}
		r, _ := NewReader(a, "app", testStartPosition(), sel(), "agent")
		if _, err := r.Next(context.Background()); err == nil {
			t.Fatalf("expected provider field validation failure for %+v", bad.Fields)
		}
	}
}

func TestReaderEnforcesProviderBatchLimits(t *testing.T) {
	id := StableTableID("app", "orders")
	a := &fakeAgent{reads: []*ReadResponse{{NextSequence: "20", Records: []RecordEnvelope{
		{Kind: KindBegin, Sequence: "11", TransactionID: 1},
		{Kind: KindInsert, Sequence: "12", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("id", "1"), fld("v", "x")}},
	}}}}
	r, _ := NewReader(a, "app", testStartPosition(), sel(), "agent")
	r.SetPolling(time.Millisecond, 1, 1<<20)
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "max_records") {
		t.Fatalf("err=%v", err)
	}
}

func TestReaderRejectsMissingAndMismatchedSchemaFence(t *testing.T) {
	id := StableTableID("app", "orders")
	records := []RecordEnvelope{{Kind: KindBegin, Sequence: "11", TransactionID: 1}, {Kind: KindInsert, Sequence: "12", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("id", "1"), fld("v", "x")}}}
	for _, a := range []*fakeAgent{
		{omitFences: true, reads: []*ReadResponse{{NextSequence: "20", Records: records}}},
		{reads: []*ReadResponse{{NextSequence: "20", SchemaFences: []SchemaFence{{TableID: id, Fingerprint: strings.Repeat("0", 64)}}, Records: records}}},
	} {
		r, err := NewReader(a, "app", testStartPosition(), sel(), "agent")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.Next(context.Background()); err == nil || !strings.Contains(strings.ToLower(err.Error()), "schema") {
			t.Fatalf("expected schema fence failure, got %v", err)
		}
	}
}

func TestReaderValidatesTableSchemaRecord(t *testing.T) {
	s := sel()
	id := s[0].ID
	a := &fakeAgent{reads: []*ReadResponse{{NextSequence: "20", Records: []RecordEnvelope{
		{Kind: KindTableSchema, TableID: id, SchemaFingerprint: s[0].SchemaFingerprint},
		{Kind: KindBegin, Sequence: "11", TransactionID: 1},
		{Kind: KindCommit, Sequence: "12", TransactionID: 1},
	}}}}
	r, err := NewReader(a, "app", testStartPosition(), s, "agent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); err != nil {
		t.Fatal(err)
	}

	a = &fakeAgent{reads: []*ReadResponse{{NextSequence: "20", Records: []RecordEnvelope{{Kind: KindTableSchema, TableID: id, SchemaFingerprint: strings.Repeat("f", 64)}}}}}
	r, _ = NewReader(a, "app", testStartPosition(), s, "agent")
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(strings.ToLower(err.Error()), "drift") {
		t.Fatalf("expected TABLE_SCHEMA drift failure, got %v", err)
	}
}

func TestReaderRejectsLegacyCheckpointWithoutLineage(t *testing.T) {
	if _, err := NewReader(&fakeAgent{}, "app", "restart=10;commit=10", sel(), "agent"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "lineage") {
		t.Fatalf("expected legacy checkpoint rejection, got %v", err)
	}
}

func TestReaderRejectsCaptureLineageChangeBeforeApply(t *testing.T) {
	id := StableTableID("app", "orders")
	other := strings.Repeat("b", CaptureLineageHexLength)
	a := &fakeAgent{reads: []*ReadResponse{{
		CaptureLineage: other,
		NextSequence:   "20",
		Records: []RecordEnvelope{
			{Kind: KindBegin, Sequence: "11", TransactionID: 1},
			{Kind: KindInsert, Sequence: "12", TransactionID: 1, TableID: id, Fields: []domain.CDCField{fld("id", "1"), fld("v", "x")}},
		},
	}}}
	r, err := NewReader(a, "app", testStartPosition(), sel(), "agent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(strings.ToLower(err.Error()), "lineage changed") {
		t.Fatalf("expected capture-lineage fail closed, got %v", err)
	}
}

func TestParsePositionRejectsRestartAfterCommit(t *testing.T) {
	if _, err := parsePosition("restart=20;commit=10"); err == nil {
		t.Fatal("expected invalid restart/commit order")
	}
}

func TestReaderAcceptsEventOwnedSmartLOBImages(t *testing.T) {
	s := lobSel()
	id := s[0].ID
	blob := []byte{0, 1, 2, 3, 255}
	clob := []byte("历史版本-一")
	a := &fakeAgent{reads: []*ReadResponse{{NextSequence: "20", SmartLOBImageContract: SmartLOBImageContract, Records: []RecordEnvelope{
		{Kind: KindBegin, Sequence: "11", TransactionID: 1},
		{Kind: KindInsert, Sequence: "12", TransactionID: 1, TableID: id,
			Fields:         []domain.CDCField{fld("id", "1"), {Column: "payload", Value: base64.StdEncoding.EncodeToString(blob), Encoding: "base64"}, {Column: "note", Value: string(clob)}},
			SmartLOBProofs: []SmartLOBImageProof{lobProof("payload", "blob", blob), lobProof("note", "clob", clob)}},
		{Kind: KindCommit, Sequence: "13", TransactionID: 1},
	}}}}
	r, err := NewReader(a, "app", testStartPosition(), s, "agent")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || len(tx.Events[0].After) != 3 || tx.Events[0].After[1].Encoding != "base64" {
		t.Fatalf("unexpected smart-LOB event: %+v", tx.Events)
	}
}

func TestReaderRejectsUnsafeOrCorruptSmartLOBImages(t *testing.T) {
	s := lobSel()
	id := s[0].ID
	blob := []byte("blob-v1")
	base := RecordEnvelope{Kind: KindInsert, Sequence: "12", TransactionID: 1, TableID: id,
		Fields: []domain.CDCField{fld("id", "1"), {Column: "payload", Value: base64.StdEncoding.EncodeToString(blob), Encoding: "base64"}, {Column: "note", Null: true}}}
	cases := []RecordEnvelope{}
	missing := base
	cases = append(cases, missing)
	unsafe := base
	p := lobProof("payload", "blob", blob)
	p.Acquisition = "select-current-row"
	unsafe.SmartLOBProofs = []SmartLOBImageProof{p}
	cases = append(cases, unsafe)
	badHash := base
	p = lobProof("payload", "blob", blob)
	p.SHA256 = strings.Repeat("0", 64)
	badHash.SmartLOBProofs = []SmartLOBImageProof{p}
	cases = append(cases, badHash)
	badLen := base
	p = lobProof("payload", "blob", blob)
	p.ByteLength++
	badLen.SmartLOBProofs = []SmartLOBImageProof{p}
	cases = append(cases, badLen)
	for i, rec := range cases {
		a := &fakeAgent{reads: []*ReadResponse{{NextSequence: "20", SmartLOBImageContract: SmartLOBImageContract, Records: []RecordEnvelope{{Kind: KindBegin, Sequence: "11", TransactionID: 1}, rec}}}}
		r, _ := NewReader(a, "app", testStartPosition(), s, "agent")
		if _, err := r.Next(context.Background()); err == nil {
			t.Fatalf("case %d expected fail closed", i)
		}
	}
}

func TestReadAndCheckpointRequireSmartLOBImageContract(t *testing.T) {
	s := lobSel()
	cp := &CheckpointResponse{Sequence: "10", CaptureLineage: testCaptureLineage, SchemaFences: SchemaFencesForSelections(s)}
	if err := ValidateCheckpointResponse(CheckpointRequest{Database: "app", Tables: s}, cp); err == nil || !strings.Contains(err.Error(), "smart-LOB") {
		t.Fatalf("checkpoint contract err=%v", err)
	}
	rr := &ReadResponse{CaptureLineage: testCaptureLineage, SchemaFences: SchemaFencesForSelections(s)}
	if err := ValidateReadResponse(ReadRequest{Database: "app", ExpectedCaptureLineage: testCaptureLineage, Tables: s}, rr); err == nil || !strings.Contains(err.Error(), "smart-LOB") {
		t.Fatalf("read contract err=%v", err)
	}
}
