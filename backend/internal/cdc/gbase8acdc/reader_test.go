package gbase8acdc

import (
	"context"
	"strings"
	"testing"

	"qmigration/backend/internal/domain"
)

type fakeAgent struct {
	cp   *CheckpointResponse
	resp *ReadResponse
	acks []AckRequest
}

func (f *fakeAgent) Health(context.Context) error { return nil }
func (f *fakeAgent) Checkpoint(context.Context, CheckpointRequest) (*CheckpointResponse, error) {
	return f.cp, nil
}
func (f *fakeAgent) Read(context.Context, ReadRequest) (*ReadResponse, error) {
	r := f.resp
	f.resp = &ReadResponse{}
	return r, nil
}
func (f *fakeAgent) Ack(_ context.Context, r AckRequest) error {
	f.acks = append(f.acks, r)
	return nil
}

func testSelection(t *testing.T) TableSelection {
	t.Helper()
	s, err := BuildTableSelection("app", "orders", []domain.ColumnInfo{{Name: "id", DataType: "bigint", ColumnType: "BIGINT", PrimaryKey: true}, {Name: "v", DataType: "varchar", ColumnType: "VARCHAR(64)", Nullable: true}}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestReaderRequiresProofsAndAcksAfterApply(t *testing.T) {
	sel := testSelection(t)
	lineage := strings.Repeat("ab", 32)
	f := &fakeAgent{resp: &ReadResponse{ResolvedSequence: "11", Transactions: []TransactionEnvelope{{Sequence: "11", TransactionID: "tx1", CaptureLineage: lineage, Atomicity: "COMMITTED_TXN_V1", SchemaFences: []SchemaFence{{TableID: sel.ID, Fingerprint: sel.SchemaFingerprint}}, Events: []domain.CDCEvent{{Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "v", Value: "x"}}}}}}}}
	r, err := NewReader(f, "app", FormatPosition(10, lineage), []TableSelection{sel}, "provider")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tx.Checkpoint.PositionType != "GBASE8A_CDC_SEQ" || len(f.acks) != 0 {
		t.Fatalf("tx=%+v acks=%v", tx, f.acks)
	}
	if err = r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if len(f.acks) != 1 || f.acks[0].Sequence != "11" {
		t.Fatalf("acks=%+v", f.acks)
	}
}
func TestReaderRejectsIncompleteRowImage(t *testing.T) {
	sel := testSelection(t)
	lineage := strings.Repeat("cd", 32)
	f := &fakeAgent{resp: &ReadResponse{Transactions: []TransactionEnvelope{{Sequence: "2", TransactionID: "tx", CaptureLineage: lineage, Atomicity: "COMMITTED_TXN_V1", SchemaFences: []SchemaFence{{TableID: sel.ID, Fingerprint: sel.SchemaFingerprint}}, Events: []domain.CDCEvent{{Operation: domain.CDCUpdate, SourceSchema: "app", SourceTable: "orders", Before: []domain.CDCField{{Column: "id", Value: "1"}}, After: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "v", Value: "y"}}}}}}}}
	r, _ := NewReader(f, "app", FormatPosition(1, lineage), []TableSelection{sel}, "provider")
	if _, err := r.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "complete before image") {
		t.Fatalf("err=%v", err)
	}
}
func TestReaderRejectsLineageChangeAndNonAtomicProvider(t *testing.T) {
	sel := testSelection(t)
	lineage := strings.Repeat("ef", 32)
	other := strings.Repeat("01", 32)
	for _, tc := range []TransactionEnvelope{{Sequence: "2", TransactionID: "tx", CaptureLineage: other, Atomicity: "COMMITTED_TXN_V1", SchemaFences: []SchemaFence{{TableID: sel.ID, Fingerprint: sel.SchemaFingerprint}}, Events: []domain.CDCEvent{{Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "v", Value: "x"}}}}}, {Sequence: "2", TransactionID: "tx", CaptureLineage: lineage, Atomicity: "ROW_STREAM", SchemaFences: []SchemaFence{{TableID: sel.ID, Fingerprint: sel.SchemaFingerprint}}, Events: []domain.CDCEvent{{Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}, {Column: "v", Value: "x"}}}}}} {
		f := &fakeAgent{resp: &ReadResponse{Transactions: []TransactionEnvelope{tc}}}
		r, _ := NewReader(f, "app", FormatPosition(1, lineage), []TableSelection{sel}, "provider")
		if _, err := r.Next(context.Background()); err == nil {
			t.Fatalf("accepted %+v", tc)
		}
	}
}
