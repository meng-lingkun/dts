package sqlservercdc

import (
	"context"
	"errors"
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/connector"
	sqlserverconnector "qmigration/backend/internal/connector/sqlserver"
	"qmigration/backend/internal/domain"
	"testing"
	"time"
)

type fakeSource struct {
	windows int
	closed  bool
}

func (f *fakeSource) DiscoverCDCCaptures(context.Context, map[string]bool) ([]sqlserverconnector.CDCCapture, error) {
	return []sqlserverconnector.CDCCapture{{Schema: "dbo", Table: "t", Instance: "dbo_t", Columns: []domain.ColumnInfo{{Name: "id", DataType: "int"}}}}, nil
}
func (f *fakeSource) ValidateCDCStart(context.Context, []sqlserverconnector.CDCCapture, string) error {
	return nil
}
func (f *fakeSource) NextCDCWindow(context.Context, string, int) (string, string, bool, error) {
	f.windows++
	if f.windows > 1 {
		return "", "", true, nil
	}
	return "0x00000000000000000002", "0x00000000000000000003", false, nil
}
func (f *fakeSource) ReadCDCChanges(context.Context, sqlserverconnector.CDCCapture, string, string) ([]sqlserverconnector.CDCChange, error) {
	return []sqlserverconnector.CDCChange{{StartLSN: "0x00000000000000000002", SeqVal: "0x00000000000000000001", Operation: 2, Schema: "dbo", Table: "t", Capture: "dbo_t", Columns: []domain.ColumnInfo{{Name: "id", DataType: "int"}}, Values: []connector.Value{{Raw: []byte("7")}}}}, nil
}
func (f *fakeSource) Close() error { f.closed = true; return nil }
func TestReaderAckAdvancesOnlyAfterRunnerApply(t *testing.T) {
	src := &fakeSource{}
	r := NewReader(src, "0x00000000000000000001", []string{"dbo.t"}, time.Millisecond, 8)
	applied := false
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := (cdcruntime.Runner{Reader: r, Apply: func(context.Context, []domain.CDCEvent) (*domain.CDCApplyResult, error) {
		if applied {
			return nil, errors.New("unexpected second apply")
		}
		applied = true
		return &domain.CDCApplyResult{Applied: 1}, nil
	}, Observe: func(tx *cdcruntime.Transaction, _ *domain.CDCApplyResult) {
		if tx.Checkpoint.PositionValue != "0x00000000000000000002" {
			t.Fatalf("checkpoint=%s", tx.Checkpoint.PositionValue)
		}
	}}).Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runner err=%v", err)
	}
	if !applied {
		t.Fatal("transaction was not applied")
	}
}

type emptySelectedSource struct{ fakeSource }

func (f *emptySelectedSource) ReadCDCChanges(context.Context, sqlserverconnector.CDCCapture, string, string) ([]sqlserverconnector.CDCChange, error) {
	return nil, nil
}

func TestReaderPersistsCheckpointForUnrelatedLSNWindow(t *testing.T) {
	src := &emptySelectedSource{}
	r := NewReader(src, "0x00000000000000000001", []string{"dbo.t"}, time.Millisecond, 8)
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tx.Checkpoint.PositionValue != "0x00000000000000000003" || len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCCheckpoint {
		t.Fatalf("tx=%+v", tx)
	}
	if r.acknowledged != "0x00000000000000000001" {
		t.Fatalf("reader advanced before durable apply: %s", r.acknowledged)
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if r.acknowledged != "0x00000000000000000003" {
		t.Fatalf("reader did not advance after acknowledge: %s", r.acknowledged)
	}
}
