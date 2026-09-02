package damenglog

import (
	"context"
	"testing"
	"time"

	cdcruntime "qmigration/backend/internal/cdc/runtime"
	damengconnector "qmigration/backend/internal/connector/dameng"
	"qmigration/backend/internal/domain"
)

type fakeSource struct {
	positions []*domain.CDCPosition
	txs       []damengconnector.DamengCDCTransaction
	calls     int
	closed    bool
}

func (f *fakeSource) CurrentCDCPosition(context.Context) (*domain.CDCPosition, error) {
	if len(f.positions) == 0 {
		return &domain.CDCPosition{PositionType: "DM_LSN", PositionValue: "110"}, nil
	}
	p := f.positions[0]
	if len(f.positions) > 1 {
		f.positions = f.positions[1:]
	}
	return p, nil
}
func (f *fakeSource) ReadLogMinerTransactions(_ context.Context, from, to string, selected map[string]bool) ([]damengconnector.DamengCDCTransaction, error) {
	f.calls++
	if from != "100" || to != "110" {
		return nil, &testErr{"unexpected window " + from + ".." + to}
	}
	if !selected["app.t"] {
		return nil, &testErr{"selection missing"}
	}
	return f.txs, nil
}
func (f *fakeSource) Close() error { f.closed = true; return nil }

type testErr struct{ s string }

func (e *testErr) Error() string { return e.s }

func TestReaderApplyBeforeAckCheckpoint(t *testing.T) {
	src := &fakeSource{positions: []*domain.CDCPosition{{PositionType: "DM_LSN", PositionValue: "110"}}, txs: []damengconnector.DamengCDCTransaction{
		{LSN: "105", Events: []domain.CDCEvent{{Operation: domain.CDCInsert, SourceSchema: "APP", SourceTable: "T", PositionType: "DM_LSN", PositionValue: "105"}}},
		{LSN: "110", Events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "DM_LSN", PositionValue: "110"}}},
	}}
	r, err := NewReader(src, "100", []string{"APP.T"}, time.Millisecond, 1000)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil || tx.Checkpoint.PositionValue != "105" {
		t.Fatalf("tx=%+v err=%v", tx, err)
	}
	// A second Next may be read before ACK, but durable reconnect state must not move.
	if r.acknowledged != "100" {
		t.Fatalf("ack moved before acknowledge: %s", r.acknowledged)
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if r.acknowledged != "105" {
		t.Fatalf("ack=%s", r.acknowledged)
	}
	tx, err = r.Next(context.Background())
	if err != nil || tx.Checkpoint.PositionValue != "110" {
		t.Fatalf("checkpoint tx=%+v err=%v", tx, err)
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if r.acknowledged != "110" || src.calls != 1 {
		t.Fatalf("ack=%s calls=%d", r.acknowledged, src.calls)
	}
}

func TestReaderRejectsBadStartAndRegressingAck(t *testing.T) {
	if _, err := NewReader(&fakeSource{}, "0", []string{"APP.T"}, 0, 0); err == nil {
		t.Fatal("zero LSN accepted")
	}
	r, err := NewReader(&fakeSource{}, "100", []string{"APP.T"}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Acknowledge(context.Background(), &cdcruntime.Transaction{Checkpoint: domain.CDCPosition{PositionType: "DM_LSN", PositionValue: "99"}}); err == nil {
		t.Fatal("regressing ack accepted")
	}
}
