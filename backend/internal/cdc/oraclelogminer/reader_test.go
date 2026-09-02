package oraclelogminer

import (
	"context"
	"errors"
	"testing"
	"time"

	oracleconnector "qmigration/backend/internal/connector/oracle"
	"qmigration/backend/internal/domain"
)

type fakeSource struct {
	current string
	txs     []oracleconnector.OracleCDCTransaction
	calls   [][2]string
	closed  bool
}

func (f *fakeSource) CurrentCDCPosition(context.Context) (*domain.CDCPosition, error) {
	return &domain.CDCPosition{PositionType: "ORACLE_SCN", PositionValue: f.current}, nil
}
func (f *fakeSource) ReadLogMinerTransactions(_ context.Context, from, to string, _ map[string]bool) ([]oracleconnector.OracleCDCTransaction, error) {
	f.calls = append(f.calls, [2]string{from, to})
	return append([]oracleconnector.OracleCDCTransaction(nil), f.txs...), nil
}
func (f *fakeSource) Close() error { f.closed = true; return nil }

func TestReaderBoundsSCNWindowAndAdvancesOnlyOnAck(t *testing.T) {
	f := &fakeSource{current: "250", txs: []oracleconnector.OracleCDCTransaction{{SCN: "150", TimestampMS: 1234, Events: []domain.CDCEvent{{ID: "e1", Operation: domain.CDCInsert}}}}}
	r := NewReader(f, "100", []string{"APP.ORDERS"}, time.Millisecond, 50)
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 1 || f.calls[0] != [2]string{"100", "150"} {
		t.Fatalf("calls=%v", f.calls)
	}
	if tx.Checkpoint.PositionValue != "150" || tx.Checkpoint.SourceTimestampMS != 1234 {
		t.Fatalf("tx=%+v", tx)
	}
	if r.acknowledged != "100" {
		t.Fatalf("advanced before ack: %s", r.acknowledged)
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if r.acknowledged != "150" {
		t.Fatalf("ack=%s", r.acknowledged)
	}
}

func TestReaderEmitsCheckpointForEmptyWindow(t *testing.T) {
	f := &fakeSource{current: "120"}
	r := NewReader(f, "100", nil, time.Millisecond, 1000)
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tx.Checkpoint.PositionValue != "120" || len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCCheckpoint {
		t.Fatalf("tx=%+v", tx)
	}
}

func TestReaderRejectsBadSCNAndAck(t *testing.T) {
	f := &fakeSource{current: "bad"}
	r := NewReader(f, "100", nil, time.Millisecond, 1)
	if _, err := r.Next(context.Background()); err == nil {
		t.Fatal("expected bad current SCN error")
	}
	if err := r.Acknowledge(context.Background(), nil); err == nil {
		t.Fatal("expected nil ack error")
	}
	f.current = "101"
	r.acknowledged = "bad"
	if _, err := r.Next(context.Background()); err == nil {
		t.Fatal("expected bad acknowledged SCN error")
	}
}

func TestReaderPollingHonorsContext(t *testing.T) {
	f := &fakeSource{current: "100"}
	r := NewReader(f, "100", nil, time.Hour, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := r.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}
