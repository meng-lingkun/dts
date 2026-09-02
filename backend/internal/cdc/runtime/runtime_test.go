package runtime

import (
	"context"
	"errors"
	"qmigration/backend/internal/domain"
	"testing"
)

type fakeReader struct {
	items  []*Transaction
	acks   []string
	closed bool
}

func (f *fakeReader) Next(context.Context) (*Transaction, error) {
	if len(f.items) == 0 {
		return nil, nil
	}
	x := f.items[0]
	f.items = f.items[1:]
	return x, nil
}
func (f *fakeReader) Acknowledge(_ context.Context, tx *Transaction) error {
	f.acks = append(f.acks, tx.Checkpoint.PositionValue)
	return nil
}
func (f *fakeReader) Close() error { f.closed = true; return nil }

func TestApplyBeforeSourceAcknowledge(t *testing.T) {
	fr := &fakeReader{items: []*Transaction{{Events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint}}, Checkpoint: domain.CDCPosition{PositionValue: "p1"}}, {Events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint}}, Checkpoint: domain.CDCPosition{PositionValue: "p2"}}}}
	calls := 0
	err := (Runner{Reader: fr, Apply: func(context.Context, []domain.CDCEvent) (*domain.CDCApplyResult, error) {
		calls++
		if calls == 2 {
			return nil, errors.New("apply failed")
		}
		return &domain.CDCApplyResult{}, nil
	}}).Run(context.Background())
	if err == nil {
		t.Fatal("expected apply failure")
	}
	if len(fr.acks) != 1 || fr.acks[0] != "p1" {
		t.Fatalf("acks=%v", fr.acks)
	}
	if !fr.closed {
		t.Fatal("reader was not closed")
	}
}

func TestGateRunsBeforeRead(t *testing.T) {
	fr := &fakeReader{}
	gate := false
	err := (Runner{Reader: fr, Gate: func(context.Context) error { gate = true; return nil }, Apply: func(context.Context, []domain.CDCEvent) (*domain.CDCApplyResult, error) {
		return &domain.CDCApplyResult{}, nil
	}}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !gate {
		t.Fatal("gate not called")
	}
}
