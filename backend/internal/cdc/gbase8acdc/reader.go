package gbase8acdc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/domain"
)

type Reader struct {
	agent      Agent
	database   string
	selections []TableSelection
	resource   string
	mu         sync.Mutex
	acked      uint64
	live       uint64
	lineage    string
	queue      []TransactionEnvelope
}

func NewReader(agent Agent, database, start string, selections []TableSelection, resource string) (*Reader, error) {
	if agent == nil {
		return nil, errors.New("nil GBase 8a CDC agent")
	}
	seq, lineage, err := ParsePosition(start)
	if err != nil {
		return nil, err
	}
	if len(selections) == 0 {
		return nil, errors.New("GBase 8a CDC requires selected tables")
	}
	return &Reader{agent: agent, database: database, selections: append([]TableSelection(nil), selections...), resource: resource, acked: seq, live: seq, lineage: lineage}, nil
}

func (r *Reader) refill(ctx context.Context) error {
	resp, err := r.agent.Read(ctx, ReadRequest{Database: r.database, AfterSequence: fmt.Sprint(r.live), ExpectedCaptureLineage: r.lineage, Tables: r.selections, MaxTransactions: 128, MaxBytes: 64 << 20})
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("GBase 8a CDC provider returned nil read response")
	}
	if err := validateTransactions(resp, r.live, r.lineage, r.selections); err != nil {
		return err
	}
	r.queue = append(r.queue, resp.Transactions...)
	if len(resp.Transactions) > 0 {
		n, _ := ParseSequence(resp.Transactions[len(resp.Transactions)-1].Sequence)
		r.live = n
	}
	return nil
}
func (r *Reader) Next(ctx context.Context) (*cdcruntime.Transaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.queue) == 0 {
		if err := r.refill(ctx); err != nil {
			return nil, err
		}
		if len(r.queue) == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return nil, nil
			}
		}
	}
	tx := r.queue[0]
	r.queue = r.queue[1:]
	seq, _ := ParseSequence(tx.Sequence)
	pos := FormatPosition(seq, r.lineage)
	events := make([]domain.CDCEvent, len(tx.Events))
	copy(events, tx.Events)
	for i := range events {
		events[i].PositionType = "GBASE8A_CDC_SEQ"
		events[i].PositionValue = pos
		events[i].Resource = r.resource
		if events[i].SourceTimestampMS == 0 {
			events[i].SourceTimestampMS = tx.SourceTimestampMS
		}
	}
	return &cdcruntime.Transaction{Events: events, Checkpoint: domain.CDCPosition{DatabaseType: string(domain.DataSourceGBase), PositionType: "GBASE8A_CDC_SEQ", PositionValue: pos, Resource: r.resource, SourceTimestampMS: tx.SourceTimestampMS}, Label: "gbase8a tx=" + strings.TrimSpace(tx.TransactionID)}, nil
}
func (r *Reader) Acknowledge(ctx context.Context, tx *cdcruntime.Transaction) error {
	if tx == nil {
		return errors.New("nil GBase 8a CDC transaction")
	}
	seq, lineage, err := ParsePosition(tx.Checkpoint.PositionValue)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if lineage != r.lineage {
		return errors.New("GBase 8a CDC ACK capture lineage mismatch")
	}
	if seq < r.acked {
		return errors.New("GBase 8a CDC ACK would move backwards")
	}
	if seq > r.live {
		return errors.New("GBase 8a CDC ACK is ahead of live reader")
	}
	if err := r.agent.Ack(ctx, AckRequest{Database: r.database, Sequence: fmt.Sprint(seq), CaptureLineage: r.lineage}); err != nil {
		return err
	}
	r.acked = seq
	return nil
}
func (r *Reader) Close() error { return nil }
