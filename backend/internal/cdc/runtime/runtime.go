package runtime

import (
	"context"
	"errors"
	"fmt"
	"qmigration/backend/internal/domain"
)

// Transaction is the protocol-independent unit emitted by every native CDC
// reader. Acknowledge MUST advance only the source-side receive checkpoint
// (for example PostgreSQL standby status or a local MySQL reconnect cursor)
// after QMigration has durably applied Events and persisted its checkpoint.
type Transaction struct {
	Events     []domain.CDCEvent
	Checkpoint domain.CDCPosition
	Label      string
}

// Reader is the unified Native CDC Reader SPI. MySQL binlog, PostgreSQL
// pgoutput and future Oracle/SQL Server readers all feed the same Runner.
type Reader interface {
	Next(context.Context) (*Transaction, error)
	Acknowledge(context.Context, *Transaction) error
	Close() error
}

type ApplyFunc func(context.Context, []domain.CDCEvent) (*domain.CDCApplyResult, error)

type GateFunc func(context.Context) error

type ObserveFunc func(*Transaction, *domain.CDCApplyResult)

type Runner struct {
	Reader  Reader
	Gate    GateFunc
	Apply   ApplyFunc
	Observe ObserveFunc
}

// Run enforces the core CDC durability ordering shared by all vendors:
//
//	read/decode -> QMigration apply+durable checkpoint -> source acknowledge
//
// If Apply fails, Acknowledge is never called. Reconnect therefore starts from
// the last source position that was already accepted by QMigration.
func (r Runner) Run(ctx context.Context) error {
	if r.Reader == nil || r.Apply == nil {
		return errors.New("cdc runtime requires reader and apply hooks")
	}
	defer r.Reader.Close()
	if r.Gate != nil {
		if err := r.Gate(ctx); err != nil {
			return err
		}
	}
	for {
		tx, err := r.Reader.Next(ctx)
		if err != nil {
			return err
		}
		if tx == nil {
			return nil
		}
		if len(tx.Events) == 0 {
			return fmt.Errorf("native CDC reader emitted empty transaction at %s", tx.Checkpoint.PositionValue)
		}
		result, err := r.Apply(ctx, tx.Events)
		if err != nil {
			return err
		}
		if err := r.Reader.Acknowledge(ctx, tx); err != nil {
			return fmt.Errorf("acknowledge source checkpoint %s: %w", tx.Checkpoint.PositionValue, err)
		}
		if r.Observe != nil {
			r.Observe(tx, result)
		}
	}
}
