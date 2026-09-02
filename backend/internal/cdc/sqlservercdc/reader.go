package sqlservercdc

import (
	"context"
	"errors"
	"fmt"
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	sqlserverconnector "qmigration/backend/internal/connector/sqlserver"
	"qmigration/backend/internal/domain"
	"strings"
	"time"
)

type Source interface {
	DiscoverCDCCaptures(context.Context, map[string]bool) ([]sqlserverconnector.CDCCapture, error)
	ValidateCDCStart(context.Context, []sqlserverconnector.CDCCapture, string) error
	NextCDCWindow(context.Context, string, int) (string, string, bool, error)
	ReadCDCChanges(context.Context, sqlserverconnector.CDCCapture, string, string) ([]sqlserverconnector.CDCChange, error)
	Close() error
}

type Reader struct {
	source          Source
	selected        map[string]bool
	captures        []sqlserverconnector.CDCCapture
	acknowledged    string
	pending         []sqlserverconnector.SQLServerCDCTransaction
	pollInterval    time.Duration
	maxTransactions int
}

func NewReader(source Source, startLSN string, selected []string, poll time.Duration, maxTransactions int) *Reader {
	m := map[string]bool{}
	for _, v := range selected {
		if x := strings.ToLower(strings.TrimSpace(v)); x != "" {
			m[x] = true
		}
	}
	if poll <= 0 {
		poll = time.Second
	}
	if maxTransactions <= 0 {
		maxTransactions = 256
	}
	return &Reader{source: source, selected: m, acknowledged: startLSN, pollInterval: poll, maxTransactions: maxTransactions}
}

func (r *Reader) load(ctx context.Context) error {
	if len(r.captures) == 0 {
		caps, err := r.source.DiscoverCDCCaptures(ctx, r.selected)
		if err != nil {
			return err
		}
		if len(caps) == 0 {
			return errors.New("no SQL Server CDC capture instances selected")
		}
		if err := r.source.ValidateCDCStart(ctx, caps, r.acknowledged); err != nil {
			return err
		}
		r.captures = caps
	}
	for {
		from, to, empty, err := r.source.NextCDCWindow(ctx, r.acknowledged, r.maxTransactions)
		if err != nil {
			return err
		}
		if empty {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.pollInterval):
				continue
			}
		}
		all := []sqlserverconnector.CDCChange{}
		for _, cap := range r.captures {
			ch, err := r.source.ReadCDCChanges(ctx, cap, from, to)
			if err != nil {
				return fmt.Errorf("read SQL Server CDC %s.%s: %w", cap.Schema, cap.Table, err)
			}
			all = append(all, ch...)
		}
		txs, err := sqlserverconnector.ChangesToTransactions(all)
		if err != nil {
			return err
		}
		if len(txs) == 0 {
			// The LSN window can contain only unrelated tables. Persist a
			// checkpoint-only event through the normal apply path instead of
			// advancing the in-memory cursor. This keeps retention-safe progress
			// durable across process restarts while preserving apply-before-ACK.
			r.pending = []sqlserverconnector.SQLServerCDCTransaction{{
				LSN: to,
				Events: []domain.CDCEvent{{
					ID:            "sqlserver-checkpoint:" + to,
					Operation:     domain.CDCCheckpoint,
					PositionType:  "SQLSERVER_LSN",
					PositionValue: to,
					Resource:      "cdc.lsn_time_mapping",
				}},
			}}
			return nil
		}
		r.pending = txs
		return nil
	}
}

func (r *Reader) Next(ctx context.Context) (*cdcruntime.Transaction, error) {
	if len(r.pending) == 0 {
		if err := r.load(ctx); err != nil {
			return nil, err
		}
	}
	x := r.pending[0]
	r.pending = r.pending[1:]
	return &cdcruntime.Transaction{Events: x.Events, Checkpoint: domain.CDCPosition{DatabaseType: string(domain.DataSourceSQLServer), PositionType: "SQLSERVER_LSN", PositionValue: x.LSN, SourceTimestampMS: x.TimestampMS}, Label: "sqlserver lsn " + x.LSN}, nil
}
func (r *Reader) Acknowledge(_ context.Context, tx *cdcruntime.Transaction) error {
	if tx == nil || strings.TrimSpace(tx.Checkpoint.PositionValue) == "" {
		return errors.New("SQL Server CDC acknowledge requires LSN")
	}
	r.acknowledged = tx.Checkpoint.PositionValue
	return nil
}
func (r *Reader) Close() error { return r.source.Close() }
