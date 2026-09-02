package damenglog

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	cdcruntime "qmigration/backend/internal/cdc/runtime"
	damengconnector "qmigration/backend/internal/connector/dameng"
	"qmigration/backend/internal/domain"
)

type Source interface {
	CurrentCDCPosition(context.Context) (*domain.CDCPosition, error)
	ReadLogMinerTransactions(context.Context, string, string, map[string]bool) ([]damengconnector.DamengCDCTransaction, error)
	Close() error
}

type Reader struct {
	source       Source
	selected     map[string]bool
	acknowledged string
	pending      []damengconnector.DamengCDCTransaction
	pollInterval time.Duration
	maxLSNSpan   uint64
}

func NewReader(source Source, startLSN string, selected []string, poll time.Duration, maxLSNSpan uint64) (*Reader, error) {
	if source == nil {
		return nil, errors.New("Dameng LogMiner reader requires source")
	}
	start, err := strconv.ParseUint(strings.TrimSpace(startLSN), 10, 64)
	if err != nil || start == 0 {
		if err == nil {
			err = errors.New("LSN must be greater than zero")
		}
		return nil, fmt.Errorf("invalid Dameng start LSN %q: %w", startLSN, err)
	}
	set := map[string]bool{}
	for _, v := range selected {
		if x := strings.ToLower(strings.TrimSpace(v)); x != "" {
			if !strings.Contains(x, ".") {
				return nil, fmt.Errorf("Dameng selected table %q must be schema.table", v)
			}
			set[x] = true
		}
	}
	if len(set) == 0 {
		return nil, errors.New("Dameng LogMiner reader requires selected tables")
	}
	if poll <= 0 {
		poll = 2 * time.Second
	}
	if maxLSNSpan == 0 {
		maxLSNSpan = 100000
	}
	return &Reader{source: source, selected: set, acknowledged: strconv.FormatUint(start, 10), pollInterval: poll, maxLSNSpan: maxLSNSpan}, nil
}

func (r *Reader) load(ctx context.Context) error {
	for {
		current, err := r.source.CurrentCDCPosition(ctx)
		if err != nil {
			return err
		}
		if !strings.EqualFold(strings.TrimSpace(current.PositionType), "DM_LSN") {
			return fmt.Errorf("Dameng source returned position type %q; expected DM_LSN", current.PositionType)
		}
		cur, err := strconv.ParseUint(strings.TrimSpace(current.PositionValue), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid Dameng current LSN %q: %w", current.PositionValue, err)
		}
		ack, err := strconv.ParseUint(strings.TrimSpace(r.acknowledged), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid Dameng acknowledged LSN %q: %w", r.acknowledged, err)
		}
		if cur <= ack {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.pollInterval):
				continue
			}
		}
		to := cur
		if r.maxLSNSpan > 0 && cur-ack > r.maxLSNSpan {
			to = ack + r.maxLSNSpan
		}
		txs, err := r.source.ReadLogMinerTransactions(ctx, strconv.FormatUint(ack, 10), strconv.FormatUint(to, 10), r.selected)
		if err != nil {
			return err
		}
		if len(txs) == 0 {
			return fmt.Errorf("Dameng LogMiner source returned no checkpoint for non-empty LSN window %d..%d", ack, to)
		}
		last, err := strconv.ParseUint(strings.TrimSpace(txs[len(txs)-1].LSN), 10, 64)
		if err != nil || last != to {
			return fmt.Errorf("Dameng LogMiner source window ended at %q; expected %d", txs[len(txs)-1].LSN, to)
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
	tx := r.pending[0]
	r.pending = r.pending[1:]
	if len(tx.Events) == 0 {
		return nil, errors.New("Dameng LogMiner emitted empty transaction")
	}
	return &cdcruntime.Transaction{Events: tx.Events, Checkpoint: domain.CDCPosition{DatabaseType: string(domain.DataSourceDameng), PositionType: "DM_LSN", PositionValue: tx.LSN, Resource: "DBMS_LOGMNR", SourceTimestampMS: tx.TimestampMS}, Label: "dameng lsn " + tx.LSN}, nil
}

func (r *Reader) Acknowledge(_ context.Context, tx *cdcruntime.Transaction) error {
	if tx == nil || !strings.EqualFold(strings.TrimSpace(tx.Checkpoint.PositionType), "DM_LSN") {
		return errors.New("Dameng LogMiner acknowledge requires DM_LSN checkpoint")
	}
	next, err := strconv.ParseUint(strings.TrimSpace(tx.Checkpoint.PositionValue), 10, 64)
	if err != nil || next == 0 {
		return fmt.Errorf("invalid Dameng acknowledge LSN %q", tx.Checkpoint.PositionValue)
	}
	prev, err := strconv.ParseUint(strings.TrimSpace(r.acknowledged), 10, 64)
	if err != nil {
		return err
	}
	if next < prev {
		return fmt.Errorf("Dameng acknowledge LSN regressed from %d to %d", prev, next)
	}
	r.acknowledged = strconv.FormatUint(next, 10)
	return nil
}

func (r *Reader) Close() error {
	if r.source == nil {
		return nil
	}
	return r.source.Close()
}
