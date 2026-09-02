package oraclelogminer

import (
	"context"
	"errors"
	"fmt"
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	oracleconnector "qmigration/backend/internal/connector/oracle"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
	"time"
)

type Source interface {
	CurrentCDCPosition(context.Context) (*domain.CDCPosition, error)
	ReadLogMinerTransactions(context.Context, string, string, map[string]bool) ([]oracleconnector.OracleCDCTransaction, error)
	Close() error
}

type Reader struct {
	source       Source
	selected     map[string]bool
	acknowledged string
	pending      []oracleconnector.OracleCDCTransaction
	pollInterval time.Duration
	maxSCNSpan   uint64
}

func NewReader(source Source, startSCN string, selected []string, poll time.Duration, maxSCNSpan uint64) *Reader {
	set := map[string]bool{}
	for _, v := range selected {
		if x := strings.ToLower(strings.TrimSpace(v)); x != "" {
			set[x] = true
		}
	}
	if poll <= 0 {
		poll = time.Second
	}
	if maxSCNSpan == 0 {
		maxSCNSpan = 100000
	}
	return &Reader{source: source, selected: set, acknowledged: strings.TrimSpace(startSCN), pollInterval: poll, maxSCNSpan: maxSCNSpan}
}

func (r *Reader) load(ctx context.Context) error {
	if r.source == nil {
		return errors.New("Oracle LogMiner reader requires source")
	}
	for {
		current, err := r.source.CurrentCDCPosition(ctx)
		if err != nil {
			return err
		}
		cur, err := strconv.ParseUint(strings.TrimSpace(current.PositionValue), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid Oracle current SCN %q: %w", current.PositionValue, err)
		}
		ack, err := strconv.ParseUint(strings.TrimSpace(r.acknowledged), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid Oracle acknowledged SCN %q: %w", r.acknowledged, err)
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
		if r.maxSCNSpan > 0 && cur-ack > r.maxSCNSpan {
			to = ack + r.maxSCNSpan
		}
		txs, err := r.source.ReadLogMinerTransactions(ctx, strconv.FormatUint(ack, 10), strconv.FormatUint(to, 10), r.selected)
		if err != nil {
			return err
		}
		if len(txs) == 0 {
			r.pending = []oracleconnector.OracleCDCTransaction{{SCN: strconv.FormatUint(to, 10), Events: []domain.CDCEvent{{ID: "oracle-checkpoint:" + strconv.FormatUint(to, 10), Operation: domain.CDCCheckpoint, PositionType: "ORACLE_SCN", PositionValue: strconv.FormatUint(to, 10), Resource: "DBMS_LOGMNR"}}}}
		} else {
			r.pending = txs
		}
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
		return nil, errors.New("Oracle LogMiner emitted empty transaction")
	}
	ts := tx.TimestampMS
	return &cdcruntime.Transaction{Events: tx.Events, Checkpoint: domain.CDCPosition{DatabaseType: string(domain.DataSourceOracle), PositionType: "ORACLE_SCN", PositionValue: tx.SCN, Resource: "DBMS_LOGMNR", SourceTimestampMS: ts}, Label: "oracle scn " + tx.SCN}, nil
}
func (r *Reader) Acknowledge(_ context.Context, tx *cdcruntime.Transaction) error {
	if tx == nil || strings.TrimSpace(tx.Checkpoint.PositionValue) == "" {
		return errors.New("Oracle LogMiner acknowledge requires SCN")
	}
	r.acknowledged = tx.Checkpoint.PositionValue
	return nil
}
func (r *Reader) Close() error {
	if r.source == nil {
		return nil
	}
	return r.source.Close()
}
