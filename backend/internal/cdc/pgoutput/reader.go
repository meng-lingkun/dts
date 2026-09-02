package pgoutput

import (
	"context"
	"fmt"
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"time"
)

// Reader adapts a PostgreSQL logical replication stream to QMigration's
// protocol-independent Native CDC Reader SPI.
type Reader struct {
	stream       connector.PostgreSQLLogicalStream
	decoder      *Decoder
	positionType string
}

func NewReader(stream connector.PostgreSQLLogicalStream) *Reader {
	return NewReaderWithDialect(stream, "LSN", "pg")
}

func NewReaderWithDialect(stream connector.PostgreSQLLogicalStream, positionType, idPrefix string) *Reader {
	if positionType == "" {
		positionType = "LSN"
	}
	return &Reader{stream: stream, decoder: NewDecoderWithDialect(positionType, idPrefix), positionType: positionType}
}

func (r *Reader) Next(ctx context.Context) (*cdcruntime.Transaction, error) {
	for {
		copyData, err := r.stream.Next(ctx)
		if err != nil {
			return nil, err
		}
		tx, err := r.decoder.Push(copyData)
		if err != nil {
			return nil, err
		}
		if tx == nil {
			continue
		}
		pos := FormatLSN(tx.EndLSN)
		events := append([]domain.CDCEvent(nil), tx.Events...)
		if len(events) == 0 {
			events = []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: r.positionType, PositionValue: pos, SourceTimestampMS: time.Now().UnixMilli()}}
		}
		return &cdcruntime.Transaction{
			Events:     events,
			Checkpoint: domain.CDCPosition{PositionType: r.positionType, PositionValue: pos, SourceTimestampMS: tx.CommitTime.UnixMilli()},
			Label:      fmt.Sprintf("xid=%d", tx.XID),
		}, nil
	}
}

func (r *Reader) Acknowledge(ctx context.Context, tx *cdcruntime.Transaction) error {
	if tx == nil || tx.Checkpoint.PositionValue == "" {
		return fmt.Errorf("missing PostgreSQL CDC checkpoint")
	}
	return r.stream.Acknowledge(ctx, tx.Checkpoint.PositionValue)
}

func (r *Reader) Close() error { return r.stream.Close() }

var _ cdcruntime.Reader = (*Reader)(nil)
