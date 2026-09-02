package mysqlbinlog

import (
	"context"
	"fmt"
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"strings"
	"time"
)

type ReaderState struct {
	File     string
	Position uint32
	Executed *GTIDSet
}

func (s ReaderState) Clone() ReaderState {
	out := s
	if s.Executed != nil {
		out.Executed = s.Executed.Clone()
	}
	return out
}

type nativeAck struct {
	state              ReaderState
	consumePendingGTID bool
	resetMetadata      bool
}

// NativeReader adapts a MySQL replication stream to the protocol-independent
// QMigration CDC Reader SPI. It owns protocol parsing and transaction assembly;
// apply/checkpoint ordering is enforced by internal/cdc/runtime.
type NativeReader struct {
	stream   connector.RawCDCStream
	meta     connector.Connector
	parser   Parser
	asm      *Assembler
	maps     map[uint64]*TableMap
	metas    map[uint64]*domain.TableMetadata
	selected map[string]bool
	zstdBin  string
	pending  []*Event
	state    ReaderState
	ack      *nativeAck
}

func NewNativeReader(stream connector.RawCDCStream, meta connector.Connector, state ReaderState, selected map[string]bool, zstdBin string) *NativeReader {
	a := &Assembler{}
	a.SetFile(state.File)
	copySelected := map[string]bool{}
	for k, v := range selected {
		copySelected[strings.ToLower(k)] = v
	}
	return &NativeReader{
		stream: stream, meta: meta, asm: a,
		maps: map[uint64]*TableMap{}, metas: map[uint64]*domain.TableMetadata{},
		selected: copySelected, zstdBin: zstdBin, state: state.Clone(),
	}
}

func (r *NativeReader) State() ReaderState { return r.state.Clone() }

func isDDLQuery(q *Query) bool {
	if q == nil {
		return false
	}
	sql := strings.ToUpper(strings.TrimSpace(q.SQL))
	for _, p := range []string{"ALTER ", "CREATE ", "DROP ", "TRUNCATE ", "RENAME "} {
		if strings.HasPrefix(sql, p) {
			return true
		}
	}
	return false
}

func (r *NativeReader) ddlTouchesSelected(q *Query) bool {
	if q == nil {
		return false
	}
	if len(r.selected) == 0 {
		return true
	}
	schema := strings.ToLower(strings.TrimSpace(q.Schema))
	sql := strings.ToLower(q.SQL)
	for key := range r.selected {
		i := strings.LastIndex(key, ".")
		if i <= 0 {
			continue
		}
		ks, table := key[:i], key[i+1:]
		if schema != "" && ks != schema {
			continue
		}
		if strings.Contains(sql, "`"+table+"`") || strings.Contains(sql, table) {
			return true
		}
	}
	return false
}

func validateCDCFields(fields []domain.CDCField) error {
	for _, f := range fields {
		switch strings.ToLower(strings.TrimSpace(f.Encoding)) {
		case "", "text", "utf8", "base64", "json":
		default:
			return fmt.Errorf("column %s uses unsupported native CDC encoding %q", f.Column, f.Encoding)
		}
	}
	return nil
}

func (r *NativeReader) decodeTransaction(tx *Transaction) ([]domain.CDCEvent, error) {
	out := []domain.CDCEvent{}
	for _, ev := range tx.Events {
		rows, err := ParseRows(ev)
		if err != nil {
			return nil, err
		}
		tm := r.maps[rows.TableID]
		md := r.metas[rows.TableID]
		if tm == nil || md == nil {
			return nil, fmt.Errorf("missing TABLE_MAP/metadata for table id %d", rows.TableID)
		}
		key := strings.ToLower(tm.Schema + "." + tm.Table)
		if len(r.selected) > 0 && !r.selected[key] {
			continue
		}
		changes, err := DecodeRows(tm, rows, md.Columns)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", key, err)
		}
		for _, ch := range changes {
			if err := validateCDCFields(ch.Before); err != nil {
				return nil, err
			}
			if err := validateCDCFields(ch.After); err != nil {
				return nil, err
			}
			op := domain.CDCInsert
			switch ev.Header.Type {
			case UpdateRowsEventV1, UpdateRowsEventV2, PartialUpdateRowsEvent:
				op = domain.CDCUpdate
			case DeleteRowsEventV1, DeleteRowsEventV2:
				op = domain.CDCDelete
			}
			out = append(out, domain.CDCEvent{
				Operation: op, SourceSchema: tm.Schema, SourceTable: tm.Table,
				Before: ch.Before, After: ch.After,
				SourceTimestampMS: int64(ev.Header.Timestamp) * 1000,
			})
		}
	}
	return out, nil
}

func (r *NativeReader) nextEvent(ctx context.Context) (*Event, error) {
	if len(r.pending) > 0 {
		ev := r.pending[0]
		r.pending = r.pending[1:]
		return ev, nil
	}
	packet, err := r.stream.Next(ctx)
	if err != nil {
		return nil, err
	}
	return r.parser.Parse(packet)
}

func (r *NativeReader) Next(ctx context.Context) (*cdcruntime.Transaction, error) {
	if r.ack != nil {
		return nil, fmt.Errorf("previous MySQL CDC transaction has not been acknowledged")
	}
	for {
		ev, err := r.nextEvent(ctx)
		if err != nil {
			return nil, err
		}
		if ev.Header.Type == TransactionPayloadEvent {
			outerLogPos := ev.Header.LogPos
			payload, err := ParseTransactionPayload(ev)
			if err != nil {
				return nil, err
			}
			plain, err := payload.Decompress(r.zstdBin)
			if err != nil {
				return nil, err
			}
			rawEvents, err := SplitTransactionEvents(plain)
			if err != nil {
				return nil, err
			}
			expanded := make([]*Event, 0, len(rawEvents))
			for _, raw := range rawEvents {
				inner, err := r.parser.Parse(raw)
				if err != nil {
					return nil, fmt.Errorf("parse nested transaction payload event: %w", err)
				}
				inner.Header.LogPos = outerLogPos
				expanded = append(expanded, inner)
			}
			r.pending = append(expanded, r.pending...)
			continue
		}
		if ev.Header.Type == TableMapEvent {
			tm, err := ParseTableMap(ev)
			if err != nil {
				return nil, err
			}
			r.maps[tm.TableID] = tm
			key := strings.ToLower(tm.Schema + "." + tm.Table)
			if len(r.selected) == 0 || r.selected[key] {
				md, err := r.meta.GetTableMetadata(ctx, tm.Schema, tm.Table)
				if err != nil {
					return nil, fmt.Errorf("metadata %s: %w", key, err)
				}
				r.metas[tm.TableID] = md
			}
			continue
		}
		if ev.Header.Type == QueryEvent {
			q, qerr := ParseQuery(ev)
			if qerr == nil && isDDLQuery(q) {
				pendingGTID := r.asm.PendingGTID()
				touches := r.ddlTouchesSelected(q)
				next := r.state.Clone()
				if next.Executed != nil {
					if pendingGTID == "" {
						return nil, fmt.Errorf("GTID DDL at %s:%d has no preceding GTID event", r.asm.File(), ev.Header.LogPos)
					}
					if err := next.Executed.Add(pendingGTID); err != nil {
						return nil, err
					}
				}
				next.File, next.Position = r.asm.File(), ev.Header.LogPos
				op := domain.CDCCheckpoint
				if touches {
					op = domain.CDCDDL
				}
				ce := domain.CDCEvent{
					Operation: op, SourceSchema: q.Schema, SQL: q.SQL,
					SourceTimestampMS: int64(ev.Header.Timestamp) * 1000,
					Resource:          r.asm.File(),
				}
				if next.Executed != nil {
					ce.PositionType, ce.PositionValue = "GTID", next.Executed.String()
				} else {
					ce.PositionType, ce.PositionValue = "BINLOG", fmt.Sprintf("%s:%d", next.File, next.Position)
				}
				r.ack = &nativeAck{state: next, consumePendingGTID: true, resetMetadata: touches}
				return &cdcruntime.Transaction{
					Events:     []domain.CDCEvent{ce},
					Checkpoint: domain.CDCPosition{PositionType: ce.PositionType, PositionValue: ce.PositionValue, Resource: ce.Resource, SourceTimestampMS: ce.SourceTimestampMS},
					Label:      "ddl",
				}, nil
			}
		}

		tx, err := r.asm.Push(ev)
		if err != nil {
			return nil, err
		}
		if tx == nil {
			continue
		}
		events, err := r.decodeTransaction(tx)
		if err != nil {
			return nil, err
		}
		next := r.state.Clone()
		if next.Executed != nil {
			if tx.GTID == "" {
				return nil, fmt.Errorf("GTID replication transaction ending at %s has no GTID event", tx.Position())
			}
			if err := next.Executed.Add(tx.GTID); err != nil {
				return nil, err
			}
		}
		next.File = tx.File
		if next.File == "" {
			next.File = r.asm.File()
		}
		next.Position = tx.EndPos
		positionType := "BINLOG"
		positionValue := fmt.Sprintf("%s:%d", next.File, next.Position)
		if next.Executed != nil {
			positionType, positionValue = "GTID", next.Executed.String()
		}
		if len(events) > 0 {
			last := &events[len(events)-1]
			last.PositionType, last.PositionValue, last.Resource = positionType, positionValue, next.File
		} else {
			events = []domain.CDCEvent{{
				Operation: domain.CDCCheckpoint, PositionType: positionType,
				PositionValue: positionValue, Resource: next.File,
				SourceTimestampMS: time.Now().UnixMilli(),
			}}
		}
		r.ack = &nativeAck{state: next}
		return &cdcruntime.Transaction{
			Events: events,
			Checkpoint: domain.CDCPosition{
				PositionType: positionType, PositionValue: positionValue,
				Resource: next.File, SourceTimestampMS: events[len(events)-1].SourceTimestampMS,
			},
			Label: fmt.Sprintf("gtid=%s xid=%d", tx.GTID, tx.XID),
		}, nil
	}
}

func (r *NativeReader) Acknowledge(_ context.Context, tx *cdcruntime.Transaction) error {
	if r.ack == nil {
		return fmt.Errorf("no MySQL CDC transaction pending acknowledgement")
	}
	if tx == nil || tx.Checkpoint.PositionValue == "" {
		return fmt.Errorf("missing MySQL CDC checkpoint")
	}
	if r.ack.state.Executed != nil {
		want := r.ack.state.Executed.String()
		if tx.Checkpoint.PositionValue != want {
			return fmt.Errorf("checkpoint mismatch: got %s want %s", tx.Checkpoint.PositionValue, want)
		}
	} else {
		want := fmt.Sprintf("%s:%d", r.ack.state.File, r.ack.state.Position)
		if tx.Checkpoint.PositionValue != want {
			return fmt.Errorf("checkpoint mismatch: got %s want %s", tx.Checkpoint.PositionValue, want)
		}
	}
	if r.ack.consumePendingGTID {
		r.asm.ConsumePendingGTID()
	}
	if r.ack.resetMetadata {
		r.maps = map[uint64]*TableMap{}
		r.metas = map[uint64]*domain.TableMetadata{}
	}
	r.state = r.ack.state.Clone()
	r.ack = nil
	return nil
}

func (r *NativeReader) Close() error { return r.stream.Close() }

var _ cdcruntime.Reader = (*NativeReader)(nil)
