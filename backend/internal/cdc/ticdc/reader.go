package ticdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/domain"
)

const (
	ticdcMaxTransactionEvents = 100000
	ticdcMaxTransactionBytes  = 128 << 20
)

type kafkaFetcher interface {
	Fetch(context.Context, string, int64, int32) ([]KafkaRecord, int64, error)
}

type kafkaPartitionFetcher interface {
	Partitions(context.Context, string) ([]int32, error)
	FetchPartition(context.Context, string, int32, int64, int32) ([]KafkaRecord, int64, error)
}

type decodedKafkaRecord struct {
	record    KafkaRecord
	events    []domain.CDCEvent
	tso       uint64
	watermark bool
}

type partitionCursor struct {
	partition  int32
	readOffset int64
	queue      []decodedKafkaRecord
	resolvedTS uint64
}

type Reader struct {
	kafka           kafkaFetcher
	topic, resource string
	selected        map[string]bool
	mu              sync.Mutex
	position, acked Position
	readOffset      int64
	hasDurableAck   bool
	pending         []KafkaRecord
	carry           *decodedKafkaRecord
	fetchBytes      int32

	modeReady  bool
	multi      bool
	partitions []int32
	cursors    map[int32]*partitionCursor
}

func NewReader(kafka kafkaFetcher, topic, changefeedID string, start Position, selected map[string]bool) (*Reader, error) {
	if kafka == nil {
		return nil, errors.New("TiCDC reader requires Kafka client")
	}
	if strings.TrimSpace(topic) == "" || strings.TrimSpace(changefeedID) == "" {
		return nil, errors.New("TiCDC reader requires topic and changefeed id")
	}
	for partition, offset := range start.normalizedOffsets() {
		if partition < 0 || offset < 0 {
			return nil, errors.New("TiCDC reader start partition/offset cannot be negative")
		}
	}
	start.Offset = start.PartitionOffset(0)
	if len(start.Offsets) == 0 {
		start.Offsets = map[int32]int64{0: start.Offset}
	}
	return &Reader{
		kafka:         kafka,
		topic:         topic,
		resource:      changefeedID + "/" + topic,
		selected:      selected,
		position:      clonePosition(start),
		acked:         clonePosition(start),
		readOffset:    start.PartitionOffset(0),
		hasDurableAck: start.TSO > 0 && anyPositiveOffset(start),
		fetchBytes:    16 << 20,
	}, nil
}

func clonePosition(p Position) Position {
	out := p
	if len(p.Offsets) > 0 {
		out.Offsets = make(map[int32]int64, len(p.Offsets))
		for partition, offset := range p.Offsets {
			out.Offsets[partition] = offset
		}
	}
	return out
}

func anyPositiveOffset(p Position) bool {
	for _, offset := range p.normalizedOffsets() {
		if offset > 0 {
			return true
		}
	}
	return false
}

func (r *Reader) Next(ctx context.Context) (*cdcruntime.Transaction, error) {
	if err := r.ensureMode(ctx); err != nil {
		return nil, err
	}
	if r.multi {
		return r.nextMulti(ctx)
	}
	return r.nextSingle(ctx)
}

func (r *Reader) ensureMode(ctx context.Context) error {
	r.mu.Lock()
	if r.modeReady {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()
	pf, ok := r.kafka.(kafkaPartitionFetcher)
	if !ok {
		r.mu.Lock()
		r.modeReady = true
		r.mu.Unlock()
		return nil
	}
	parts, err := pf.Partitions(ctx, r.topic)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return errors.New("TiCDC topic has no Kafka partitions")
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	seen := map[int32]bool{}
	for _, partition := range parts {
		if partition < 0 || seen[partition] {
			return fmt.Errorf("invalid/duplicate Kafka partition %d", partition)
		}
		seen[partition] = true
	}
	if len(parts) == 1 && parts[0] == 0 {
		r.mu.Lock()
		r.modeReady = true
		r.mu.Unlock()
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	persisted := r.acked.normalizedOffsets()
	if r.hasDurableAck {
		for partition := range persisted {
			if !seen[partition] {
				return fmt.Errorf("TiCDC Kafka partition topology changed: durable checkpoint contains partition %d which is no longer present", partition)
			}
		}
		if len(persisted) == 1 && persisted[0] > 0 && len(parts) > 1 {
			return errors.New("TiCDC topic changed from a single partition to multiple partitions after a durable offset was acknowledged; create a new capture/changefeed instead of guessing missing partition offsets")
		}
	}
	r.multi = true
	r.partitions = append([]int32(nil), parts...)
	r.cursors = make(map[int32]*partitionCursor, len(parts))
	for _, partition := range parts {
		offset := int64(0)
		if v, ok := persisted[partition]; ok {
			offset = v
		}
		r.cursors[partition] = &partitionCursor{partition: partition, readOffset: offset}
	}
	// Expand a pre-Full TSO-only/offset-zero checkpoint to the discovered topic
	// shape. This is safe because no Kafka record has been acknowledged yet.
	if !r.hasDurableAck {
		offsets := make(map[int32]int64, len(parts))
		for _, partition := range parts {
			offsets[partition] = r.cursors[partition].readOffset
		}
		r.acked.Offsets = offsets
		r.acked.Offset = offsets[0]
		r.position = clonePosition(r.acked)
	}
	r.modeReady = true
	return nil
}

func (r *Reader) nextSingle(ctx context.Context) (*cdcruntime.Transaction, error) {
	first, err := r.nextDecoded(ctx)
	if err != nil {
		return nil, err
	}
	if r.isPreviouslyAcknowledged(first.tso) {
		return r.duplicateCheckpoint(first), nil
	}
	if containsDDL(first.events) || first.watermark {
		return r.finishGroup(first.tso, []decodedKafkaRecord{*first}), nil
	}

	groupTSO := first.tso
	group := []decodedKafkaRecord{*first}
	eventCount := len(first.events)
	byteCount := len(first.record.Value)
	if err := validateTransactionBounds(groupTSO, eventCount, byteCount); err != nil {
		return nil, err
	}
	for {
		next, err := r.nextDecoded(ctx)
		if err != nil {
			return nil, err
		}
		if next.tso != groupTSO || containsDDL(next.events) || next.watermark {
			r.mu.Lock()
			r.carry = next
			r.mu.Unlock()
			break
		}
		group = append(group, *next)
		eventCount += len(next.events)
		byteCount += len(next.record.Value)
		if err := validateTransactionBounds(groupTSO, eventCount, byteCount); err != nil {
			return nil, err
		}
	}
	return r.finishGroup(groupTSO, group), nil
}

func (r *Reader) nextMulti(ctx context.Context) (*cdcruntime.Transaction, error) {
	pf := r.kafka.(kafkaPartitionFetcher)
	for {
		r.mu.Lock()
		ackedTSO := r.acked.TSO
		durable := r.hasDurableAck
		r.mu.Unlock()

		// Seed every partition with either a data event or a Resolved TS above
		// the durable TSO. TiCDC documents Resolved TS as the cross-partition
		// ordering fence: all messages earlier than that TS have been emitted.
		for _, partition := range r.partitions {
			c := r.cursors[partition]
			for len(c.queue) == 0 && (!durable || c.resolvedTS <= ackedTSO) {
				if err := r.pumpPartition(ctx, pf, c); err != nil {
					return nil, err
				}
			}
		}
		r.discardAcknowledgedMulti(ackedTSO, durable)

		candidate, hasData := r.minimumQueuedTSO()
		if !hasData {
			// No data is pending. Advance only after every partition has a
			// Resolved TS strictly beyond the durable point.
			for _, partition := range r.partitions {
				c := r.cursors[partition]
				for c.resolvedTS <= ackedTSO {
					if err := r.pumpPartition(ctx, pf, c); err != nil {
						return nil, err
					}
				}
			}
			r.discardAcknowledgedMulti(ackedTSO, durable)
			if next, ok := r.minimumQueuedTSO(); ok {
				candidate, hasData = next, true
			} else {
				barrier := r.minimumResolvedTS()
				return r.multiCheckpoint(barrier, sourceMillis(0, barrier), "resolved"), nil
			}
		}

		if hasData {
			// A Resolved TS of exactly candidate only proves events *earlier*
			// than candidate are complete, so require resolvedTS > candidate on
			// every partition before publishing that commitTS.
			for _, partition := range r.partitions {
				c := r.cursors[partition]
				for c.resolvedTS <= candidate {
					if err := r.pumpPartition(ctx, pf, c); err != nil {
						return nil, err
					}
				}
			}
			r.discardAcknowledgedMulti(ackedTSO, durable)
			if lower, ok := r.minimumQueuedTSO(); ok && lower < candidate {
				// Fetching to the resolved frontier exposed an earlier event from
				// another partition. Re-run with the true global minimum.
				continue
			}

			group := r.takeQueuedTSO(candidate)
			if len(group) == 0 {
				continue
			}
			eventCount, byteCount := 0, 0
			var sourceMS int64
			for _, item := range group {
				eventCount += len(item.events)
				byteCount += len(item.record.Value)
				for _, e := range item.events {
					if e.SourceTimestampMS > sourceMS {
						sourceMS = e.SourceTimestampMS
					}
				}
			}
			if err := validateTransactionBounds(candidate, eventCount, byteCount); err != nil {
				return nil, err
			}
			return r.finishMultiGroup(candidate, group, sourceMS), nil
		}
	}
}

func (r *Reader) pumpPartition(ctx context.Context, pf kafkaPartitionFetcher, c *partitionCursor) error {
	for {
		recs, _, err := pf.FetchPartition(ctx, r.topic, c.partition, c.readOffset, r.fetchBytes)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		for _, rec := range recs {
			if rec.Offset < c.readOffset {
				continue
			}
			rec.Partition = c.partition
			if rec.Offset+1 > c.readOffset {
				c.readOffset = rec.Offset + 1
			}
			// Kafka tombstones/empty values carry no TiCDC row event. Advancing
			// their offset is safe, but they are not a Resolved TS fence.
			if len(rec.Value) == 0 {
				continue
			}
			decoded, err := r.decodeRecord(rec)
			if err != nil {
				return fmt.Errorf("TiCDC topic %s partition %d offset %d: %w", r.topic, c.partition, rec.Offset, err)
			}
			if decoded.watermark {
				if decoded.tso > c.resolvedTS {
					c.resolvedTS = decoded.tso
				}
				continue
			}
			c.queue = append(c.queue, *decoded)
		}
		return nil
	}
}

func (r *Reader) discardAcknowledgedMulti(ackedTSO uint64, durable bool) {
	if !durable || ackedTSO == 0 {
		return
	}
	for _, partition := range r.partitions {
		c := r.cursors[partition]
		kept := c.queue[:0]
		for _, item := range c.queue {
			if item.tso <= ackedTSO {
				continue
			}
			kept = append(kept, item)
		}
		c.queue = kept
	}
}

func (r *Reader) minimumQueuedTSO() (uint64, bool) {
	var candidate uint64
	has := false
	for _, partition := range r.partitions {
		for _, item := range r.cursors[partition].queue {
			if !has || item.tso < candidate {
				candidate, has = item.tso, true
			}
		}
	}
	return candidate, has
}

func (r *Reader) minimumResolvedTS() uint64 {
	var barrier uint64
	for i, partition := range r.partitions {
		resolved := r.cursors[partition].resolvedTS
		if i == 0 || resolved < barrier {
			barrier = resolved
		}
	}
	return barrier
}

func (r *Reader) takeQueuedTSO(tso uint64) []decodedKafkaRecord {
	group := make([]decodedKafkaRecord, 0)
	for _, partition := range r.partitions {
		c := r.cursors[partition]
		kept := c.queue[:0]
		for _, item := range c.queue {
			if item.tso == tso {
				group = append(group, item)
				continue
			}
			kept = append(kept, item)
		}
		c.queue = kept
	}
	sort.SliceStable(group, func(i, j int) bool {
		if group[i].record.Partition == group[j].record.Partition {
			return group[i].record.Offset < group[j].record.Offset
		}
		return group[i].record.Partition < group[j].record.Partition
	})
	return group
}

func (r *Reader) multiCheckpoint(tso uint64, sourceMS int64, reason string) *cdcruntime.Transaction {
	pos := r.multiPosition(tso)
	e := checkpointEvent(pos, r.resource, sourceMS)
	return &cdcruntime.Transaction{Events: []domain.CDCEvent{e}, Checkpoint: eventPosition(e), Label: fmt.Sprintf("ticdc:%s:tso=%d partitions=%d", reason, tso, len(r.partitions))}
}

func (r *Reader) finishMultiGroup(tso uint64, group []decodedKafkaRecord, sourceMS int64) *cdcruntime.Transaction {
	pos := r.multiPosition(tso)
	events := make([]domain.CDCEvent, 0)
	ddlSeen := map[string]bool{}
	for _, item := range group {
		for _, e := range item.events {
			if e.SourceTimestampMS > sourceMS {
				sourceMS = e.SourceTimestampMS
			}
			if e.Operation == domain.CDCCheckpoint {
				continue
			}
			if e.Operation == domain.CDCDDL {
				key := strings.ToLower(strings.TrimSpace(e.SourceSchema)) + "\x00" + strings.ToLower(strings.TrimSpace(e.SourceTable)) + "\x00" + strings.TrimSpace(e.SQL)
				if ddlSeen[key] {
					continue
				}
				ddlSeen[key] = true
			}
			events = append(events, e)
		}
	}
	if len(events) == 0 {
		events = []domain.CDCEvent{checkpointEvent(pos, r.resource, sourceMS)}
	}
	end := &events[len(events)-1]
	end.PositionType = "TIDB_TSO"
	end.PositionValue = pos.String()
	end.Resource = r.resource
	if end.SourceTimestampMS == 0 {
		end.SourceTimestampMS = sourceMS
	}
	return &cdcruntime.Transaction{Events: events, Checkpoint: eventPosition(*end), Label: fmt.Sprintf("ticdc:tso=%d partitions=%d events=%d", tso, len(r.partitions), len(events))}
}

func (r *Reader) multiPosition(tso uint64) Position {
	offsets := make(map[int32]int64, len(r.partitions))
	for _, partition := range r.partitions {
		c := r.cursors[partition]
		if len(c.queue) > 0 {
			minOffset := c.queue[0].record.Offset
			for _, item := range c.queue[1:] {
				if item.record.Offset < minOffset {
					minOffset = item.record.Offset
				}
			}
			offsets[partition] = minOffset
		} else {
			offsets[partition] = c.readOffset
		}
	}
	return Position{TSO: tso, Offset: offsets[0], Offsets: offsets}
}

func validateTransactionBounds(tso uint64, eventCount, byteCount int) error {
	if eventCount > ticdcMaxTransactionEvents || byteCount > ticdcMaxTransactionBytes {
		return fmt.Errorf("TiCDC transaction TSO %d exceeds QMigration bounds: events=%d bytes=%d", tso, eventCount, byteCount)
	}
	return nil
}

func (r *Reader) decodeRecord(rec KafkaRecord) (*decodedKafkaRecord, error) {
	if len(rec.Value) == 0 {
		r.mu.Lock()
		tso := r.position.TSO
		r.mu.Unlock()
		return &decodedKafkaRecord{record: rec, events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint}}, tso: tso, watermark: true}, nil
	}
	var envelope struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(rec.Value, &envelope)
	events, tso, err := DecodeCanalJSON(rec.Value, r.selected)
	if err != nil {
		return nil, err
	}
	return &decodedKafkaRecord{record: rec, events: events, tso: tso, watermark: strings.EqualFold(strings.TrimSpace(envelope.Type), "TIDB_WATERMARK")}, nil
}

func (r *Reader) nextDecoded(ctx context.Context) (*decodedKafkaRecord, error) {
	r.mu.Lock()
	if r.carry != nil {
		v := r.carry
		r.carry = nil
		r.mu.Unlock()
		return v, nil
	}
	r.mu.Unlock()
	for {
		rec, err := r.nextRecord(ctx)
		if err != nil {
			return nil, err
		}
		decoded, err := r.decodeRecord(rec)
		if err != nil {
			return nil, fmt.Errorf("TiCDC topic %s offset %d: %w", r.topic, rec.Offset, err)
		}
		return decoded, nil
	}
}

func (r *Reader) nextRecord(ctx context.Context) (KafkaRecord, error) {
	for {
		r.mu.Lock()
		if len(r.pending) > 0 {
			rec := r.pending[0]
			r.pending = r.pending[1:]
			r.mu.Unlock()
			return rec, nil
		}
		offset := r.readOffset
		r.mu.Unlock()
		recs, _, err := r.kafka.Fetch(ctx, r.topic, offset, r.fetchBytes)
		if err != nil {
			return KafkaRecord{}, err
		}
		if len(recs) == 0 {
			select {
			case <-ctx.Done():
				return KafkaRecord{}, ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		r.mu.Lock()
		for _, rec := range recs {
			if rec.Offset >= r.readOffset {
				r.pending = append(r.pending, rec)
				if rec.Offset+1 > r.readOffset {
					r.readOffset = rec.Offset + 1
				}
			}
		}
		r.mu.Unlock()
	}
}

func (r *Reader) finishGroup(tso uint64, group []decodedKafkaRecord) *cdcruntime.Transaction {
	last := group[len(group)-1]
	pos := Position{TSO: tso, Offset: last.record.Offset + 1, Offsets: map[int32]int64{0: last.record.Offset + 1}}
	events := make([]domain.CDCEvent, 0)
	var sourceMS int64
	for _, item := range group {
		for _, e := range item.events {
			if e.SourceTimestampMS > sourceMS {
				sourceMS = e.SourceTimestampMS
			}
			if e.Operation != domain.CDCCheckpoint {
				events = append(events, e)
			}
		}
	}
	if len(events) == 0 {
		events = []domain.CDCEvent{checkpointEvent(pos, r.resource, sourceMS)}
	}
	end := &events[len(events)-1]
	end.PositionType = "TIDB_TSO"
	end.PositionValue = pos.String()
	end.Resource = r.resource
	if end.SourceTimestampMS == 0 {
		end.SourceTimestampMS = sourceMS
	}
	return &cdcruntime.Transaction{Events: events, Checkpoint: eventPosition(*end), Label: fmt.Sprintf("ticdc:tso=%d offsets=%d-%d", tso, group[0].record.Offset, last.record.Offset)}
}

func (r *Reader) isPreviouslyAcknowledged(tso uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hasDurableAck && tso > 0 && tso <= r.acked.TSO
}

func (r *Reader) duplicateCheckpoint(item *decodedKafkaRecord) *cdcruntime.Transaction {
	r.mu.Lock()
	tso := r.acked.TSO
	r.mu.Unlock()
	pos := Position{TSO: tso, Offset: item.record.Offset + 1, Offsets: map[int32]int64{0: item.record.Offset + 1}}
	e := checkpointEvent(pos, r.resource, sourceMillis(0, item.tso))
	return &cdcruntime.Transaction{Events: []domain.CDCEvent{e}, Checkpoint: eventPosition(e), Label: fmt.Sprintf("ticdc:duplicate:%d", item.record.Offset)}
}

func containsDDL(events []domain.CDCEvent) bool {
	for _, e := range events {
		if e.Operation == domain.CDCDDL {
			return true
		}
	}
	return false
}

func eventPosition(e domain.CDCEvent) domain.CDCPosition {
	return domain.CDCPosition{DatabaseType: string(domain.DataSourceTiDB), PositionType: e.PositionType, PositionValue: e.PositionValue, Resource: e.Resource, SourceTimestampMS: e.SourceTimestampMS}
}
func checkpointEvent(pos Position, resource string, sourceMS int64) domain.CDCEvent {
	return domain.CDCEvent{Operation: domain.CDCCheckpoint, PositionType: "TIDB_TSO", PositionValue: pos.String(), Resource: resource, SourceTimestampMS: sourceMS}
}

func (r *Reader) Acknowledge(_ context.Context, tx *cdcruntime.Transaction) error {
	if tx == nil {
		return errors.New("cannot acknowledge nil TiCDC transaction")
	}
	pos, err := ParsePosition(tx.Checkpoint.PositionValue)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if pos.TSO < r.acked.TSO {
		return fmt.Errorf("TiCDC acknowledge TSO regressed from %d to %d", r.acked.TSO, pos.TSO)
	}
	oldOffsets := r.acked.normalizedOffsets()
	newOffsets := pos.normalizedOffsets()
	for partition, oldOffset := range oldOffsets {
		newOffset, ok := newOffsets[partition]
		if !ok {
			return fmt.Errorf("TiCDC acknowledge dropped durable Kafka partition %d", partition)
		}
		if newOffset < oldOffset {
			return fmt.Errorf("TiCDC acknowledge partition %d offset regressed from %d to %d", partition, oldOffset, newOffset)
		}
	}
	if r.multi {
		for _, partition := range r.partitions {
			if _, ok := newOffsets[partition]; !ok {
				return fmt.Errorf("TiCDC acknowledge is missing active Kafka partition %d", partition)
			}
		}
	}
	r.acked = clonePosition(pos)
	r.position = clonePosition(pos)
	r.hasDurableAck = true
	return nil
}

func (r *Reader) Acknowledged() Position {
	r.mu.Lock()
	defer r.mu.Unlock()
	return clonePosition(r.acked)
}
func (r *Reader) Close() error { return nil }
