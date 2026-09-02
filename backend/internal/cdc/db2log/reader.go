package db2log

import (
	"bytes"
	"context"
	"encoding/hex"
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
	MaxTransactionEvents = 100000
	MaxTransactionBytes  = 128 << 20
	MaxOpenTransactions  = 10000
)

type Agent interface {
	Position(context.Context) (*PositionResponse, error)
	Bootstrap(context.Context, BootstrapRequest) (*BootstrapResponse, error)
	Read(context.Context, LRI, int, int) (*ReadResponse, error)
}

type lobChunk struct {
	offset uint64
	data   []byte
}

type lobColumnBuffer struct {
	chunks     []lobChunk
	amountOnly bool
}

type lobGroup struct {
	tableKey        uint32
	byteOrder       string
	columns         map[uint16]*lobColumnBuffer
	xmlColumns      map[uint16][]byte
	xmlSeen         map[string][]byte
	vectorColumns   map[uint16][]byte
	vectorSeen      map[string][]byte
	informationOnly bool
}

func newLOBGroup(tableKey uint32, byteOrder string) *lobGroup {
	return &lobGroup{
		tableKey: tableKey, byteOrder: byteOrder,
		columns: map[uint16]*lobColumnBuffer{}, xmlColumns: map[uint16][]byte{}, xmlSeen: map[string][]byte{},
		vectorColumns: map[uint16][]byte{}, vectorSeen: map[string][]byte{},
	}
}

func (g *lobGroup) add(rec *LOBManagerRecord) error {
	if rec == nil {
		return nil
	}
	if rec.InformationOnly {
		g.informationOnly = true
		return nil
	}
	if rec.OriginalOperation != 1 && rec.OriginalOperation != 4 {
		return fmt.Errorf("DB2 LOB original operation %d cannot produce a complete INSERT/UPDATE after-image", rec.OriginalOperation)
	}
	b := g.columns[rec.ColumnID]
	if b == nil {
		b = &lobColumnBuffer{}
		g.columns[rec.ColumnID] = b
	}
	if rec.AmountOnly {
		b.amountOnly = true
		return nil
	}
	end := rec.ByteOffset + uint64(len(rec.Data))
	if end > MaxTransactionBytes {
		return fmt.Errorf("DB2 LOB column %d exceeds %d-byte transaction safety bound", rec.ColumnID, MaxTransactionBytes)
	}
	for _, old := range b.chunks {
		oldEnd := old.offset + uint64(len(old.data))
		if rec.ByteOffset < oldEnd && old.offset < end {
			// Exact duplicate chunks are harmless when a read window is retried;
			// any non-identical overlap is ambiguous and must fail closed.
			if old.offset == rec.ByteOffset && bytes.Equal(old.data, rec.Data) {
				return nil
			}
			return fmt.Errorf("DB2 LOB column %d has overlapping chunks at offsets %d and %d", rec.ColumnID, old.offset, rec.ByteOffset)
		}
	}
	b.chunks = append(b.chunks, lobChunk{offset: rec.ByteOffset, data: append([]byte(nil), rec.Data...)})
	return nil
}

func (g *lobGroup) addXML(rec *XMLManagerRecord) error {
	if rec == nil {
		return nil
	}
	key := rec.LRI.String()
	if old, ok := g.xmlSeen[key]; ok {
		if bytes.Equal(old, rec.Data) {
			return nil
		}
		return fmt.Errorf("DB2 XML log LRI %s was replayed with different bytes", key)
	}
	if int(rec.ColumnID) < 0 {
		return errors.New("invalid DB2 XML column id")
	}
	cur := g.xmlColumns[rec.ColumnID]
	if len(cur)+len(rec.Data) > MaxTransactionBytes {
		return fmt.Errorf("DB2 XML column %d exceeds %d-byte transaction safety bound", rec.ColumnID, MaxTransactionBytes)
	}
	g.xmlColumns[rec.ColumnID] = append(cur, rec.Data...)
	g.xmlSeen[key] = append([]byte(nil), rec.Data...)
	return nil
}

func (g *lobGroup) addVector(rec *VectorManagerRecord) error {
	if rec == nil {
		return nil
	}
	key := rec.LRI.String()
	if old, ok := g.vectorSeen[key]; ok {
		if bytes.Equal(old, rec.Data) {
			return nil
		}
		return fmt.Errorf("DB2 VECTOR log LRI %s was replayed with different bytes", key)
	}
	if old, ok := g.vectorColumns[rec.ColumnID]; ok {
		if bytes.Equal(old, rec.Data) {
			g.vectorSeen[key] = append([]byte(nil), rec.Data...)
			return nil
		}
		return fmt.Errorf("DB2 VECTOR column %d has multiple serialized values inside one out-of-row group", rec.ColumnID)
	}
	if len(rec.Data) == 0 || len(rec.Data) > MaxTransactionBytes {
		return fmt.Errorf("DB2 VECTOR column %d serialized size %d is outside safety bound", rec.ColumnID, len(rec.Data))
	}
	g.vectorColumns[rec.ColumnID] = append([]byte(nil), rec.Data...)
	g.vectorSeen[key] = append([]byte(nil), rec.Data...)
	return nil
}

func assembleLOBColumn(id uint16, b *lobColumnBuffer) ([]byte, error) {
	if b == nil {
		return nil, nil
	}
	if b.amountOnly {
		return nil, fmt.Errorf("DB2 LOB column %d is NOT LOGGED (ADD LOB AMOUNT has no bytes)", id)
	}
	if len(b.chunks) == 0 {
		return nil, nil
	}
	sort.Slice(b.chunks, func(i, j int) bool { return b.chunks[i].offset < b.chunks[j].offset })
	var out []byte
	var next uint64
	for _, c := range b.chunks {
		if c.offset != next {
			return nil, fmt.Errorf("DB2 LOB column %d has a gap: expected offset %d got %d", id, next, c.offset)
		}
		if uint64(len(out))+uint64(len(c.data)) > MaxTransactionBytes {
			return nil, fmt.Errorf("DB2 LOB column %d exceeds safety bound", id)
		}
		out = append(out, c.data...)
		next += uint64(len(c.data))
	}
	return out, nil
}

func (g *lobGroup) values(columns int) (map[int][]byte, error) {
	if g == nil {
		return nil, nil
	}
	bo, err := orderOf(g.byteOrder)
	if err != nil {
		return nil, err
	}
	out := map[int][]byte{}
	for id, b := range g.columns {
		raw, err := assembleLOBColumn(id, b)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			continue
		}
		if id == LOBColumnConsolidated {
			parts, err := splitConsolidatedVarying(raw, columns, bo)
			if err != nil {
				return nil, err
			}
			for ci, v := range parts {
				if old, exists := out[ci]; exists && !bytes.Equal(old, v) {
					return nil, fmt.Errorf("DB2 consolidated/direct LOB data conflict for column %d", ci)
				}
				out[ci] = v
			}
			continue
		}
		if int(id) >= columns {
			return nil, fmt.Errorf("DB2 LOB column id %d exceeds table column count %d", id, columns)
		}
		out[int(id)] = raw
	}
	for id, raw := range g.xmlColumns {
		if int(id) >= columns {
			return nil, fmt.Errorf("DB2 XML column id %d exceeds table column count %d", id, columns)
		}
		if old, exists := out[int(id)]; exists && !bytes.Equal(old, raw) {
			return nil, fmt.Errorf("DB2 XML/LOB data conflict for column %d", id)
		}
		out[int(id)] = append([]byte(nil), raw...)
	}
	for id, raw := range g.vectorColumns {
		if int(id) >= columns {
			return nil, fmt.Errorf("DB2 VECTOR column id %d exceeds table column count %d", id, columns)
		}
		if old, exists := out[int(id)]; exists && !bytes.Equal(old, raw) {
			return nil, fmt.Errorf("DB2 VECTOR/LOB/XML data conflict for column %d", id)
		}
		out[int(id)] = append([]byte(nil), raw...)
	}
	return out, nil
}

type txState struct {
	tid                     string
	events                  []domain.CDCEvent
	eventKeys               []db2EventKey
	bytes                   int
	unsafe                  string
	sourceMS                int64
	pendingLOB              *lobGroup
	lastRelocationCandidate *relocationCandidate
	pendingDecomposed       *decomposedDelete
}

type db2EventKey struct {
	tableKey uint32
	rid      string
	op       domain.CDCOperation
}

func isDMSRowMutation(fn byte) bool {
	switch fn {
	case DMSInsert, DMSInsertEmpty, DMSDelete, DMSDeleteEmpty, DMSUpdate, DMSMultiInsert:
		return true
	default:
		return false
	}
}

type relocationCandidate struct {
	eventID  string
	tableKey uint32
	rid      string
}

type decomposedDelete struct {
	tableKey uint32
	rid      string
	before   []domain.CDCField
	sourceMS int64
}

func (x *txState) appendEvent(key db2EventKey, ev domain.CDCEvent) error {
	x.events = append(x.events, ev)
	x.eventKeys = append(x.eventKeys, key)
	if len(x.events) != len(x.eventKeys) {
		return errors.New("DB2 transaction event identity ledger is inconsistent")
	}
	return nil
}

func (x *txState) clearRelocationCandidate() {
	x.lastRelocationCandidate = nil
}

func (x *txState) markRelocationCandidate(key db2EventKey, ev domain.CDCEvent) {
	x.lastRelocationCandidate = &relocationCandidate{eventID: ev.ID, tableKey: key.tableKey, rid: key.rid}
}

func (x *txState) linkIndirectUpdate(tableKey uint32, p *ParsedRecord, eventID string, sourceMS int64) error {
	if p == nil || !p.AfterInPrecedingInsert {
		return errors.New("DB2 indirect update linkage called without indirect update")
	}
	c := x.lastRelocationCandidate
	if c == nil {
		return errors.New("DB2 indirect update has no preceding 0x04 INSERT after-image candidate")
	}
	if c.tableKey != tableKey {
		return fmt.Errorf("DB2 indirect update table 0x%08x does not match preceding INSERT table 0x%08x", tableKey, c.tableKey)
	}
	if len(x.events) == 0 || len(x.events) != len(x.eventKeys) {
		return errors.New("DB2 indirect update event ledger is inconsistent")
	}
	i := len(x.events) - 1
	if x.events[i].ID != c.eventID || x.eventKeys[i].op != domain.CDCInsert || x.eventKeys[i].tableKey != tableKey {
		return errors.New("DB2 indirect update preceding INSERT is no longer the immediately preceding selected-table mutation")
	}
	x.events[i].Operation = domain.CDCUpdate
	x.events[i].Before = append([]domain.CDCField(nil), p.Before...)
	x.events[i].ID = eventID
	if sourceMS > x.events[i].SourceTimestampMS {
		x.events[i].SourceTimestampMS = sourceMS
	}
	x.eventKeys[i] = db2EventKey{tableKey: tableKey, rid: p.RID, op: domain.CDCUpdate}
	x.clearRelocationCandidate()
	return nil
}

func (x *txState) beginDecomposedDelete(tableKey uint32, p *ParsedRecord, sourceMS int64) error {
	if p == nil || len(p.Rows) != 1 || p.Rows[0].RID == "" || len(p.Rows[0].Before) == 0 {
		return errors.New("DB2 decomposed-update DELETE is missing RID/before-image")
	}
	if x.pendingDecomposed != nil {
		return errors.New("DB2 decomposed update started before the previous delete/insert pair completed")
	}
	x.clearRelocationCandidate()
	x.pendingDecomposed = &decomposedDelete{tableKey: tableKey, rid: p.Rows[0].RID, before: append([]domain.CDCField(nil), p.Rows[0].Before...), sourceMS: sourceMS}
	return nil
}

func (x *txState) finishDecomposedInsert(tableKey uint32, p *ParsedRecord, sel Selection, e RecordEnvelope, resource string) error {
	pending := x.pendingDecomposed
	if pending == nil {
		return errors.New("DB2 decomposed-update INSERT has no immediately preceding decomposed DELETE")
	}
	if pending.tableKey != tableKey {
		return fmt.Errorf("DB2 decomposed-update INSERT table 0x%08x does not match pending DELETE table 0x%08x", tableKey, pending.tableKey)
	}
	if p == nil || len(p.Rows) != 1 || p.Rows[0].RID == "" || len(p.Rows[0].After) == 0 {
		return errors.New("DB2 decomposed-update INSERT is missing RID/after-image")
	}
	ev := domain.CDCEvent{
		ID:                "db2:" + e.LRI.String(),
		Operation:         domain.CDCUpdate,
		SourceSchema:      sel.Schema,
		SourceTable:       sel.Table,
		Before:            pending.before,
		After:             p.Rows[0].After,
		Resource:          resource,
		SourceTimestampMS: max64(pending.sourceMS, e.SourceTimestampMS),
	}
	if err := x.appendEvent(db2EventKey{tableKey: tableKey, rid: pending.rid, op: domain.CDCUpdate}, ev); err != nil {
		return err
	}
	x.pendingDecomposed = nil
	x.clearRelocationCandidate()
	return nil
}

func (x *txState) removeLatest(tableKey uint32, rid string, ops ...domain.CDCOperation) error {
	if len(x.events) != len(x.eventKeys) {
		return errors.New("DB2 transaction event identity ledger is inconsistent")
	}
	allowed := func(op domain.CDCOperation) bool {
		for _, want := range ops {
			if op == want {
				return true
			}
		}
		return false
	}
	for i := len(x.eventKeys) - 1; i >= 0; i-- {
		k := x.eventKeys[i]
		if k.tableKey != tableKey || k.rid != rid || !allowed(k.op) {
			continue
		}
		removedID := x.events[i].ID
		x.events = append(x.events[:i], x.events[i+1:]...)
		x.eventKeys = append(x.eventKeys[:i], x.eventKeys[i+1:]...)
		if x.lastRelocationCandidate != nil && x.lastRelocationCandidate.eventID == removedID {
			x.clearRelocationCandidate()
		}
		return nil
	}
	return fmt.Errorf("DB2 rollback could not match prior selected-table change for RID %s", rid)
}

type Reader struct {
	agent        Agent
	selections   map[uint32]Selection
	descriptors  map[uint32]*TableDescriptor
	mu           sync.Mutex
	cursor       LRI
	acknowledged LRI
	transactions map[string]*txState
	parent       map[string]string
	ready        []*cdcruntime.Transaction
	resource     string
	poll         time.Duration
}

func NewReader(ctx context.Context, agent Agent, start LRI, selections []Selection, resource string) (*Reader, error) {
	if agent == nil {
		return nil, errors.New("DB2 CDC reader requires log agent")
	}
	if start.IsZero() {
		return nil, errors.New("DB2 CDC start LRI is required")
	}
	if len(selections) == 0 {
		return nil, errors.New("DB2 CDC requires selected tables")
	}
	r := &Reader{agent: agent, cursor: start, acknowledged: start, selections: map[uint32]Selection{}, descriptors: map[uint32]*TableDescriptor{}, transactions: map[string]*txState{}, parent: map[string]string{}, resource: resource, poll: 500 * time.Millisecond}
	ids := make([]TableIdentity, 0, len(selections))
	for _, s := range selections {
		if len(s.PrimaryKeys) == 0 {
			return nil, fmt.Errorf("DB2 CDC table %s.%s has no primary key", s.Schema, s.Table)
		}
		if len(s.Columns) == 0 {
			return nil, fmt.Errorf("DB2 CDC table %s.%s has no columns", s.Schema, s.Table)
		}
		r.selections[s.Key()] = s
		ids = append(ids, TableIdentity{Schema: s.Schema, Table: s.Table, TablespaceID: s.TablespaceID, TableID: s.TableID})
	}
	boot, err := agent.Bootstrap(ctx, BootstrapRequest{EndLRI: start, Tables: ids})
	if err != nil {
		return nil, fmt.Errorf("DB2 descriptor bootstrap: %w", err)
	}
	for _, e := range boot.Records {
		if err := r.consumeDescriptor(e); err != nil {
			return nil, err
		}
	}
	for k, s := range r.selections {
		if r.descriptors[k] == nil {
			return nil, fmt.Errorf("DB2 descriptor bootstrap did not find Initialize Table for %s.%s (tbspace=%d tableid=%d)", s.Schema, s.Table, s.TablespaceID, s.TableID)
		}
		if len(r.descriptors[k].Fields) != len(s.Columns) {
			return nil, fmt.Errorf("DB2 descriptor bootstrap column mismatch for %s.%s: log=%d catalog=%d", s.Schema, s.Table, len(r.descriptors[k].Fields), len(s.Columns))
		}
	}
	return r, nil
}

func (r *Reader) canonicalTID(tid string) string {
	tid = strings.ToLower(strings.TrimSpace(tid))
	seen := map[string]bool{}
	for {
		p := r.parent[tid]
		if p == "" || p == tid || seen[tid] {
			return tid
		}
		seen[tid] = true
		tid = p
	}
}
func (r *Reader) state(tid string) (*txState, error) {
	tid = r.canonicalTID(tid)
	if x := r.transactions[tid]; x != nil {
		return x, nil
	}
	if len(r.transactions) >= MaxOpenTransactions {
		return nil, fmt.Errorf("DB2 CDC open transaction count exceeds %d", MaxOpenTransactions)
	}
	x := &txState{tid: tid}
	r.transactions[tid] = x
	return x, nil
}
func (r *Reader) consumeDescriptor(e RecordEnvelope) error {
	p, err := ParseDataManager(e, nil, nil)
	if err != nil {
		return err
	}
	if p == nil || p.Descriptor == nil {
		return nil
	}
	key := uint32(p.TablespaceID)<<16 | uint32(p.TableID)
	if _, ok := r.selections[key]; ok {
		r.descriptors[key] = p.Descriptor
	}
	return nil
}
func (r *Reader) process(e RecordEnvelope) error {
	tid, err := NormalizeTID(e.TID)
	if err != nil {
		return fmt.Errorf("DB2 LRI %s: %w", e.LRI.String(), err)
	}
	switch e.LogType {
	case LogTypeSubtransaction:
		raw, err := decodeEnvelopeRaw(e)
		if err != nil {
			return err
		}
		if len(raw) < 46 {
			return errors.New("DB2 subtransaction record is truncated")
		}
		child := hex.EncodeToString(raw[40:46])
		child, err = NormalizeTID(child)
		if err != nil {
			return err
		}
		parent := r.canonicalTID(tid)
		r.parent[child] = parent
		if c := r.transactions[child]; c != nil {
			p, err := r.state(parent)
			if err != nil {
				return err
			}
			if c.pendingDecomposed != nil || p.pendingDecomposed != nil {
				return errors.New("DB2 subtransaction boundary encountered with an incomplete decomposed-update delete/insert pair")
			}
			if c.pendingLOB != nil {
				if p.pendingLOB != nil {
					return errors.New("DB2 subtransaction merge has two unconsumed out-of-row groups")
				}
				p.pendingLOB = c.pendingLOB
			}
			p.events = append(p.events, c.events...)
			p.eventKeys = append(p.eventKeys, c.eventKeys...)
			// A 0x02 UPDATE is documented to reference the preceding INSERT for the
			// transaction. Crossing a subtransaction merge makes "preceding" ambiguous,
			// so discard the optimization candidate and fail closed later if needed.
			p.clearRelocationCandidate()
			p.bytes += c.bytes
			if c.unsafe != "" {
				p.unsafe = c.unsafe
			}
			if c.sourceMS > p.sourceMS {
				p.sourceMS = c.sourceMS
			}
			delete(r.transactions, child)
		}
		return nil
	case LogTypeAbort, LogTypeHeuristicAbort:
		root := r.canonicalTID(tid)
		delete(r.transactions, root)
		return nil
	case LogTypeCommit, LogTypeHeuristicCommit, LogTypeMPPSubCommit, LogTypeMPPCoordCommit:
		root := r.canonicalTID(tid)
		x := r.transactions[root]
		delete(r.transactions, root)
		if x == nil {
			return nil
		}
		if x.unsafe != "" {
			return fmt.Errorf("DB2 transaction %s cannot be safely applied: %s", root, x.unsafe)
		}
		if x.pendingDecomposed != nil {
			return fmt.Errorf("DB2 transaction %s committed with an incomplete decomposed-update delete/insert pair", root)
		}
		if x.pendingLOB != nil {
			return fmt.Errorf("DB2 transaction %s committed with an unconsumed out-of-row LOB group", root)
		}
		if len(x.events) == 0 {
			return nil
		}
		pos := e.NextLRI
		if pos.IsZero() {
			return fmt.Errorf("DB2 commit %s has no next LRI", e.LRI.String())
		}
		last := &x.events[len(x.events)-1]
		last.PositionType = "DB2_LRI"
		last.PositionValue = pos.String()
		last.Resource = r.resource
		if last.SourceTimestampMS == 0 {
			last.SourceTimestampMS = max64(x.sourceMS, e.SourceTimestampMS)
		}
		r.ready = append(r.ready, &cdcruntime.Transaction{Events: x.events, Checkpoint: domain.CDCPosition{DatabaseType: string(domain.DataSourceDB2), PositionType: "DB2_LRI", PositionValue: pos.String(), Resource: r.resource, SourceTimestampMS: last.SourceTimestampMS}, Label: "db2 tid " + root})
		return nil
	case LogTypeNormal, LogTypeInformational, LogTypeCompensation:
	default:
		return nil
	}

	// Db2 12.1.4+ DMS function 213 carries a VECTOR column's string
	// representation after StartOutOfRow and before the following INSERT/UPDATE.
	vectorRec, err := ParseVectorManager(e)
	if err != nil {
		return err
	}
	if vectorRec != nil {
		key := uint32(vectorRec.TablespaceID)<<16 | uint32(vectorRec.TableID)
		if _, selected := r.selections[key]; !selected {
			return nil
		}
		x, err := r.state(tid)
		if err != nil {
			return err
		}
		if x.pendingLOB == nil {
			return fmt.Errorf("DB2 selected-table VECTOR record at %s arrived without start-of-out-of-row marker", e.LRI.String())
		}
		if x.pendingLOB.tableKey != key {
			return fmt.Errorf("DB2 VECTOR table 0x%08x does not match pending table 0x%08x", key, x.pendingLOB.tableKey)
		}
		if x.pendingLOB.byteOrder != "" && !strings.EqualFold(x.pendingLOB.byteOrder, e.ByteOrder) {
			return errors.New("DB2 VECTOR group changed byte order")
		}
		if err := x.pendingLOB.addVector(vectorRec); err != nil {
			return err
		}
		x.bytes += len(e.RawBase64)
		if x.bytes > MaxTransactionBytes {
			return fmt.Errorf("DB2 transaction %s exceeds %d-byte bound while buffering VECTOR data", x.tid, MaxTransactionBytes)
		}
		return nil
	}

	// Db2 11.5.8+ CSL component-15 records carry serialized XML chunks.
	// They are informational (0x0069), appear after DMS StartOutOfRow and
	// before the selected table row, and are concatenated in log order.
	xmlRec, err := ParseXMLManager(e)
	if err != nil {
		return err
	}
	if xmlRec != nil {
		key := uint32(xmlRec.ParentTablespaceID)<<16 | uint32(xmlRec.ParentTableID)
		if _, selected := r.selections[key]; !selected {
			return nil
		}
		x, err := r.state(tid)
		if err != nil {
			return err
		}
		if x.pendingLOB == nil {
			return fmt.Errorf("DB2 selected-table XML record at %s arrived without start-of-out-of-row marker", e.LRI.String())
		}
		if x.pendingLOB.tableKey != key {
			return fmt.Errorf("DB2 XML parent table 0x%08x does not match pending table 0x%08x", key, x.pendingLOB.tableKey)
		}
		if x.pendingLOB.byteOrder != "" && !strings.EqualFold(x.pendingLOB.byteOrder, e.ByteOrder) {
			return errors.New("DB2 XML group changed byte order")
		}
		if err := x.pendingLOB.addXML(xmlRec); err != nil {
			return err
		}
		x.bytes += len(e.RawBase64)
		if x.bytes > MaxTransactionBytes {
			return fmt.Errorf("DB2 transaction %s exceeds %d-byte bound while buffering XML data", x.tid, MaxTransactionBytes)
		}
		return nil
	}

	// LOB manager component-5 records are emitted between the DMS
	// start-of-out-of-row marker and the row record.  Accumulate only selected
	// parent tables; unrelated LOB objects are ignored.
	lob, err := ParseLOBManager(e)
	if err != nil {
		return err
	}
	if lob != nil {
		key := uint32(lob.ParentTablespaceID)<<16 | uint32(lob.ParentTableID)
		if _, selected := r.selections[key]; !selected {
			return nil
		}
		x, err := r.state(tid)
		if err != nil {
			return err
		}
		if x.pendingLOB == nil {
			return fmt.Errorf("DB2 selected-table LOB record at %s arrived without start-of-out-of-row marker", e.LRI.String())
		}
		if x.pendingLOB.tableKey != key {
			return fmt.Errorf("DB2 LOB parent table 0x%08x does not match pending table 0x%08x", key, x.pendingLOB.tableKey)
		}
		if x.pendingLOB.byteOrder != "" && !strings.EqualFold(x.pendingLOB.byteOrder, e.ByteOrder) {
			return errors.New("DB2 LOB group changed byte order")
		}
		if err := x.pendingLOB.add(lob); err != nil {
			return err
		}
		x.bytes += len(e.RawBase64)
		if x.bytes > MaxTransactionBytes {
			return fmt.Errorf("DB2 transaction %s exceeds %d-byte bound while buffering LOB data", x.tid, MaxTransactionBytes)
		}
		return nil
	}

	// First parse the DMS identity without assuming this table is selected.
	p, err := ParseDataManager(e, nil, nil)
	if err != nil {
		return err
	}
	if p == nil {
		return nil
	}
	key := uint32(p.TablespaceID)<<16 | uint32(p.TableID)
	sel, selected := r.selections[key]
	if p.Descriptor != nil {
		if selected {
			r.descriptors[key] = p.Descriptor
		}
		return nil
	}
	if !selected {
		// The documented outer-0x02 UPDATE refers to the preceding INSERT for
		// the *transaction*, not merely the preceding selected-table INSERT. An
		// intervening row mutation on a table the migration did not select makes a
		// previously remembered candidate stale and must prevent false linkage.
		if isDMSRowMutation(p.Function) {
			if x := r.transactions[r.canonicalTID(tid)]; x != nil {
				x.clearRelocationCandidate()
			}
		}
		return nil
	}
	desc := r.descriptors[key]
	if desc == nil {
		return fmt.Errorf("DB2 selected table %s.%s has no active log descriptor", sel.Schema, sel.Table)
	}
	x, err := r.state(tid)
	if err != nil {
		return err
	}
	if p.Function == DMSStartOutOfRow {
		x.clearRelocationCandidate()
		if x.pendingLOB != nil {
			return fmt.Errorf("DB2 transaction %s started a new out-of-row group before consuming the previous one", x.tid)
		}
		x.pendingLOB = newLOBGroup(key, e.ByteOrder)
		x.bytes += len(e.RawBase64)
		if x.bytes > MaxTransactionBytes {
			return fmt.Errorf("DB2 transaction %s exceeds %d-byte bound while starting out-of-row data", x.tid, MaxTransactionBytes)
		}
		return nil
	}
	if p.Function == DMSUndoOutOfRow {
		x.clearRelocationCandidate()
		// This marker compensates StartOutOfRow. If the row-level mutation was
		// already materialized the pending group is nil; otherwise discard the
		// speculative out-of-row bytes now that Db2 rolled them back.
		x.pendingLOB = nil
		x.bytes += len(e.RawBase64)
		if x.bytes > MaxTransactionBytes {
			return fmt.Errorf("DB2 transaction %s exceeds %d-byte bound while undoing out-of-row data", x.tid, MaxTransactionBytes)
		}
		return nil
	}
	var lobValues map[int][]byte
	if x.pendingLOB != nil {
		if x.pendingLOB.tableKey != key {
			return fmt.Errorf("DB2 pending out-of-row group belongs to table 0x%08x, row is 0x%08x", x.pendingLOB.tableKey, key)
		}
		lobValues, err = x.pendingLOB.values(len(sel.Columns))
		if err != nil {
			return fmt.Errorf("DB2 %s.%s LOB reconstruction: %w", sel.Schema, sel.Table, err)
		}
	}
	p, err = ParseDataManagerWithLOB(e, &sel, desc, lobValues)
	if err != nil {
		return fmt.Errorf("DB2 %s.%s LRI %s: %w", sel.Schema, sel.Table, e.LRI.String(), err)
	}
	if x.pendingLOB != nil && (p.Function == DMSInsert || p.Function == DMSInsertEmpty || p.Function == DMSUpdate || p.Function == DMSDelete || p.Function == DMSDeleteEmpty || p.Function == DMSMultiInsert) {
		x.pendingLOB = nil
	}
	if e.LogType == LogTypeCompensation {
		x.clearRelocationCandidate()
		if x.pendingDecomposed != nil {
			x.unsafe = "compensation arrived while a decomposed-update delete/insert pair was incomplete"
			return nil
		}
		if err := r.applyCompensation(x, key, p); err != nil {
			x.unsafe = err.Error()
			return nil
		}
		x.bytes += len(e.RawBase64)
		if x.bytes > MaxTransactionBytes {
			return fmt.Errorf("DB2 transaction %s exceeds %d-byte bound while applying compensation", x.tid, MaxTransactionBytes)
		}
		return nil
	}
	if p.UnsafeUndo {
		x.unsafe = "undo Data Manager record was not carried as a compensation log record"
		return nil
	}
	if p.AfterInPrecedingInsert {
		if x.pendingDecomposed != nil {
			return errors.New("DB2 indirect UPDATE arrived while a decomposed-update delete/insert pair was incomplete")
		}
		if err := x.linkIndirectUpdate(key, p, "db2:"+e.LRI.String(), e.SourceTimestampMS); err != nil {
			return fmt.Errorf("DB2 %s.%s LRI %s: %w", sel.Schema, sel.Table, e.LRI.String(), err)
		}
		x.bytes += len(e.RawBase64)
		if e.SourceTimestampMS > x.sourceMS {
			x.sourceMS = e.SourceTimestampMS
		}
		if len(x.events) > MaxTransactionEvents || x.bytes > MaxTransactionBytes {
			return fmt.Errorf("DB2 transaction %s exceeds bounds after indirect update linkage: events=%d bytes=%d", x.tid, len(x.events), x.bytes)
		}
		return nil
	}
	rows := p.Rows
	if len(rows) == 0 && (len(p.Before) > 0 || len(p.After) > 0) {
		rows = []ParsedRow{{RID: p.RID, Before: p.Before, After: p.After}}
	}
	if len(rows) == 0 {
		return nil
	}
	decomposed := p.IUDFlags&IUDFlagDecomposedUpdate != 0
	if decomposed && p.Function == DMSDelete {
		if err := x.beginDecomposedDelete(key, p, e.SourceTimestampMS); err != nil {
			return fmt.Errorf("DB2 %s.%s LRI %s: %w", sel.Schema, sel.Table, e.LRI.String(), err)
		}
		x.bytes += len(e.RawBase64)
		if x.bytes > MaxTransactionBytes {
			return fmt.Errorf("DB2 transaction %s exceeds %d-byte bound while buffering decomposed update", x.tid, MaxTransactionBytes)
		}
		return nil
	}
	if decomposed && p.Function == DMSInsert {
		if err := x.finishDecomposedInsert(key, p, sel, e, r.resource); err != nil {
			return fmt.Errorf("DB2 %s.%s LRI %s: %w", sel.Schema, sel.Table, e.LRI.String(), err)
		}
		x.bytes += len(e.RawBase64)
		if e.SourceTimestampMS > x.sourceMS {
			x.sourceMS = e.SourceTimestampMS
		}
		if len(x.events) > MaxTransactionEvents || x.bytes > MaxTransactionBytes {
			return fmt.Errorf("DB2 transaction %s exceeds bounds after decomposed update: events=%d bytes=%d", x.tid, len(x.events), x.bytes)
		}
		return nil
	}
	if x.pendingDecomposed != nil {
		return fmt.Errorf("DB2 %s.%s LRI %s interrupted an incomplete decomposed-update delete/insert pair with DMS function %d", sel.Schema, sel.Table, e.LRI.String(), p.Function)
	}
	// A prior outer-0x04 INSERT is only eligible for the immediately following
	// selected-table indirect UPDATE. Any other selected mutation proves it was a
	// standalone complete INSERT, so stop considering it a relocation candidate.
	x.clearRelocationCandidate()
	for i, row := range rows {
		var ev domain.CDCEvent
		switch {
		case len(row.After) > 0 && len(row.Before) == 0:
			ev = domain.CDCEvent{Operation: domain.CDCInsert, After: row.After}
		case len(row.Before) > 0 && len(row.After) == 0:
			ev = domain.CDCEvent{Operation: domain.CDCDelete, Before: row.Before}
		case len(row.Before) > 0 && len(row.After) > 0:
			ev = domain.CDCEvent{Operation: domain.CDCUpdate, Before: row.Before, After: row.After}
		default:
			continue
		}
		ev.SourceSchema = sel.Schema
		ev.SourceTable = sel.Table
		ev.Resource = r.resource
		ev.SourceTimestampMS = e.SourceTimestampMS
		ev.ID = "db2:" + e.LRI.String()
		if len(rows) > 1 {
			ev.ID = fmt.Sprintf("%s#%d", ev.ID, i)
		}
		eventKey := db2EventKey{tableKey: key, rid: row.RID, op: ev.Operation}
		if err := x.appendEvent(eventKey, ev); err != nil {
			return err
		}
		if p.Function == DMSInsert && len(rows) == 1 && p.RowOuterType&0x04 != 0 {
			x.markRelocationCandidate(eventKey, ev)
		}
	}
	x.bytes += len(e.RawBase64)
	if e.SourceTimestampMS > x.sourceMS {
		x.sourceMS = e.SourceTimestampMS
	}
	if len(x.events) > MaxTransactionEvents || x.bytes > MaxTransactionBytes {
		return fmt.Errorf("DB2 transaction %s exceeds bounds: events=%d bytes=%d", x.tid, len(x.events), x.bytes)
	}
	return nil
}

func (r *Reader) applyCompensation(x *txState, tableKey uint32, p *ParsedRecord) error {
	if x == nil || p == nil {
		return errors.New("DB2 compensation has no transaction/Data Manager state")
	}
	switch p.Function {
	case DMSUndoInsert, DMSUndoInsertEmpty:
		if len(p.Rows) != 1 || p.Rows[0].RID == "" {
			return errors.New("DB2 undo-insert compensation has no RID")
		}
		return x.removeLatest(tableKey, p.Rows[0].RID, domain.CDCInsert)
	case DMSUndoDelete, DMSUndoDeleteEmpty:
		if len(p.Rows) != 1 || p.Rows[0].RID == "" {
			return errors.New("DB2 undo-delete compensation has no RID")
		}
		return x.removeLatest(tableKey, p.Rows[0].RID, domain.CDCDelete)
	case DMSUndoUpdate:
		if len(p.Rows) != 1 || p.Rows[0].RID == "" {
			return errors.New("DB2 undo-update compensation has no RID")
		}
		return x.removeLatest(tableKey, p.Rows[0].RID, domain.CDCUpdate)
	case DMSUndoMultiInsert:
		if len(p.Rows) == 0 {
			return errors.New("DB2 undo-multi-insert compensation has no rollback descriptions")
		}
		for _, row := range p.Rows {
			if row.RID == "" {
				return errors.New("DB2 undo-multi-insert compensation contains an empty RID")
			}
			if err := x.removeLatest(tableKey, row.RID, domain.CDCInsert); err != nil {
				return err
			}
		}
		return nil
	case DMSUndoOutOfRow:
		x.pendingLOB = nil
		return nil
	default:
		return fmt.Errorf("unsupported selected-table DB2 compensation Data Manager function %d", p.Function)
	}
}

func (r *Reader) load(ctx context.Context) error {
	for len(r.ready) == 0 {
		resp, err := r.agent.Read(ctx, r.cursor, 4096, 32<<20)
		if err != nil {
			return err
		}
		if len(resp.Records) == 0 {
			if resp.ReadToCurrent && len(r.transactions) == 0 && !resp.NextStartLRI.IsZero() && CompareLRI(resp.NextStartLRI, r.cursor) > 0 {
				pos := resp.NextStartLRI
				e := domain.CDCEvent{ID: "db2-checkpoint:" + pos.String(), Operation: domain.CDCCheckpoint, PositionType: "DB2_LRI", PositionValue: pos.String(), Resource: r.resource}
				r.ready = append(r.ready, &cdcruntime.Transaction{Events: []domain.CDCEvent{e}, Checkpoint: domain.CDCPosition{DatabaseType: string(domain.DataSourceDB2), PositionType: "DB2_LRI", PositionValue: pos.String(), Resource: r.resource}, Label: "db2 checkpoint " + pos.String()})
				r.cursor = pos
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.poll):
				continue
			}
		}
		for _, e := range resp.Records {
			if CompareLRI(e.LRI, r.cursor) < 0 {
				continue
			}
			if err := r.process(e); err != nil {
				return err
			}
			if !e.NextLRI.IsZero() && CompareLRI(e.NextLRI, r.cursor) > 0 {
				r.cursor = e.NextLRI
			}
		}
		if !resp.NextStartLRI.IsZero() && CompareLRI(resp.NextStartLRI, r.cursor) > 0 {
			r.cursor = resp.NextStartLRI
		}
		// A non-empty window can contain only aborted/unrelated work. Once
		// the provider says the window is current and no selected transaction
		// remains open, persist the read boundary through the normal apply path
		// so restart does not rescan it forever. Never checkpoint across an open
		// selected transaction because its earlier row records would be lost.
		if len(r.ready) == 0 && resp.ReadToCurrent && len(r.transactions) == 0 && CompareLRI(r.cursor, r.acknowledged) > 0 {
			pos := r.cursor
			e := domain.CDCEvent{ID: "db2-checkpoint:" + pos.String(), Operation: domain.CDCCheckpoint, PositionType: "DB2_LRI", PositionValue: pos.String(), Resource: r.resource}
			r.ready = append(r.ready, &cdcruntime.Transaction{Events: []domain.CDCEvent{e}, Checkpoint: domain.CDCPosition{DatabaseType: string(domain.DataSourceDB2), PositionType: "DB2_LRI", PositionValue: pos.String(), Resource: r.resource}, Label: "db2 checkpoint " + pos.String()})
		}
	}
	return nil
}
func (r *Reader) Next(ctx context.Context) (*cdcruntime.Transaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ready) == 0 {
		if err := r.load(ctx); err != nil {
			return nil, err
		}
	}
	tx := r.ready[0]
	r.ready = r.ready[1:]
	return tx, nil
}
func (r *Reader) Acknowledge(_ context.Context, tx *cdcruntime.Transaction) error {
	if tx == nil {
		return errors.New("cannot acknowledge nil DB2 transaction")
	}
	p, err := ParseLRI(tx.Checkpoint.PositionValue)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if CompareLRI(p, r.acknowledged) < 0 {
		return fmt.Errorf("DB2 acknowledge LRI regressed from %s to %s", r.acknowledged.String(), p.String())
	}
	r.acknowledged = p
	return nil
}
func (r *Reader) Close() error      { return nil }
func (r *Reader) Acknowledged() LRI { r.mu.Lock(); defer r.mu.Unlock(); return r.acknowledged }
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
