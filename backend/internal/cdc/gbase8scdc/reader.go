package gbase8scdc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
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

const (
	KindBegin        = "BEGIN"
	KindCommit       = "COMMIT"
	KindRollback     = "ROLLBACK"
	KindInsert       = "INSERT"
	KindDelete       = "DELETE"
	KindUpdateBefore = "UPDATE_BEFORE"
	KindUpdateAfter  = "UPDATE_AFTER"
	KindDiscard      = "DISCARD"
	KindTruncate     = "TRUNCATE"
	KindTableSchema  = "TABLE_SCHEMA"
	KindTimeout      = "TIMEOUT"
	KindError        = "ERROR"
)

type txEvent struct {
	seq   uint64
	event domain.CDCEvent
}
type txState struct {
	beginSeq      uint64
	events        []txEvent
	bytes         int
	pendingBefore *RecordEnvelope
	truncateSeq   uint64
	lastTimestamp int64
}

type durablePosition struct {
	Restart, Commit uint64
	CaptureLineage  string
}

func parseSequence(v string) (uint64, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, errors.New("empty sequence")
	}
	return strconv.ParseUint(v, 10, 64)
}
func sequenceString(v uint64) string { return strconv.FormatUint(v, 10) }
func parsePosition(v string) (durablePosition, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return durablePosition{}, errors.New("empty GBase 8s CDC position")
	}
	if !strings.Contains(v, "=") {
		n, e := parseSequence(v)
		return durablePosition{Restart: n, Commit: n}, e
	}
	var p durablePosition
	seenR, seenC, seenCapture := false, false, false
	for _, part := range strings.Split(v, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return p, fmt.Errorf("invalid GBase 8s CDC position %q", v)
		}
		key, value := strings.ToLower(strings.TrimSpace(kv[0])), strings.TrimSpace(kv[1])
		switch key {
		case "restart":
			n, e := parseSequence(value)
			if e != nil {
				return p, e
			}
			p.Restart, seenR = n, true
		case "commit":
			n, e := parseSequence(value)
			if e != nil {
				return p, e
			}
			p.Commit, seenC = n, true
		case "capture":
			lineage, e := NormalizeCaptureLineage(value)
			if e != nil {
				return p, e
			}
			p.CaptureLineage, seenCapture = lineage, true
		default:
			return p, fmt.Errorf("unknown GBase 8s CDC position field %q", kv[0])
		}
	}
	if !seenR || !seenC {
		return p, fmt.Errorf("GBase 8s CDC position requires restart and commit: %q", v)
	}
	if p.Restart > p.Commit {
		return p, fmt.Errorf("GBase 8s CDC restart position %d is after commit %d", p.Restart, p.Commit)
	}
	if !seenCapture {
		p.CaptureLineage = ""
	}
	return p, nil
}

func formatPosition(p durablePosition) string {
	if strings.TrimSpace(p.CaptureLineage) == "" {
		return fmt.Sprintf("restart=%d;commit=%d", p.Restart, p.Commit)
	}
	return fmt.Sprintf("restart=%d;commit=%d;capture=%s", p.Restart, p.Commit, p.CaptureLineage)
}

func InitialPosition(sequence, captureLineage string) (string, error) {
	n, err := parseSequence(sequence)
	if err != nil {
		return "", err
	}
	lineage, err := NormalizeCaptureLineage(captureLineage)
	if err != nil {
		return "", err
	}
	return formatPosition(durablePosition{Restart: n, Commit: n, CaptureLineage: lineage}), nil
}

func eventBytes(e domain.CDCEvent) int {
	n := len(e.SourceSchema) + len(e.SourceTable) + len(e.SQL)
	for _, f := range e.Before {
		n += len(f.Column) + len(f.Value) + len(f.Encoding) + 1
	}
	for _, f := range e.After {
		n += len(f.Column) + len(f.Value) + len(f.Encoding) + 1
	}
	return n
}
func cloneFields(in []domain.CDCField) []domain.CDCField {
	out := make([]domain.CDCField, len(in))
	copy(out, in)
	return out
}

func recordEnvelopeBytes(rec RecordEnvelope) int {
	n := len(rec.Kind) + len(rec.Sequence) + len(rec.ErrorText) + 32
	for _, f := range rec.Fields {
		n += len(f.Column) + len(f.Value) + len(f.Encoding) + 1
	}
	for _, p := range rec.SmartLOBProofs {
		n += len(p.Column) + len(p.Kind) + len(p.SHA256) + len(p.Acquisition) + 24
	}
	return n
}

func validateProviderFields(sel TableSelection, fields []domain.CDCField) error {
	if len(fields) != len(sel.Columns) {
		return fmt.Errorf("GBase 8s CDC provider returned %d columns for %s.%s; expected %d full-row columns", len(fields), sel.Schema, sel.Table, len(sel.Columns))
	}
	for i, f := range fields {
		if !strings.EqualFold(strings.TrimSpace(f.Column), strings.TrimSpace(sel.Columns[i])) {
			return fmt.Errorf("GBase 8s CDC provider column %d for %s.%s is %q; expected %q", i, sel.Schema, sel.Table, f.Column, sel.Columns[i])
		}
		enc := strings.ToLower(strings.TrimSpace(f.Encoding))
		if f.Null {
			if f.Value != "" || enc != "" {
				return fmt.Errorf("GBase 8s CDC NULL field %s.%s.%s must not carry value/encoding", sel.Schema, sel.Table, f.Column)
			}
			continue
		}
		switch enc {
		case "":
		case "base64":
			if _, err := base64.StdEncoding.DecodeString(f.Value); err != nil {
				return fmt.Errorf("GBase 8s CDC provider field %s.%s.%s has invalid base64: %w", sel.Schema, sel.Table, f.Column, err)
			}
		default:
			return fmt.Errorf("GBase 8s CDC provider field %s.%s.%s has unsupported encoding %q", sel.Schema, sel.Table, f.Column, f.Encoding)
		}
	}
	return nil
}

func decodedProviderFieldBytes(f domain.CDCField) ([]byte, error) {
	if f.Null {
		return nil, nil
	}
	switch strings.ToLower(strings.TrimSpace(f.Encoding)) {
	case "":
		return []byte(f.Value), nil
	case "base64":
		b, err := base64.StdEncoding.DecodeString(f.Value)
		if err != nil {
			return nil, err
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported encoding %q", f.Encoding)
	}
}

func validateSmartLOBProofs(sel TableSelection, rec RecordEnvelope) error {
	required := map[string]string{}
	for _, c := range sel.SchemaColumns {
		kind := strings.ToLower(strings.TrimSpace(c.SmartLOB))
		if kind == "" {
			kind = smartLOBKind(c.ColumnType)
		}
		if kind != "" {
			required[strings.ToLower(strings.TrimSpace(c.Name))] = kind
		}
	}
	if len(required) == 0 {
		if len(rec.SmartLOBProofs) != 0 {
			return fmt.Errorf("GBase 8s CDC provider returned smart-LOB proofs for non-LOB table %s.%s", sel.Schema, sel.Table)
		}
		return nil
	}
	proofs := make(map[string]SmartLOBImageProof, len(rec.SmartLOBProofs))
	for _, proof := range rec.SmartLOBProofs {
		col := strings.ToLower(strings.TrimSpace(proof.Column))
		kind, ok := required[col]
		if !ok {
			return fmt.Errorf("GBase 8s CDC provider returned smart-LOB proof for non-smart-LOB column %s.%s.%s", sel.Schema, sel.Table, proof.Column)
		}
		if _, exists := proofs[col]; exists {
			return fmt.Errorf("GBase 8s CDC provider returned duplicate smart-LOB proof for %s.%s.%s", sel.Schema, sel.Table, proof.Column)
		}
		if strings.ToLower(strings.TrimSpace(proof.Kind)) != kind {
			return fmt.Errorf("GBase 8s CDC smart-LOB proof kind mismatch for %s.%s.%s: got %q want %q", sel.Schema, sel.Table, proof.Column, proof.Kind, kind)
		}
		if strings.TrimSpace(proof.Acquisition) != SmartLOBImageContract {
			return fmt.Errorf("GBase 8s CDC smart-LOB proof for %s.%s.%s uses unsafe acquisition %q; current-row lookup is never accepted", sel.Schema, sel.Table, proof.Column, proof.Acquisition)
		}
		sha := strings.ToLower(strings.TrimSpace(proof.SHA256))
		if len(sha) != 64 {
			return fmt.Errorf("GBase 8s CDC smart-LOB proof for %s.%s.%s has invalid SHA-256 length", sel.Schema, sel.Table, proof.Column)
		}
		if _, err := hex.DecodeString(sha); err != nil {
			return fmt.Errorf("GBase 8s CDC smart-LOB proof for %s.%s.%s has invalid SHA-256: %w", sel.Schema, sel.Table, proof.Column, err)
		}
		if proof.ByteLength < 0 {
			return fmt.Errorf("GBase 8s CDC smart-LOB proof for %s.%s.%s has negative byte length", sel.Schema, sel.Table, proof.Column)
		}
		proof.SHA256 = sha
		proofs[col] = proof
	}
	for _, f := range rec.Fields {
		col := strings.ToLower(strings.TrimSpace(f.Column))
		kind, isLOB := required[col]
		if !isLOB {
			continue
		}
		proof, hasProof := proofs[col]
		if f.Null {
			if hasProof {
				return fmt.Errorf("GBase 8s CDC NULL smart-LOB %s.%s.%s must not carry an image proof", sel.Schema, sel.Table, f.Column)
			}
			continue
		}
		if !hasProof {
			return fmt.Errorf("GBase 8s CDC non-NULL smart-LOB %s.%s.%s has no event-owned image proof", sel.Schema, sel.Table, f.Column)
		}
		if kind == "blob" && strings.ToLower(strings.TrimSpace(f.Encoding)) != "base64" {
			return fmt.Errorf("GBase 8s CDC BLOB %s.%s.%s must use base64 transport", sel.Schema, sel.Table, f.Column)
		}
		b, err := decodedProviderFieldBytes(f)
		if err != nil {
			return fmt.Errorf("GBase 8s CDC smart-LOB %s.%s.%s decode: %w", sel.Schema, sel.Table, f.Column, err)
		}
		if int64(len(b)) != proof.ByteLength {
			return fmt.Errorf("GBase 8s CDC smart-LOB %s.%s.%s byte length mismatch: image=%d proof=%d", sel.Schema, sel.Table, f.Column, len(b), proof.ByteLength)
		}
		got := fmt.Sprintf("%x", sha256.Sum256(b))
		if got != proof.SHA256 {
			return fmt.Errorf("GBase 8s CDC smart-LOB %s.%s.%s SHA-256 mismatch", sel.Schema, sel.Table, f.Column)
		}
		delete(proofs, col)
	}
	if len(proofs) != 0 {
		return fmt.Errorf("GBase 8s CDC provider returned smart-LOB proofs without matching non-NULL fields for %s.%s", sel.Schema, sel.Table)
	}
	return nil
}

func validateProviderRecord(sel TableSelection, rec RecordEnvelope) error {
	if err := validateProviderFields(sel, rec.Fields); err != nil {
		return err
	}
	return validateSmartLOBProofs(sel, rec)
}

// Reader owns transaction/restart semantics. Provider Read is allowed to move a
// live CSDK session forward, but only Acknowledge changes the durable commit
// watermark. Durable restart rewinds to the earliest still-open BEGIN, matching
// the syscdcv1 recovery rule for long transactions.
type Reader struct {
	agent                               Agent
	database                            string
	selections                          []TableSelection
	tables                              map[int]TableSelection
	resource                            string
	poll                                time.Duration
	maxRecords, maxBytes                int
	mu                                  sync.Mutex
	ackedCommit, restartSeq, liveCursor uint64
	captureLineage                      string
	queue                               []RecordEnvelope
	open                                map[uint64]*txState
}

func NewReader(agent Agent, database, start string, selections []TableSelection, resource string) (*Reader, error) {
	if agent == nil {
		return nil, errors.New("GBase 8s CDC reader requires provider agent")
	}
	pos, err := parsePosition(start)
	if err != nil {
		return nil, fmt.Errorf("invalid GBase 8s CDC start position: %w", err)
	}
	if strings.TrimSpace(pos.CaptureLineage) == "" {
		return nil, errors.New("GBase 8s CDC RC24 checkpoint has no capture lineage; restart from a newly captured pre-Full checkpoint instead of attaching an RC23/older checkpoint to a new provider session")
	}
	if len(selections) == 0 {
		return nil, errors.New("GBase 8s CDC reader requires selected tables")
	}
	tables := make(map[int]TableSelection, len(selections))
	for _, s := range selections {
		if err := ValidateTableSelection(s); err != nil {
			return nil, err
		}
		if _, ok := tables[s.ID]; ok {
			return nil, fmt.Errorf("duplicate GBase 8s CDC table id %d", s.ID)
		}
		tables[s.ID] = s
	}
	return &Reader{agent: agent, database: strings.TrimSpace(database), selections: append([]TableSelection(nil), selections...), tables: tables, resource: strings.TrimSpace(resource), poll: time.Second, maxRecords: 4096, maxBytes: 32 << 20, ackedCommit: pos.Commit, restartSeq: pos.Restart, liveCursor: pos.Restart, captureLineage: pos.CaptureLineage, open: map[uint64]*txState{}}, nil
}
func (r *Reader) SetPolling(p time.Duration, mr, mb int) {
	if p > 0 {
		r.poll = p
	}
	if mr > 0 {
		r.maxRecords = mr
	}
	if mb > 0 {
		r.maxBytes = mb
	}
}
func (r *Reader) recordEvent(rec RecordEnvelope, op domain.CDCOperation, before, after []domain.CDCField) (domain.CDCEvent, error) {
	t, ok := r.tables[rec.TableID]
	if !ok {
		return domain.CDCEvent{}, fmt.Errorf("GBase 8s CDC record references unknown table id %d", rec.TableID)
	}
	if _, err := parseSequence(rec.Sequence); err != nil {
		return domain.CDCEvent{}, err
	}
	if before != nil {
		if err := validateProviderFields(t, before); err != nil {
			return domain.CDCEvent{}, err
		}
	}
	if after != nil {
		if err := validateProviderFields(t, after); err != nil {
			return domain.CDCEvent{}, err
		}
	}
	return domain.CDCEvent{ID: "gbase8s:" + rec.Sequence, Operation: op, SourceSchema: t.Schema, SourceTable: t.Table, Before: cloneFields(before), After: cloneFields(after), PositionType: "GBASE8S_CDC_SEQ", PositionValue: rec.Sequence, Resource: r.resource, SourceTimestampMS: rec.SourceTimestampMS}, nil
}
func (r *Reader) appendEvent(txid uint64, rec RecordEnvelope, e domain.CDCEvent) error {
	tx := r.open[txid]
	if tx == nil {
		return fmt.Errorf("GBase 8s CDC DML for transaction %d arrived before BEGIN", txid)
	}
	seq, err := parseSequence(rec.Sequence)
	if err != nil {
		return err
	}
	if seq < tx.beginSeq {
		return fmt.Errorf("GBase 8s CDC record %d precedes transaction %d BEGIN %d", seq, txid, tx.beginSeq)
	}
	if len(tx.events) >= MaxTransactionEvents {
		return fmt.Errorf("GBase 8s CDC transaction %d exceeds %d events", txid, MaxTransactionEvents)
	}
	sz := eventBytes(e)
	if tx.bytes+sz > MaxTransactionBytes {
		return fmt.Errorf("GBase 8s CDC transaction %d exceeds %d bytes", txid, MaxTransactionBytes)
	}
	tx.events = append(tx.events, txEvent{seq, e})
	tx.bytes += sz
	if rec.SourceTimestampMS > tx.lastTimestamp {
		tx.lastTimestamp = rec.SourceTimestampMS
	}
	return nil
}
func (r *Reader) restartAt(commit uint64) uint64 {
	restart := commit
	for _, tx := range r.open {
		if tx.beginSeq < restart {
			restart = tx.beginSeq
		}
	}
	return restart
}

func ensureBeforeTruncate(tx *txState, txid uint64, kind string) error {
	if tx != nil && tx.truncateSeq != 0 {
		return fmt.Errorf("GBase 8s CDC %s for transaction %d follows TRUNCATE sequence %d; provider stream violates source transaction rules", kind, txid, tx.truncateSeq)
	}
	return nil
}

func (r *Reader) handle(rec RecordEnvelope) (*cdcruntime.Transaction, error) {
	kind := strings.ToUpper(strings.TrimSpace(rec.Kind))
	var seq uint64
	var err error
	if strings.TrimSpace(rec.Sequence) != "" {
		seq, err = parseSequence(rec.Sequence)
		if err != nil {
			return nil, err
		}
	}
	switch kind {
	case KindTableSchema:
		table, ok := r.tables[rec.TableID]
		if !ok {
			return nil, fmt.Errorf("GBase 8s CDC TABLE_SCHEMA references unknown table id %d", rec.TableID)
		}
		fp := strings.ToLower(strings.TrimSpace(rec.SchemaFingerprint))
		if fp == "" {
			return nil, fmt.Errorf("GBase 8s CDC TABLE_SCHEMA for %s.%s has no schema fingerprint", table.Schema, table.Table)
		}
		if fp != strings.ToLower(strings.TrimSpace(table.SchemaFingerprint)) {
			return nil, fmt.Errorf("GBase 8s CDC TABLE_SCHEMA drift for %s.%s: provider=%s planned=%s", table.Schema, table.Table, fp, table.SchemaFingerprint)
		}
		return nil, nil
	case KindTimeout:
		return nil, nil
	case KindError:
		if rec.ErrorText == "" {
			rec.ErrorText = "provider reported CDC session error"
		}
		return nil, fmt.Errorf("GBase 8s CDC provider error %d: %s", rec.ErrorCode, rec.ErrorText)
	case KindBegin:
		if rec.TransactionID == 0 {
			return nil, errors.New("GBase 8s CDC BEGIN has transaction id 0")
		}
		if len(r.open) >= MaxOpenTransactions {
			return nil, fmt.Errorf("GBase 8s CDC open transactions exceed %d", MaxOpenTransactions)
		}
		if _, ok := r.open[rec.TransactionID]; ok {
			return nil, fmt.Errorf("duplicate GBase 8s CDC BEGIN for transaction %d", rec.TransactionID)
		}
		r.open[rec.TransactionID] = &txState{beginSeq: seq, lastTimestamp: rec.SourceTimestampMS}
		return nil, nil
	case KindRollback:
		delete(r.open, rec.TransactionID)
		return nil, nil
	case KindDiscard:
		tx := r.open[rec.TransactionID]
		if tx == nil {
			return nil, fmt.Errorf("GBase 8s CDC DISCARD references unknown transaction %d", rec.TransactionID)
		}
		if err := ensureBeforeTruncate(tx, rec.TransactionID, KindDiscard); err != nil {
			return nil, err
		}
		kept := tx.events[:0]
		bytes := 0
		for _, e := range tx.events {
			if e.seq < seq {
				kept = append(kept, e)
				bytes += eventBytes(e.event)
			}
		}
		tx.events = kept
		tx.bytes = bytes
		if tx.pendingBefore != nil {
			pseq, _ := parseSequence(tx.pendingBefore.Sequence)
			if pseq >= seq {
				tx.pendingBefore = nil
			}
		}
		return nil, nil
	case KindInsert:
		if err := ensureBeforeTruncate(r.open[rec.TransactionID], rec.TransactionID, KindInsert); err != nil {
			return nil, err
		}
		table, ok := r.tables[rec.TableID]
		if !ok {
			return nil, fmt.Errorf("GBase 8s CDC record references unknown table id %d", rec.TableID)
		}
		if err := validateProviderRecord(table, rec); err != nil {
			return nil, err
		}
		e, e2 := r.recordEvent(rec, domain.CDCInsert, nil, rec.Fields)
		if e2 != nil {
			return nil, e2
		}
		return nil, r.appendEvent(rec.TransactionID, rec, e)
	case KindDelete:
		if err := ensureBeforeTruncate(r.open[rec.TransactionID], rec.TransactionID, KindDelete); err != nil {
			return nil, err
		}
		table, ok := r.tables[rec.TableID]
		if !ok {
			return nil, fmt.Errorf("GBase 8s CDC record references unknown table id %d", rec.TableID)
		}
		if err := validateProviderRecord(table, rec); err != nil {
			return nil, err
		}
		e, e2 := r.recordEvent(rec, domain.CDCDelete, rec.Fields, nil)
		if e2 != nil {
			return nil, e2
		}
		return nil, r.appendEvent(rec.TransactionID, rec, e)
	case KindUpdateBefore:
		tx := r.open[rec.TransactionID]
		if tx == nil {
			return nil, fmt.Errorf("GBase 8s CDC UPDATE_BEFORE for transaction %d arrived before BEGIN", rec.TransactionID)
		}
		if err := ensureBeforeTruncate(tx, rec.TransactionID, KindUpdateBefore); err != nil {
			return nil, err
		}
		if tx.pendingBefore != nil {
			return nil, fmt.Errorf("GBase 8s CDC transaction %d has overlapping UPDATE_BEFORE records", rec.TransactionID)
		}
		table, ok := r.tables[rec.TableID]
		if !ok {
			return nil, fmt.Errorf("GBase 8s CDC record references unknown table id %d", rec.TableID)
		}
		if err := validateProviderRecord(table, rec); err != nil {
			return nil, err
		}
		if tx.bytes+recordEnvelopeBytes(rec) > MaxTransactionBytes {
			return nil, fmt.Errorf("GBase 8s CDC transaction %d exceeds %d bytes while buffering UPDATE_BEFORE", rec.TransactionID, MaxTransactionBytes)
		}
		copyRec := rec
		copyRec.Fields = cloneFields(rec.Fields)
		copyRec.SmartLOBProofs = append([]SmartLOBImageProof(nil), rec.SmartLOBProofs...)
		tx.pendingBefore = &copyRec
		return nil, nil
	case KindUpdateAfter:
		tx := r.open[rec.TransactionID]
		if tx == nil || tx.pendingBefore == nil {
			return nil, fmt.Errorf("GBase 8s CDC UPDATE_AFTER for transaction %d has no matching before image", rec.TransactionID)
		}
		if err := ensureBeforeTruncate(tx, rec.TransactionID, KindUpdateAfter); err != nil {
			return nil, err
		}
		before := tx.pendingBefore
		if before.TableID != rec.TableID {
			return nil, fmt.Errorf("GBase 8s CDC UPDATE pair crosses table ids %d/%d", before.TableID, rec.TableID)
		}
		table, ok := r.tables[rec.TableID]
		if !ok {
			return nil, fmt.Errorf("GBase 8s CDC record references unknown table id %d", rec.TableID)
		}
		if err := validateProviderRecord(table, rec); err != nil {
			return nil, err
		}
		e, e2 := r.recordEvent(rec, domain.CDCUpdate, before.Fields, rec.Fields)
		if e2 != nil {
			return nil, e2
		}
		tx.pendingBefore = nil
		return nil, r.appendEvent(rec.TransactionID, rec, e)
	case KindTruncate:
		tx := r.open[rec.TransactionID]
		if tx == nil {
			return nil, fmt.Errorf("GBase 8s CDC TRUNCATE for transaction %d arrived before BEGIN", rec.TransactionID)
		}
		if tx.pendingBefore != nil {
			return nil, fmt.Errorf("GBase 8s CDC TRUNCATE for transaction %d follows unmatched UPDATE_BEFORE", rec.TransactionID)
		}
		if tx.truncateSeq != 0 {
			return nil, fmt.Errorf("GBase 8s CDC transaction %d contains multiple TRUNCATE records", rec.TransactionID)
		}
		if len(rec.Fields) != 0 {
			return nil, fmt.Errorf("GBase 8s CDC TRUNCATE at sequence %s must not contain row fields", rec.Sequence)
		}
		e, e2 := r.recordEvent(rec, domain.CDCTruncate, nil, nil)
		if e2 != nil {
			return nil, e2
		}
		if err := r.appendEvent(rec.TransactionID, rec, e); err != nil {
			return nil, err
		}
		tx.truncateSeq = seq
		return nil, nil
	case KindCommit:
		tx := r.open[rec.TransactionID]
		if tx == nil {
			return nil, fmt.Errorf("GBase 8s CDC COMMIT references unknown transaction %d", rec.TransactionID)
		}
		delete(r.open, rec.TransactionID)
		if tx.pendingBefore != nil {
			return nil, fmt.Errorf("GBase 8s CDC transaction %d committed with unmatched UPDATE_BEFORE", rec.TransactionID)
		}
		if seq <= r.ackedCommit {
			return nil, nil
		}
		restart := r.restartAt(seq)
		pos := formatPosition(durablePosition{Restart: restart, Commit: seq, CaptureLineage: r.captureLineage})
		if len(tx.events) == 0 {
			ts := rec.SourceTimestampMS
			if ts == 0 {
				ts = tx.lastTimestamp
			}
			e := domain.CDCEvent{ID: "gbase8s-checkpoint:" + sequenceString(seq), Operation: domain.CDCCheckpoint, PositionType: "GBASE8S_CDC_SEQ", PositionValue: pos, Resource: r.resource, SourceTimestampMS: ts}
			return &cdcruntime.Transaction{Events: []domain.CDCEvent{e}, Checkpoint: domain.CDCPosition{DatabaseType: string(domain.DataSourceGBase8s), PositionType: "GBASE8S_CDC_SEQ", PositionValue: pos, Resource: r.resource, SourceTimestampMS: ts}, Label: fmt.Sprintf("GBase8s checkpoint tx=%d commit=%d restart=%d", rec.TransactionID, seq, restart)}, nil
		}
		events := make([]domain.CDCEvent, len(tx.events))
		for i := range tx.events {
			events[i] = tx.events[i].event
			events[i].PositionValue = pos
			if rec.SourceTimestampMS != 0 {
				events[i].SourceTimestampMS = rec.SourceTimestampMS
			}
		}
		ts := rec.SourceTimestampMS
		if ts == 0 {
			ts = tx.lastTimestamp
		}
		return &cdcruntime.Transaction{Events: events, Checkpoint: domain.CDCPosition{DatabaseType: string(domain.DataSourceGBase8s), PositionType: "GBASE8S_CDC_SEQ", PositionValue: pos, Resource: r.resource, SourceTimestampMS: ts}, Label: fmt.Sprintf("GBase8s tx=%d commit=%d restart=%d", rec.TransactionID, seq, restart)}, nil
	default:
		return nil, fmt.Errorf("unsupported GBase 8s CDC record kind %q", rec.Kind)
	}
}

func (r *Reader) refill(ctx context.Context) error {
	start := r.liveCursor
	req := ReadRequest{Database: r.database, StartSequence: sequenceString(start), ExpectedCaptureLineage: r.captureLineage, Tables: r.selections, MaxRecords: r.maxRecords, MaxBytes: r.maxBytes}
	resp, err := r.agent.Read(ctx, req)
	if err != nil {
		return err
	}
	if err := ValidateReadResponse(req, resp); err != nil {
		return err
	}
	if len(resp.Records) > r.maxRecords {
		return fmt.Errorf("GBase 8s CDC provider returned %d records; max_records=%d", len(resp.Records), r.maxRecords)
	}
	totalBytes := 0
	for _, rec := range resp.Records {
		totalBytes += recordEnvelopeBytes(rec)
		if totalBytes > r.maxBytes {
			return fmt.Errorf("GBase 8s CDC provider response exceeds max_bytes=%d", r.maxBytes)
		}
	}
	if strings.TrimSpace(resp.NextSequence) == "" && len(resp.Records) > 0 {
		return errors.New("GBase 8s CDC provider returned records without next_sequence")
	}
	if strings.TrimSpace(resp.NextSequence) != "" {
		n, e := parseSequence(resp.NextSequence)
		if e != nil {
			return fmt.Errorf("invalid GBase 8s CDC provider next_sequence: %w", e)
		}
		if n < start {
			return fmt.Errorf("GBase 8s CDC provider next_sequence moved backwards: %d < %d", n, start)
		}
		r.liveCursor = n
	}
	r.queue = append(r.queue[:0], resp.Records...)
	return nil
}
func (r *Reader) Next(ctx context.Context) (*cdcruntime.Transaction, error) {
	for {
		if len(r.queue) == 0 {
			if err := r.refill(ctx); err != nil {
				return nil, err
			}
			if len(r.queue) == 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(r.poll):
				}
				continue
			}
		}
		rec := r.queue[0]
		r.queue = r.queue[1:]
		tx, err := r.handle(rec)
		if err != nil {
			return nil, err
		}
		if tx != nil {
			return tx, nil
		}
	}
}
func (r *Reader) Acknowledge(_ context.Context, tx *cdcruntime.Transaction) error {
	if tx == nil {
		return errors.New("cannot acknowledge nil GBase 8s CDC transaction")
	}
	if strings.ToUpper(strings.TrimSpace(tx.Checkpoint.PositionType)) != "GBASE8S_CDC_SEQ" {
		return fmt.Errorf("unexpected GBase 8s checkpoint type %q", tx.Checkpoint.PositionType)
	}
	p, err := parsePosition(tx.Checkpoint.PositionValue)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if p.CaptureLineage != r.captureLineage {
		return fmt.Errorf("GBase 8s CDC acknowledgement capture lineage changed: %s != %s", p.CaptureLineage, r.captureLineage)
	}
	if p.Commit < r.ackedCommit {
		return fmt.Errorf("GBase 8s CDC acknowledgement moved backwards: %d < %d", p.Commit, r.ackedCommit)
	}
	if p.Restart < r.restartSeq {
		return fmt.Errorf("GBase 8s CDC restart watermark moved backwards: %d < %d", p.Restart, r.restartSeq)
	}
	r.ackedCommit = p.Commit
	r.restartSeq = p.Restart
	return nil
}
func (r *Reader) Acknowledged() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return formatPosition(durablePosition{Restart: r.restartSeq, Commit: r.ackedCommit, CaptureLineage: r.captureLineage})
}
func (r *Reader) Close() error { return nil }
