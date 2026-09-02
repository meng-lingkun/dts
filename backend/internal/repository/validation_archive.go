package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"sort"
	"strconv"
	"strings"
	"time"

	"qmigration/backend/internal/domain"
)

var ErrValidationArchiveNotTerminal = errors.New("validation archive requires terminal migration state")
var ErrNoValidationEvidence = errors.New("no validation evidence available to archive")

// ValidationEvidenceRow is one successful full-load chunk plus its latest
// validation attempt. An empty ValidationID means the chunk has no durable
// validation evidence yet.
type ValidationEvidenceRow struct {
	ChunkID        string
	ChunkNo        int
	SplitType      string
	ValidationID   string
	Status         domain.ValidationStatus
	SourceRows     int64
	TargetRows     int64
	SourceChecksum string
	TargetChecksum string
	LastError      string
	StartedAt      time.Time
	FinishedAt     time.Time
}

// ValidationArchiveProvider is optional so older/external repositories remain
// source compatible. Production wrappers must forward it to the metadata store.
type ValidationArchiveProvider interface {
	GetValidationArchive(context.Context, string) (*domain.ValidationArchive, error)
	CreateValidationArchive(context.Context, *domain.ValidationArchive) (bool, error)
	ListValidationEvidencePage(context.Context, string, string, int, string, int) ([]ValidationEvidenceRow, error)
	LatestValidationStatusCounts(context.Context, string) (success, mismatch, validationError, missing int, err error)
}

func terminalValidationArchiveStatus(s domain.MigrationStatus) bool {
	switch s {
	case domain.StatusFinished, domain.StatusFailed, domain.StatusCancelled, domain.StatusRolledBack:
		return true
	default:
		return false
	}
}

func isComplexArchiveSplit(split string) bool {
	s := strings.ToUpper(strings.TrimSpace(split))
	return s != "PRIMARY_KEY_RANGE" && s != "PK_RANGE" && s != "PK_RANGE_ADAPTIVE"
}

func digestField(h hash.Hash, value string) {
	_, _ = fmt.Fprintf(h, "%d:", len(value))
	_, _ = h.Write([]byte(value))
	_, _ = h.Write([]byte{'|'})
}

func digestInt(h hash.Hash, value int64) { digestField(h, strconv.FormatInt(value, 10)) }
func digestTime(h hash.Hash, value time.Time) {
	if value.IsZero() {
		digestField(h, "")
		return
	}
	digestField(h, value.UTC().Format(time.RFC3339Nano))
}
func digestHex(h hash.Hash) string { return hex.EncodeToString(h.Sum(nil)) }

func evidencePage(ctx context.Context, repo Repository, taskID, tableID string, afterNo int, afterID string, limit int) ([]ValidationEvidenceRow, error) {
	if p, ok := repo.(ValidationArchiveProvider); ok {
		return p.ListValidationEvidencePage(ctx, taskID, tableID, afterNo, afterID, limit)
	}
	chunks, err := ListTableChunksPage(ctx, repo, taskID, tableID, afterNo, afterID, limit)
	if err != nil {
		return nil, err
	}
	results, err := repo.ListValidationResults(ctx, taskID)
	if err != nil {
		return nil, err
	}
	latest := map[string]domain.ValidationResult{}
	for _, v := range results {
		old, ok := latest[v.ChunkID]
		if !ok || v.FinishedAt.After(old.FinishedAt) || (v.FinishedAt.Equal(old.FinishedAt) && v.ID > old.ID) {
			latest[v.ChunkID] = v
		}
	}
	out := make([]ValidationEvidenceRow, 0, len(chunks))
	for _, ch := range chunks {
		if ch.Status != domain.ChunkSuccess {
			continue
		}
		r := ValidationEvidenceRow{ChunkID: ch.ID, ChunkNo: ch.ChunkNo, SplitType: ch.SplitType}
		if v, ok := latest[ch.ID]; ok {
			r.ValidationID, r.Status = v.ID, v.Status
			r.SourceRows, r.TargetRows = v.SourceRows, v.TargetRows
			r.SourceChecksum, r.TargetChecksum = v.SourceChecksum, v.TargetChecksum
			r.LastError, r.StartedAt, r.FinishedAt = v.LastError, v.StartedAt, v.FinishedAt
		}
		out = append(out, r)
	}
	return out, nil
}

func buildTableArchive(ctx context.Context, repo Repository, taskID string, table domain.MigrationTable, pageSize int) (domain.ValidationTableArchive, error) {
	out := domain.ValidationTableArchive{TableID: table.ID, SourceSchema: table.SourceSchema, SourceTable: table.SourceTable, TargetSchema: table.TargetSchema, TargetTable: table.TargetTable, EvidenceScope: "CHUNK_SET", ChecksumKind: "CHUNK_SET_SHA256"}
	evidenceHash, sourceHash, targetHash := sha256.New(), sha256.New(), sha256.New()
	afterNo, afterID := -1, ""
	complex := false
	unionInitialized, unionConsistent := false, true
	var unionSourceRows, unionTargetRows int64
	var unionSourceChecksum, unionTargetChecksum string
	for {
		page, err := evidencePage(ctx, repo, taskID, table.ID, afterNo, afterID, pageSize)
		if err != nil {
			return out, err
		}
		if len(page) == 0 {
			break
		}
		for _, row := range page {
			out.TotalChunks++
			if isComplexArchiveSplit(row.SplitType) {
				complex = true
			}
			digestField(evidenceHash, row.ChunkID)
			digestInt(evidenceHash, int64(row.ChunkNo))
			digestField(evidenceHash, row.SplitType)
			digestField(evidenceHash, row.ValidationID)
			digestField(evidenceHash, string(row.Status))
			digestInt(evidenceHash, row.SourceRows)
			digestInt(evidenceHash, row.TargetRows)
			digestField(evidenceHash, row.SourceChecksum)
			digestField(evidenceHash, row.TargetChecksum)
			digestField(evidenceHash, row.LastError)
			digestTime(evidenceHash, row.StartedAt)
			digestTime(evidenceHash, row.FinishedAt)
			if row.ValidationID == "" {
				out.MissingChunks++
				continue
			}
			out.CoveredChunks++
			switch row.Status {
			case domain.ValidationSuccess:
				out.SuccessChunks++
			case domain.ValidationMismatch:
				out.MismatchChunks++
			case domain.ValidationError:
				out.ErrorChunks++
			}
			if out.FirstStartedAt.IsZero() || (!row.StartedAt.IsZero() && row.StartedAt.Before(out.FirstStartedAt)) {
				out.FirstStartedAt = row.StartedAt
			}
			if row.FinishedAt.After(out.LastFinishedAt) {
				out.LastFinishedAt = row.FinishedAt
			}
			digestField(sourceHash, row.ChunkID)
			digestInt(sourceHash, row.SourceRows)
			digestField(sourceHash, row.SourceChecksum)
			digestField(targetHash, row.ChunkID)
			digestInt(targetHash, row.TargetRows)
			digestField(targetHash, row.TargetChecksum)
			if complex {
				if !unionInitialized {
					unionInitialized = true
					unionSourceRows, unionTargetRows = row.SourceRows, row.TargetRows
					unionSourceChecksum, unionTargetChecksum = row.SourceChecksum, row.TargetChecksum
				} else if unionSourceRows != row.SourceRows || unionTargetRows != row.TargetRows || unionSourceChecksum != row.SourceChecksum || unionTargetChecksum != row.TargetChecksum {
					unionConsistent = false
				}
			} else {
				out.SourceRows += row.SourceRows
				out.TargetRows += row.TargetRows
			}
		}
		last := page[len(page)-1]
		afterNo, afterID = last.ChunkNo, last.ChunkID
		if len(page) < pageSize {
			break
		}
	}
	out.EvidenceDigest = digestHex(evidenceHash)
	out.SourceChecksumDigest = digestHex(sourceHash)
	out.TargetChecksumDigest = digestHex(targetHash)
	if complex {
		if unionInitialized && unionConsistent {
			out.EvidenceScope = "TABLE_UNION"
			out.ChecksumKind = "TABLE_UNION"
			out.SourceRows, out.TargetRows = unionSourceRows, unionTargetRows
			out.SourceChecksum, out.TargetChecksum = unionSourceChecksum, unionTargetChecksum
		} else {
			out.EvidenceScope = "MIXED"
			out.ChecksumKind = "EVIDENCE_SHA256"
		}
	}
	return out, nil
}

func buildLegacyValidationArchive(ctx context.Context, repo Repository, taskID string, task *domain.MigrationTask) (*domain.ValidationArchive, error) {
	items, err := repo.ListValidationResults(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, ErrNoValidationEvidence
	}
	latest := map[string]domain.ValidationResult{}
	for _, v := range items {
		old, ok := latest[v.ChunkID]
		if !ok || v.FinishedAt.After(old.FinishedAt) || (v.FinishedAt.Equal(old.FinishedAt) && v.ID > old.ID) {
			latest[v.ChunkID] = v
		}
	}
	tableMeta, _ := repo.ListMigrationTables(ctx, taskID)
	meta := map[string]domain.MigrationTable{}
	for _, t := range tableMeta {
		meta[t.ID] = t
	}
	groups := map[string][]domain.ValidationResult{}
	for _, v := range latest {
		groups[v.TableID] = append(groups[v.TableID], v)
	}
	tableIDs := make([]string, 0, len(groups))
	for id := range groups {
		tableIDs = append(tableIDs, id)
	}
	sort.Strings(tableIDs)
	a := &domain.ValidationArchive{
		TaskID: taskID, TerminalStatus: task.Status, ValidationMode: task.ValidationMode,
		ValidationBarrierPositionType: task.ValidationBarrierPositionType, ValidationBarrierPosition: task.ValidationBarrierPositionValue,
		ValidationBarrierResource: task.ValidationBarrierResource, ArchivedAt: time.Now().UTC(),
	}
	taskHash := sha256.New()
	digestField(taskHash, taskID)
	digestField(taskHash, string(task.Status))
	digestField(taskHash, "LEGACY_RESULT_SET")
	for _, tableID := range tableIDs {
		rows := groups[tableID]
		sort.Slice(rows, func(i, j int) bool { return rows[i].ChunkID < rows[j].ChunkID })
		m := meta[tableID]
		t := domain.ValidationTableArchive{TableID: tableID, SourceSchema: m.SourceSchema, SourceTable: m.SourceTable, TargetSchema: m.TargetSchema, TargetTable: m.TargetTable, EvidenceScope: "LEGACY_RESULT_SET", ChecksumKind: "EVIDENCE_SHA256"}
		h, sh, th := sha256.New(), sha256.New(), sha256.New()
		for _, v := range rows {
			t.TotalChunks++
			t.CoveredChunks++
			switch v.Status {
			case domain.ValidationSuccess:
				t.SuccessChunks++
			case domain.ValidationMismatch:
				t.MismatchChunks++
			case domain.ValidationError:
				t.ErrorChunks++
			}
			t.SourceRows += v.SourceRows
			t.TargetRows += v.TargetRows
			if t.FirstStartedAt.IsZero() || (!v.StartedAt.IsZero() && v.StartedAt.Before(t.FirstStartedAt)) {
				t.FirstStartedAt = v.StartedAt
			}
			if v.FinishedAt.After(t.LastFinishedAt) {
				t.LastFinishedAt = v.FinishedAt
			}
			digestField(h, v.ChunkID)
			digestField(h, v.ID)
			digestField(h, string(v.Status))
			digestInt(h, v.SourceRows)
			digestInt(h, v.TargetRows)
			digestField(h, v.SourceChecksum)
			digestField(h, v.TargetChecksum)
			digestField(h, v.LastError)
			digestTime(h, v.StartedAt)
			digestTime(h, v.FinishedAt)
			digestField(sh, v.ChunkID)
			digestInt(sh, v.SourceRows)
			digestField(sh, v.SourceChecksum)
			digestField(th, v.ChunkID)
			digestInt(th, v.TargetRows)
			digestField(th, v.TargetChecksum)
		}
		t.EvidenceDigest, t.SourceChecksumDigest, t.TargetChecksumDigest = digestHex(h), digestHex(sh), digestHex(th)
		a.Tables = append(a.Tables, t)
		a.TotalTables++
		a.TotalChunks += t.TotalChunks
		a.CoveredChunks += t.CoveredChunks
		a.SuccessChunks += t.SuccessChunks
		a.MismatchChunks += t.MismatchChunks
		a.ErrorChunks += t.ErrorChunks
		digestField(taskHash, tableID)
		digestField(taskHash, t.EvidenceDigest)
	}
	a.EvidenceDigest = digestHex(taskHash)
	return a, nil
}

// EnsureValidationArchive creates exactly one immutable archive. Concurrent
// creators are safe: CreateValidationArchive must use insert-if-absent semantics.
func EnsureValidationArchive(ctx context.Context, repo Repository, taskID string, pageSize int) (*domain.ValidationArchive, bool, error) {
	p, ok := repo.(ValidationArchiveProvider)
	if !ok {
		return nil, false, nil
	}
	if existing, err := p.GetValidationArchive(ctx, taskID); err != nil {
		return nil, false, err
	} else if existing != nil {
		return existing, false, nil
	}
	task, err := repo.GetMigration(ctx, taskID)
	if err != nil {
		return nil, false, err
	}
	if !terminalValidationArchiveStatus(task.Status) {
		return nil, false, ErrValidationArchiveNotTerminal
	}
	if pageSize <= 0 || pageSize > 5000 {
		pageSize = 512
	}
	tables, err := repo.ListMigrationTables(ctx, taskID)
	if err != nil {
		return nil, false, err
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].ID < tables[j].ID })
	archive := &domain.ValidationArchive{
		TaskID: taskID, TerminalStatus: task.Status, ValidationMode: task.ValidationMode,
		ValidationBarrierPositionType: task.ValidationBarrierPositionType,
		ValidationBarrierPosition:     task.ValidationBarrierPositionValue,
		ValidationBarrierResource:     task.ValidationBarrierResource,
		ArchivedAt:                    time.Now().UTC(),
	}
	taskHash := sha256.New()
	digestField(taskHash, taskID)
	digestField(taskHash, string(task.Status))
	digestField(taskHash, task.ValidationMode)
	digestField(taskHash, task.ValidationBarrierPositionType)
	digestField(taskHash, task.ValidationBarrierPositionValue)
	digestField(taskHash, task.ValidationBarrierResource)
	for _, table := range tables {
		t, err := buildTableArchive(ctx, repo, taskID, table, pageSize)
		if err != nil {
			return nil, false, fmt.Errorf("archive validation table %s: %w", table.ID, err)
		}
		if t.TotalChunks == 0 {
			continue
		}
		archive.Tables = append(archive.Tables, t)
		archive.TotalTables++
		archive.TotalChunks += t.TotalChunks
		archive.CoveredChunks += t.CoveredChunks
		archive.SuccessChunks += t.SuccessChunks
		archive.MismatchChunks += t.MismatchChunks
		archive.ErrorChunks += t.ErrorChunks
		archive.MissingChunks += t.MissingChunks
		digestField(taskHash, t.TableID)
		digestField(taskHash, t.EvidenceDigest)
	}
	if archive.CoveredChunks == 0 {
		legacy, legacyErr := buildLegacyValidationArchive(ctx, repo, taskID, task)
		if legacyErr != nil {
			return nil, false, legacyErr
		}
		archive = legacy
	} else {
		archive.EvidenceDigest = digestHex(taskHash)
	}
	created, err := p.CreateValidationArchive(ctx, archive)
	if err != nil {
		return nil, false, err
	}
	if !created {
		existing, getErr := p.GetValidationArchive(ctx, taskID)
		return existing, false, getErr
	}
	return archive, true, nil
}

func LatestValidationMismatchCount(ctx context.Context, repo Repository, taskID string) (int, error) {
	if p, ok := repo.(ValidationArchiveProvider); ok {
		if a, err := p.GetValidationArchive(ctx, taskID); err != nil {
			return 0, err
		} else if a != nil {
			return a.MismatchChunks + a.ErrorChunks, nil
		}
		success, mismatch, validationError, _, err := p.LatestValidationStatusCounts(ctx, taskID)
		_ = success
		if err != nil {
			return 0, err
		}
		return mismatch + validationError, nil
	}
	items, err := repo.ListValidationResults(ctx, taskID)
	if err != nil {
		return 0, err
	}
	latest := map[string]domain.ValidationResult{}
	for _, v := range items {
		old, ok := latest[v.ChunkID]
		if !ok || v.FinishedAt.After(old.FinishedAt) || (v.FinishedAt.Equal(old.FinishedAt) && v.ID > old.ID) {
			latest[v.ChunkID] = v
		}
	}
	count := 0
	for _, v := range latest {
		if v.Status == domain.ValidationMismatch || v.Status == domain.ValidationError {
			count++
		}
	}
	return count, nil
}
