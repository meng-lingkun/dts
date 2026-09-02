package repository_test

import (
	"context"
	"testing"
	"time"

	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/repository"
	"qmigration/backend/internal/repository/memory"
)

func seedArchiveTask(t *testing.T, status domain.MigrationStatus, split string, chunks int) (*memory.Store, string, string) {
	t.Helper()
	ctx := context.Background()
	s := memory.New()
	taskID, tableID := "archive-task", "archive-table"
	task := &domain.MigrationTask{ID: taskID, Name: "archive", Status: status, ValidationMode: "CHUNK_CHECKSUM", UpdatedAt: time.Now().Add(-2 * time.Hour)}
	if err := s.CreateMigration(ctx, task); err != nil {
		t.Fatal(err)
	}
	tbl := &domain.MigrationTable{ID: tableID, TaskID: taskID, SourceSchema: "s", SourceTable: "t", TargetSchema: "d", TargetTable: "t", PrimaryKey: "id", Status: "FINISHED"}
	if err := s.CreateMigrationTable(ctx, tbl); err != nil {
		t.Fatal(err)
	}
	items := make([]domain.MigrationChunk, 0, chunks)
	for i := 0; i < chunks; i++ {
		items = append(items, domain.MigrationChunk{ID: "chunk-" + string(rune('a'+i)), TaskID: taskID, TableID: tableID, ChunkNo: i, SplitType: split, Status: domain.ChunkSuccess})
	}
	if err := s.CreateChunks(ctx, items); err != nil {
		t.Fatal(err)
	}
	return s, taskID, tableID
}

func TestRC45ValidationArchiveRangeIsImmutableAndSumsRows(t *testing.T) {
	ctx := context.Background()
	s, taskID, tableID := seedArchiveTask(t, domain.StatusFinished, "PRIMARY_KEY_RANGE", 2)
	now := time.Now()
	for _, v := range []domain.ValidationResult{
		{ID: "v1", TaskID: taskID, TableID: tableID, ChunkID: "chunk-a", Status: domain.ValidationSuccess, SourceRows: 10, TargetRows: 10, SourceChecksum: "aa", TargetChecksum: "aa", StartedAt: now.Add(-time.Minute), FinishedAt: now},
		{ID: "v2", TaskID: taskID, TableID: tableID, ChunkID: "chunk-b", Status: domain.ValidationMismatch, SourceRows: 20, TargetRows: 20, SourceChecksum: "bb", TargetChecksum: "cc", StartedAt: now.Add(-time.Minute), FinishedAt: now},
	} {
		vv := v
		if err := s.CreateValidationResult(ctx, &vv); err != nil {
			t.Fatal(err)
		}
	}
	a, created, err := repository.EnsureValidationArchive(ctx, s, taskID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("expected archive creation")
	}
	if a.TotalChunks != 2 || a.CoveredChunks != 2 || a.SuccessChunks != 1 || a.MismatchChunks != 1 {
		t.Fatalf("unexpected task archive: %+v", a)
	}
	if len(a.Tables) != 1 {
		t.Fatalf("expected one table archive, got %d", len(a.Tables))
	}
	tbl := a.Tables[0]
	if tbl.EvidenceScope != "CHUNK_SET" || tbl.ChecksumKind != "CHUNK_SET_SHA256" {
		t.Fatalf("unexpected range scope: %+v", tbl)
	}
	if tbl.SourceRows != 30 || tbl.TargetRows != 30 {
		t.Fatalf("expected exact chunk row sum 30/30, got %d/%d", tbl.SourceRows, tbl.TargetRows)
	}
	if len(a.EvidenceDigest) != 64 || len(tbl.SourceChecksumDigest) != 64 {
		t.Fatalf("expected sha256 digests, got task=%q source=%q", a.EvidenceDigest, tbl.SourceChecksumDigest)
	}
	sealed := a.EvidenceDigest
	newer := domain.ValidationResult{ID: "v3", TaskID: taskID, TableID: tableID, ChunkID: "chunk-b", Status: domain.ValidationSuccess, SourceRows: 20, TargetRows: 20, SourceChecksum: "bb", TargetChecksum: "bb", StartedAt: now, FinishedAt: now.Add(time.Second)}
	if err := s.CreateValidationResult(ctx, &newer); err != nil {
		t.Fatal(err)
	}
	a2, created2, err := repository.EnsureValidationArchive(ctx, s, taskID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if created2 {
		t.Fatal("immutable archive must not be replaced")
	}
	if a2.EvidenceDigest != sealed || a2.MismatchChunks != 1 {
		t.Fatalf("archive changed after later result: %+v", a2)
	}
}

func TestRC45ValidationArchiveTableUnionDoesNotDoubleRows(t *testing.T) {
	ctx := context.Background()
	s, taskID, tableID := seedArchiveTask(t, domain.StatusFinished, "HASH", 2)
	now := time.Now()
	for i, cid := range []string{"chunk-a", "chunk-b"} {
		v := domain.ValidationResult{ID: "union-" + string(rune('a'+i)), TaskID: taskID, TableID: tableID, ChunkID: cid, Status: domain.ValidationSuccess, SourceRows: 100, TargetRows: 100, SourceChecksum: "whole-source", TargetChecksum: "whole-source", StartedAt: now.Add(-time.Minute), FinishedAt: now}
		if err := s.CreateValidationResult(ctx, &v); err != nil {
			t.Fatal(err)
		}
	}
	a, _, err := repository.EnsureValidationArchive(ctx, s, taskID, 1)
	if err != nil {
		t.Fatal(err)
	}
	tbl := a.Tables[0]
	if tbl.EvidenceScope != "TABLE_UNION" || tbl.ChecksumKind != "TABLE_UNION" {
		t.Fatalf("expected table union archive, got %+v", tbl)
	}
	if tbl.SourceRows != 100 || tbl.TargetRows != 100 {
		t.Fatalf("table-union rows must not be multiplied by chunk coverage: %+v", tbl)
	}
	if tbl.SourceChecksum != "whole-source" || tbl.TargetChecksum != "whole-source" {
		t.Fatalf("expected exact table union checksum: %+v", tbl)
	}
}

func TestRC45TerminalRetentionArchivesBeforeDeletingDetail(t *testing.T) {
	ctx := context.Background()
	s, taskID, tableID := seedArchiveTask(t, domain.StatusFinished, "PRIMARY_KEY_RANGE", 1)
	old := time.Now().Add(-3 * time.Hour)
	task, _ := s.GetMigration(ctx, taskID)
	task.UpdatedAt = old
	if err := s.UpdateMigration(ctx, task); err != nil {
		t.Fatal(err)
	}
	v := domain.ValidationResult{ID: "old", TaskID: taskID, TableID: tableID, ChunkID: "chunk-a", Status: domain.ValidationSuccess, SourceRows: 1, TargetRows: 1, SourceChecksum: "x", TargetChecksum: "x", StartedAt: old, FinishedAt: old}
	if err := s.CreateValidationResult(ctx, &v); err != nil {
		t.Fatal(err)
	}
	res, err := s.PruneMetadata(ctx, repository.MetadataRetentionPolicy{ValidationTerminalMaxAge: time.Hour, ValidationArchivePageSize: 1, ValidationArchiveTasksPerRun: 1, BatchRows: 100, MaxBatches: 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.ValidationArchivesCreated != 1 || res.ValidationDeleted != 1 {
		t.Fatalf("expected archive then detail prune, got %+v", res)
	}
	a, err := s.GetValidationArchive(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if a == nil || a.CoveredChunks != 1 {
		t.Fatalf("archive missing after prune: %+v", a)
	}
	items, err := s.ListValidationResults(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("expected detailed results pruned, got %d", len(items))
	}
}
