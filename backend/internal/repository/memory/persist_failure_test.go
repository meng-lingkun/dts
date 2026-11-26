package memory

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

func TestRepositorySnapshotsIsolateNestedData(t *testing.T) {
	ctx := context.Background()
	s := New()
	task := &domain.MigrationTask{ID: "t", Tables: []domain.TableMapping{{Partitions: []string{"p0"}}}, TransformRules: []domain.TransformRule{{Value: "source"}}}
	if err := s.CreateMigration(ctx, task); err != nil {
		t.Fatal(err)
	}
	task.Tables[0].Partitions[0] = "caller"
	read, _ := s.GetMigration(ctx, "t")
	read.TransformRules[0].Value = "uncommitted"
	check, _ := s.GetMigration(ctx, "t")
	if check.Tables[0].Partitions[0] != "p0" || check.TransformRules[0].Value != "source" {
		t.Fatalf("nested fields changed without a revision: %+v", check)
	}
	record := &domain.CDCSpoolRecord{TaskID: "t", ID: "r", Direction: "forward", Status: domain.CDCSpoolPending, Events: []domain.CDCEvent{{After: []domain.CDCField{{Column: "a", Value: "original"}}}}}
	if err := s.CreateCDCSpool(ctx, record); err != nil {
		t.Fatal(err)
	}
	record.Events[0].After[0].Value = "changed"
	pending, _ := s.LatestPendingCDCSpool(ctx, "t", "forward")
	if pending.Events[0].After[0].Value != "original" {
		t.Fatalf("stored transaction changed: %+v", pending.Events)
	}
}

func TestCDCWriteFailureDoesNotAcknowledgeOrAdvance(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metadata.json")
	s, err := NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	// The snapshot cannot be renamed over a directory, even as root.
	unwritable := t.TempDir()
	s.path = unwritable
	record := domain.CDCSpoolRecord{ID: "tx", TaskID: "t", Direction: "forward", Status: domain.CDCSpoolPending}
	position := domain.CDCPosition{ID: "p", TaskID: "t", Direction: "forward"}
	for i := 0; i < 2; i++ {
		if err := s.CreateCDCSpool(ctx, &record); err == nil || record.Sequence != 0 {
			t.Fatalf("failed append acknowledged: sequence %d, %v", record.Sequence, err)
		}
	}
	if err := s.CreateCDCPosition(ctx, &position); err == nil {
		t.Fatal("unexpected checkpoint success")
	}
	positions, err := s.ListCDCPositions(ctx, "t", 2)
	if err != nil || len(positions) != 0 {
		t.Fatalf("checkpoint visible after write failure: %+v %v", positions, err)
	}
	s.path = path
	if err := s.CreateCDCSpool(ctx, &record); err != nil || record.Sequence != 1 {
		t.Fatalf("retry: %+v, %v", record, err)
	}
	s.path = unwritable
	if err := s.MarkCDCSpoolApplied(ctx, "tx", time.Now()); err == nil {
		t.Fatal("unexpected apply success")
	}
	recovered, err := NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, store := range []*Store{s, recovered} {
		row, err := store.LatestPendingCDCSpool(ctx, "t", "forward")
		if err != nil || row == nil || row.ID != "tx" || row.Sequence != 1 {
			t.Fatalf("pending transaction lost: %+v %v", row, err)
		}
	}
}

func TestFailedClaimRestoresExpiredLeases(t *testing.T) {
	ctx := context.Background()
	s := New()
	expired := time.Now().Add(-time.Minute)
	s.migrations["t"] = domain.MigrationTask{ID: "t", Status: domain.StatusFullMigrating}
	s.chunks["c"] = domain.MigrationChunk{ID: "c", TaskID: "t", Status: domain.ChunkRunning, WorkerID: "first", LeaseGeneration: 7, LeaseUntil: expired}
	s.engineJobs["j"] = domain.EngineJob{ID: "j", TaskID: "t", Engine: "dts", Status: domain.EngineJobRunning, WorkerID: "first", LeaseGeneration: 7, LeaseUntil: expired}
	s.path = t.TempDir()
	// Both the no-candidate sweep and a successful candidate selection must
	// propagate the snapshot failure and leave the old generation intact.
	for _, caps := range [][]string{nil, {"dts"}} {
		chunk, err := s.ClaimChunk(ctx, "second", time.Minute, caps)
		if chunk != nil || err == nil || errors.Is(err, repository.ErrNoChunk) {
			t.Fatalf("unpersisted chunk claim: %+v %v", chunk, err)
		}
		job, err := s.ClaimEngineJob(ctx, "second", time.Minute, caps)
		if job != nil || err == nil || errors.Is(err, repository.ErrNoChunk) {
			t.Fatalf("unpersisted job claim: %+v %v", job, err)
		}
		old, _ := s.GetChunk(ctx, "c")
		reader, _ := s.GetEngineJob(ctx, "j")
		if old.WorkerID != "first" || reader.WorkerID != "first" || old.LeaseGeneration != 7 || reader.LeaseGeneration != 7 || old.RetryCount != 0 || reader.RetryCount != 0 {
			t.Fatalf("failed takeover became visible: %+v %+v", old, reader)
		}
	}
	s.path = filepath.Join(t.TempDir(), "metadata.json")
	chunk, err := s.ClaimChunk(ctx, "second", time.Minute, []string{"dts"})
	if err != nil || chunk.LeaseGeneration != 8 || chunk.RetryCount != 1 {
		t.Fatalf("retry did not grant exactly one lease: %+v %v", chunk, err)
	}
}

func TestFailedRetentionPreservesEvidence(t *testing.T) {
	ctx := context.Background()
	s := New()
	old := time.Now().Add(-24 * time.Hour)
	s.audits = []domain.AuditEvent{{ID: "e", CreatedAt: old}}
	s.taskLogs = []domain.TaskLog{{ID: "l", CreatedAt: old}}
	s.validations["v"] = domain.ValidationResult{ID: "v", TaskID: "one", ChunkID: "c", FinishedAt: old}
	s.validations["w"] = domain.ValidationResult{ID: "w", TaskID: "one", ChunkID: "c", FinishedAt: time.Now()}
	if err := s.DeleteValidationResults(ctx, ""); err != nil || len(s.validations) != 2 {
		t.Fatalf("empty task selector erased evidence: %v", err)
	}
	s.path = t.TempDir()
	result, err := s.PruneMetadata(ctx, repository.MetadataRetentionPolicy{AuditMaxAge: time.Hour, TaskLogMaxAge: time.Hour, ValidationMaxAttemptsPerChunk: 1})
	if err == nil || result.TotalDeleted() != 0 || len(s.audits) != 1 || len(s.taskLogs) != 1 || len(s.validations) != 2 {
		t.Fatalf("failed prune lost evidence: %+v %v", result, err)
	}
}

func TestTiedValidationAttemptsAgreeOnCutoverAndRepair(t *testing.T) {
	ctx := context.Background()
	s := New()
	s.chunks["c"] = domain.MigrationChunk{ID: "c", TaskID: "t", Status: domain.ChunkSuccess}
	at := time.Now()
	s.validations["a"] = domain.ValidationResult{ID: "a", TaskID: "t", ChunkID: "c", Status: domain.ValidationSuccess, FinishedAt: at}
	s.validations["b"] = domain.ValidationResult{ID: "b", TaskID: "t", ChunkID: "c", Status: domain.ValidationMismatch, FinishedAt: at}
	for i := 0; i < 32; i++ {
		id, status, err := s.FirstInvalidSuccessfulChunk(ctx, "t")
		if err != nil || id != "c" || status != domain.ValidationMismatch {
			t.Fatalf("cutover decision: %q %s %v", id, status, err)
		}
		ids, err := s.ListRepairableValidationChunkIDs(ctx, "t", 1)
		if err != nil || len(ids) != 1 || ids[0] != "c" {
			t.Fatalf("repair candidates: %+v %v", ids, err)
		}
	}
}
