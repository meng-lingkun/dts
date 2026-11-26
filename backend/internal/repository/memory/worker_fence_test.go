package memory

import (
	"context"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestSameWorkerIDCannotReportFromOldGeneration(t *testing.T) {
	ctx := context.Background()
	s := New()
	_ = s.CreateMigration(ctx, &domain.MigrationTask{ID: "t", Status: domain.StatusFullMigrating})
	_ = s.CreateChunks(ctx, []domain.MigrationChunk{{ID: "c", TaskID: "t", Status: domain.ChunkPending}})
	first, err := s.ClaimChunk(ctx, "same-worker", time.Minute, []string{"dts"})
	if err != nil {
		t.Fatal(err)
	}
	oldCtx := repository.WithLeaseGeneration(ctx, first.LeaseGeneration)
	expired := *first
	expired.LeaseUntil = time.Now().Add(-time.Second)
	if err := s.UpdateChunk(ctx, &expired); err != nil {
		t.Fatal(err)
	}
	if err := s.RenewChunkLease(oldCtx, "c", "same-worker", time.Minute); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("expired renewal revived: %v", err)
	}
	second, err := s.ClaimChunk(ctx, "same-worker", time.Minute, []string{"dts"})
	if err != nil {
		t.Fatal(err)
	}
	if second.LeaseGeneration <= first.LeaseGeneration {
		t.Fatal("generation did not advance")
	}
	first.Status = domain.ChunkSuccess
	if err := s.UpdateOwnedChunk(oldCtx, "same-worker", first); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("old completion accepted: %v", err)
	}
	if err := s.UpdateChunkProgress(oldCtx, "c", "same-worker", domain.ChunkProgress{RowsWritten: 999}); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("old progress accepted: %v", err)
	}
	second.Status = domain.ChunkSuccess
	if err := s.UpdateOwnedChunk(repository.WithLeaseGeneration(ctx, second.LeaseGeneration), "same-worker", second); err != nil {
		t.Fatal(err)
	}
	actual, _ := s.GetChunk(ctx, "c")
	if actual.RowsWritten == 999 || actual.Status != domain.ChunkSuccess {
		t.Fatal("stale result corrupted successor")
	}
}

func TestCDCEpochPersistsAndRejectsLateMetadata(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metadata.json")
	s, err := NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.AcquireCDCSpoolDrainLease(ctx, "t", "forward", "old", time.Minute); err != nil || !ok {
		t.Fatal(err)
	}
	epoch, err := s.CDCExecutionEpoch(ctx, "t", "forward", "old")
	if err != nil {
		t.Fatal(err)
	}
	stale := repository.WithCDCFence(ctx, repository.CDCFence{TaskID: "t", Direction: "forward", Owner: "old", Epoch: epoch})
	if err := s.ReleaseCDCSpoolDrainLease(ctx, "t", "forward", "old"); err != nil {
		t.Fatal(err)
	}
	s, err = NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.AcquireCDCSpoolDrainLease(ctx, "t", "forward", "new", time.Minute); err != nil || !ok {
		t.Fatal(err)
	}
	newer, _ := s.CDCExecutionEpoch(ctx, "t", "forward", "new")
	if newer <= epoch {
		t.Fatal("epoch reset on restart")
	}
	if err := s.CreateCDCPosition(stale, &domain.CDCPosition{ID: "old", TaskID: "t", Direction: "forward"}); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("stale checkpoint accepted: %v", err)
	}
	if err := s.CreateCDCSpool(stale, &domain.CDCSpoolRecord{ID: "old", TaskID: "t", Direction: "forward"}); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("stale spool accepted: %v", err)
	}
	live := repository.WithCDCFence(ctx, repository.CDCFence{TaskID: "t", Direction: "forward", Owner: "new", Epoch: newer})
	if err := s.CreateCDCPosition(live, &domain.CDCPosition{ID: "new", TaskID: "t", Direction: "forward"}); err != nil {
		t.Fatal(err)
	}
}
