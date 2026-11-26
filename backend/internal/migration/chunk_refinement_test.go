package migration

import (
	"context"
	"errors"
	"testing"
	"time"

	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
	"dts/backend/internal/repository/memory"
)

type refinementFailureStore struct {
	*memory.Store
	attempts int
}

func (s *refinementFailureStore) SplitPendingChunk(context.Context, domain.MigrationChunk, []domain.MigrationChunk) error {
	s.attempts++
	return errors.New("metadata write failed")
}

func refinementFixture(t *testing.T, s *memory.Store) (domain.MigrationChunk, domain.MigrationChunk) {
	t.Helper()
	ctx := context.Background()
	task := domain.MigrationTask{ID: "task", Mode: domain.ModeFull, FullEngine: "dts", Status: domain.StatusFullMigrating, Parallelism: 2}
	if err := s.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	table := domain.MigrationTable{ID: "table", TaskID: task.ID, PrimaryKey: "id", Engine: "dts", Status: "RUNNING"}
	if err := s.CreateMigrationTable(ctx, &table); err != nil {
		t.Fatal(err)
	}
	running := domain.MigrationChunk{ID: "running", TaskID: task.ID, TableID: table.ID, ChunkNo: 1, SplitType: "PK_RANGE", PrimaryKey: "id", Start: 0, End: 9, WorkerID: "worker", Status: domain.ChunkRunning, LeaseGeneration: 3, LeaseUntil: time.Now().Add(time.Minute)}
	pending := domain.MigrationChunk{ID: "pending", TaskID: task.ID, TableID: table.ID, ChunkNo: 2, SplitType: "PK_RANGE", PrimaryKey: "id", Start: 10, End: 19, Status: domain.ChunkPending}
	if err := s.CreateChunks(ctx, []domain.MigrationChunk{running, pending}); err != nil {
		t.Fatal(err)
	}
	return running, pending
}

func TestCompletedChunkRefreshesProgressAfterRefinementFails(t *testing.T) {
	repo := &refinementFailureStore{Store: memory.New()}
	running, pending := refinementFixture(t, repo.Store)
	svc := NewService(repo, nil)
	ctx := repository.WithLeaseGeneration(context.Background(), running.LeaseGeneration)
	t.Setenv("DTS_ADAPTIVE_CHUNK_TARGET_SECONDS", "1")
	if err := svc.CompleteChunk(ctx, running.WorkerID, running.ID, domain.ChunkResult{DurationMS: 3000, RowsWritten: 5}); err != nil {
		t.Fatal(err)
	}
	task, err := repo.GetMigration(ctx, running.TaskID)
	if err != nil || repo.attempts != 1 || task.TotalChunks != 2 || task.FinishedChunks != 1 || task.RowsMigrated != 5 || task.AdaptiveHotspotSplits != 0 {
		t.Fatalf("attempts=%d task=%+v err=%v", repo.attempts, task, err)
	}
	got, err := repo.GetChunk(ctx, pending.ID)
	if err != nil || got.End != pending.End || got.Start != pending.Start || got.Status != domain.ChunkPending {
		t.Fatalf("pending work changed: %+v %v", got, err)
	}
}

func TestTerminalYieldRequiresExactEndAndCurrentLease(t *testing.T) {
	for _, scenario := range []string{"exact end", "beyond end", "expired before commit"} {
		t.Run(scenario, func(t *testing.T) {
			repo := memory.New()
			running, _ := refinementFixture(t, repo)
			ctx := repository.WithLeaseGeneration(context.Background(), running.LeaseGeneration)
			svc := NewService(repo, nil)
			result := domain.ChunkResult{Yielded: true, CursorJSON: `{"after_pk":9}`}
			if scenario == "beyond end" {
				result.CursorJSON = `{"after_pk":10}`
			}
			if scenario == "expired before commit" {
				expired := running
				expired.LeaseUntil = time.Now().Add(-time.Minute)
				if err := repo.UpdateChunk(ctx, &expired); err != nil {
					t.Fatal(err)
				}
			}
			err := svc.completeYieldedChunk(ctx, running.WorkerID, &running, result)
			stored, readErr := repo.GetChunk(ctx, running.ID)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if scenario == "exact end" {
				if err != nil || stored.Status != domain.ChunkSuccess || stored.LeaseGeneration != running.LeaseGeneration+1 {
					t.Fatalf("completion: %+v %v", stored, err)
				}
			} else if err == nil || stored.Status != domain.ChunkRunning || stored.LeaseGeneration != running.LeaseGeneration || (scenario == "expired before commit" && !errors.Is(err, repository.ErrLeaseOwner)) {
				t.Fatalf("invalid completion: %+v %v", stored, err)
			}
		})
	}
}
