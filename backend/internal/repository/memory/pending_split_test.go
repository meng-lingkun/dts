package memory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

func pendingRangePlan(t *testing.T, s *Store) (domain.MigrationChunk, []domain.MigrationChunk) {
	t.Helper()
	c := domain.MigrationChunk{ID: "head", TaskID: "task", TableID: "table", ChunkNo: 1, SplitType: "PK_RANGE", PrimaryKey: "id", Start: 1, End: 10, Status: domain.ChunkPending, CustomWhere: "active = 1"}
	if err := s.CreateChunks(context.Background(), []domain.MigrationChunk{c}); err != nil {
		t.Fatal(err)
	}
	first, second := c, c
	first.SplitType, first.End = "PK_RANGE_ADAPTIVE", 5
	second.ID, second.ChunkNo, second.Start, second.SplitType = "tail", 2, 6, first.SplitType
	return c, []domain.MigrationChunk{first, second}
}

func TestPendingSplitConcurrentAttemptsCommitOnePartition(t *testing.T) {
	ctx := context.Background()
	s := New()
	expected, pieces := pendingRangePlan(t, s)
	start, result := make(chan struct{}), make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { <-start; result <- s.SplitPendingChunk(ctx, expected, pieces) }()
	}
	close(start)
	won, stale := 0, 0
	for i := 0; i < 2; i++ {
		switch err := <-result; {
		case err == nil:
			won++
		case errors.Is(err, repository.ErrConflict):
			stale++
		default:
			t.Fatal(err)
		}
	}
	got, err := s.ListChunks(ctx, "task")
	if err != nil || won != 1 || stale != 1 || len(got) != 2 || got[0].Start != 1 || got[0].End != 5 || got[1].Start != 6 || got[1].End != 10 || got[1].CustomWhere != expected.CustomWhere {
		t.Fatalf("won=%d stale=%d partition=%+v err=%v", won, stale, got, err)
	}
	if err := s.UpdateChunk(ctx, &expected); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("stale writer could restore the original range: %v", err)
	}
}

func TestPendingSplitLeavesRangeIntactOnFailure(t *testing.T) {
	for _, cause := range []string{"collision", "persist", "claimed", "checkpoint", "changed upper bound", "gap", "source filter"} {
		t.Run(cause, func(t *testing.T) {
			ctx := context.Background()
			s := New()
			expected, pieces := pendingRangePlan(t, s)
			current := expected
			count := 1
			switch cause {
			case "collision":
				other := pieces[1]
				other.ID = "existing-with-same-number"
				if err := s.CreateChunks(ctx, []domain.MigrationChunk{other}); err != nil {
					t.Fatal(err)
				}
				count++
			case "persist":
				parent := filepath.Join(t.TempDir(), "regular-file")
				if err := os.WriteFile(parent, []byte("x"), 0600); err != nil {
					t.Fatal(err)
				}
				s.path = filepath.Join(parent, "state.json")
			case "claimed":
				current.Status, current.WorkerID = domain.ChunkRunning, "worker"
			case "checkpoint":
				current.RowsRead = 4
			case "changed upper bound":
				current.End = 18
			case "gap":
				pieces[1].Start++
			case "source filter":
				pieces[1].CustomWhere = ""
			}
			if cause == "claimed" || cause == "checkpoint" || cause == "changed upper bound" {
				if err := s.UpdateChunk(ctx, &current); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.SplitPendingChunk(ctx, expected, pieces); err == nil {
				t.Fatal("split unexpectedly committed")
			}
			got, err := s.ListChunks(ctx, "task")
			if err != nil || len(got) != count || got[0].End != current.End || got[0].Status != current.Status || got[0].RowsRead != current.RowsRead || got[0].LeaseGeneration != current.LeaseGeneration {
				t.Fatalf("state changed: %+v %v", got, err)
			}
			if _, err := s.GetChunk(ctx, "tail"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("tail exists: %v", err)
			}
		})
	}
}
