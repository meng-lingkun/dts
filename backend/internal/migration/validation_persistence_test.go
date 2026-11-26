package migration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
	"dts/backend/internal/repository/memory"
)

type validationEmptyFactory struct{}

func (validationEmptyFactory) New(domain.DataSource) (connector.Connector, error) {
	return rc44EmptyDataConnector{}, nil
}

// Hooks run synchronously at the metadata boundary of an otherwise real validation.
type validationHookStore struct {
	*memory.Store
	beforeGet    func() error
	beforeDelete func() error
	beforeResult func(*domain.ValidationResult) error
	beforeUpdate func() error
	archives     int
	released     chan struct{}
}

func (r *validationHookStore) GetMigration(ctx context.Context, id string) (*domain.MigrationTask, error) {
	if r.beforeGet != nil {
		if err := r.beforeGet(); err != nil {
			return nil, err
		}
	}
	return r.Store.GetMigration(ctx, id)
}

func (r *validationHookStore) DeleteValidationResults(ctx context.Context, id string) error {
	if r.beforeDelete != nil {
		if err := r.beforeDelete(); err != nil {
			return err
		}
	}
	return r.Store.DeleteValidationResults(ctx, id)
}

func (r *validationHookStore) CreateValidationResult(ctx context.Context, result *domain.ValidationResult) error {
	if r.beforeResult != nil {
		if err := r.beforeResult(result); err != nil {
			return err
		}
	}
	return r.Store.CreateValidationResult(ctx, result)
}

func (r *validationHookStore) UpdateMigration(ctx context.Context, task *domain.MigrationTask) error {
	if r.beforeUpdate != nil {
		if err := r.beforeUpdate(); err != nil {
			return err
		}
	}
	return r.Store.UpdateMigration(ctx, task)
}

func (r *validationHookStore) GetValidationArchive(ctx context.Context, id string) (*domain.ValidationArchive, error) {
	r.archives++
	return r.Store.GetValidationArchive(ctx, id)
}

func (r *validationHookStore) ReleaseControlOperation(ctx context.Context, id, operation, owner string) error {
	err := r.Store.ReleaseControlOperation(ctx, id, operation, owner)
	if r.released != nil && operation == controlOperationValidation {
		close(r.released)
	}
	return err
}

func validationRunFixture(t *testing.T, split string, planned, actual int) (*Service, *validationHookStore) {
	t.Helper()
	ctx := context.Background()
	r := &validationHookStore{Store: memory.New()}
	for _, id := range []string{"src", "dst"} {
		if err := r.CreateDataSource(ctx, &domain.DataSource{ID: id, Name: id, Type: domain.DataSourceMySQL}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.CreateMigration(ctx, &domain.MigrationTask{ID: "task", SourceID: "src", TargetID: "dst",
		Status: domain.StatusValidating, Mode: domain.ModeFull, TotalChunks: planned, BatchRows: 50}); err != nil {
		t.Fatal(err)
	}
	if err := r.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "table", TaskID: "task", PrimaryKey: "id",
		TargetPrimaryKey: "id", MinPK: 1, MaxPK: int64(actual), TotalChunks: planned, Columns: []domain.ColumnInfo{{Name: "id", DataType: "int"}}}); err != nil {
		t.Fatal(err)
	}
	var chunks []domain.MigrationChunk
	for i := 0; i < actual; i++ {
		chunks = append(chunks, domain.MigrationChunk{ID: fmt.Sprintf("chunk-%d", i), TaskID: "task", TableID: "table",
			ChunkNo: i, SplitType: split, PrimaryKey: "id", Start: int64(i + 1), End: int64(i + 1), Status: domain.ChunkSuccess})
	}
	if err := r.CreateChunks(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, validationEmptyFactory{})
	return NewService(r, reg), r
}

func expectValidationState(t *testing.T, r *validationHookStore, status domain.MigrationStatus, results int) {
	t.Helper()
	ctx := context.Background()
	task, err := r.Store.GetMigration(ctx, "task")
	if err != nil || task.Status != status {
		t.Fatalf("task = %+v, err = %v, want %s", task, err, status)
	}
	records, err := r.ListValidationResults(ctx, "task")
	if err != nil || len(records) != results {
		t.Fatalf("records = %+v, err = %v, want %d", records, err, results)
	}
	if status != domain.StatusFinished && r.archives != 0 {
		t.Fatalf("validation tried to archive before completion")
	}
}

func TestValidationRebuildsPartialEvidenceAfterWriteFailure(t *testing.T) {
	ctx := context.Background()
	for _, split := range []string{"PRIMARY_KEY_RANGE", "HASH"} {
		t.Run(split, func(t *testing.T) {
			s, r := validationRunFixture(t, split, 2, 2)
			fault := errors.New("metadata store unavailable")
			r.beforeResult = func(v *domain.ValidationResult) error {
				if v.ChunkID == "chunk-1" {
					return fault
				}
				return nil
			}
			if err := s.validateTask(ctx, "task"); !errors.Is(err, fault) {
				t.Fatalf("got %v, want persistence error", err)
			}
			expectValidationState(t, r, domain.StatusValidating, 1)
			r.beforeResult = nil
			r.released = make(chan struct{})
			if err := s.RecoverInterrupted(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-r.released:
			case <-time.After(5 * time.Second):
				t.Fatal("validation recovery did not finish")
			}
			expectValidationState(t, r, domain.StatusFinished, 2)
			records, err := r.ListValidationResults(ctx, "task")
			if err != nil {
				t.Fatal(err)
			}
			success := make(map[string]bool)
			for _, record := range records {
				success[record.ChunkID] = record.Status == domain.ValidationSuccess
			}
			if !success["chunk-0"] || !success["chunk-1"] {
				t.Fatalf("expected successful evidence for each chunk: %+v", records)
			}
		})
	}
}

func TestValidationMetadataErrorsBeforeCompletion(t *testing.T) {
	ctx := context.Background()
	fault := errors.New("metadata write or read failed")
	for _, phase := range []string{"reset", "final-read", "commit", "discard-drift"} {
		t.Run(phase, func(t *testing.T) {
			s, r := validationRunFixture(t, "PRIMARY_KEY_RANGE", 1, 1)
			var count int
			switch phase {
			case "reset":
				r.beforeDelete = func() error { return fault }
			case "final-read":
				r.beforeResult = func(*domain.ValidationResult) error { r.beforeGet = func() error { return fault }; return nil }
			case "commit":
				r.beforeUpdate = func() error { return fault }
			case "discard-drift":
				task, err := r.GetMigration(ctx, "task")
				if err != nil {
					t.Fatal(err)
				}
				task.Mode = domain.ModeFullAndIncremental
				task.CDCStartPositionValue = "start"
				if err := r.UpdateMigration(ctx, task); err != nil {
					t.Fatal(err)
				}
				r.beforeDelete = func() error {
					count++
					if count == 2 {
						return fault
					}
					return nil
				}
			}
			if err := s.validateTask(ctx, "task"); !errors.Is(err, fault) {
				t.Fatalf("got %v, want metadata error", err)
			}
			n := 1
			if phase == "reset" {
				n = 0
			}
			expectValidationState(t, r, domain.StatusValidating, n)
		})
	}
}

func TestValidationRespectsConcurrentPauseAndCancel(t *testing.T) {
	ctx := context.Background()
	for _, status := range []domain.MigrationStatus{domain.StatusPaused, domain.StatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			s, r := validationRunFixture(t, "PRIMARY_KEY_RANGE", 1, 1)
			change := func() error {
				current, err := r.Store.GetMigration(ctx, "task")
				if err != nil {
					return err
				}
				current.Status = status
				return r.Store.UpdateMigration(ctx, current)
			}
			if status == domain.StatusPaused {
				r.beforeResult = func(*domain.ValidationResult) error { return change() }
			} else {
				r.beforeUpdate = change
			}
			err := s.validateTask(ctx, "task")
			if status == domain.StatusCancelled && !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("expected CAS conflict, got %v", err)
			}
			if status == domain.StatusPaused && err != nil {
				t.Fatal(err)
			}
			expectValidationState(t, r, status, 1)
		})
	}
}

func TestValidationHonorsPlannedChunkCoverage(t *testing.T) {
	for _, tc := range []struct {
		planned, actual int
		status          domain.MigrationStatus
	}{
		{0, 0, domain.StatusFinished}, {1, 0, domain.StatusFailed}, {2, 1, domain.StatusFailed},
	} {
		t.Run(fmt.Sprintf("%d-of-%d", tc.actual, tc.planned), func(t *testing.T) {
			s, r := validationRunFixture(t, "PRIMARY_KEY_RANGE", tc.planned, tc.actual)
			if err := s.validateTask(context.Background(), "task"); err != nil {
				t.Fatal(err)
			}
			expectValidationState(t, r, tc.status, tc.actual)
		})
	}
}

func TestCompletedEmptyValidationDoesNotRestartOnEveryDrain(t *testing.T) {
	ctx := context.Background()
	s, r := validationRunFixture(t, "PRIMARY_KEY_RANGE", 0, 0)
	task, err := r.GetMigration(ctx, "task")
	if err != nil {
		t.Fatal(err)
	}
	task.Mode = domain.ModeFullAndIncremental
	task.Status = domain.StatusCDCCatchingUp
	task.ValidationEnabled = true
	task.CDCStartPositionType = "binlog"
	task.CDCStartPositionValue = "file:4"
	task.ValidationBarrierCapturedAt = time.Now()
	if err := r.UpdateMigration(ctx, task); err != nil {
		t.Fatal(err)
	}
	if s.maybeStartValidationAfterCatchup(ctx, task.ID) {
		t.Fatal("already completed empty plan started another validation")
	}
	expectValidationState(t, r, domain.StatusCDCCatchingUp, 0)
}

func TestCutoverValidationRequiresAllPlannedChunks(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name                       string
		planned, actual            int
		pending, evidence, allowed bool
	}{
		{"missing", 1, 0, false, false, false},
		{"pending", 1, 1, true, false, false},
		{"missing-evidence", 1, 1, false, false, false},
		{"complete", 1, 1, false, true, true},
		{"empty", 0, 0, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r := validationRunFixture(t, "PRIMARY_KEY_RANGE", tc.planned, tc.actual)
			task, err := r.GetMigration(ctx, "task")
			if err != nil {
				t.Fatal(err)
			}
			if tc.pending {
				chunk, err := r.GetChunk(ctx, "chunk-0")
				if err != nil {
					t.Fatal(err)
				}
				chunk.Status = domain.ChunkPending
				if err := r.UpdateChunk(ctx, chunk); err != nil {
					t.Fatal(err)
				}
			}
			if tc.evidence {
				if err := r.CreateValidationResult(ctx, &domain.ValidationResult{ID: "r", TaskID: "task", ChunkID: "chunk-0", Status: domain.ValidationSuccess}); err != nil {
					t.Fatal(err)
				}
			}
			err = s.requireSuccessfulValidation(ctx, task)
			if (err == nil) != tc.allowed {
				t.Fatalf("allowed = %v, got %v", tc.allowed, err)
			}
		})
	}
}
