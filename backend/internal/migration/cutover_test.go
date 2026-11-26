package migration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
	"dts/backend/internal/repository/memory"
)

type stepReadFailure struct {
	*memory.Store
	step string
	err  error
}

func (s *stepReadFailure) GetMigration(ctx context.Context, id string) (*domain.MigrationTask, error) {
	m, err := s.Store.GetMigration(ctx, id)
	if err == nil && m.CutoverOperation != nil && m.CutoverOperation.Phase == s.step && s.err != nil {
		fault := s.err
		s.err = nil
		return nil, fault
	}
	return m, err
}

func TestCutoverMetadataReadFailure(t *testing.T) {
	for _, step := range []string{"SYNC_GENERATED_STATE", "FINALIZE_GENERATED_MODES"} {
		t.Run(step, func(t *testing.T) {
			ctx := context.Background()
			svc, store, id := finalizationFixture(t, domain.StatusReadyCutover)
			fault := errors.New("metadata unavailable")
			svc.repo = &stepReadFailure{store, step, fault}
			if err := svc.Cutover(ctx, id); !errors.Is(err, fault) {
				t.Fatalf("lost metadata read failure: %v", err)
			}
			task, err := store.GetMigration(ctx, id)
			if err != nil || task.Status != domain.StatusFailed || !strings.Contains(task.LastError, fault.Error()) {
				t.Fatalf("finalization status: %+v, %v", task, err)
			}
			steps := task.CutoverOperation.Steps
			if last := steps[len(steps)-1]; last.Name != step || last.Status != "FAILED" {
				t.Fatalf("missing failed step: %+v", steps)
			}
			svc.failTask(id, errors.New("delayed duplicate"))
			alerts, err := store.ListAlerts(ctx)
			if err != nil || len(alerts) != 1 {
				t.Fatalf("failure alert count %d: %v", len(alerts), err)
			}
		})
	}
}

func TestLateFailureAfterRollbackPreservesOutcome(t *testing.T) {
	svc, store, id := finalizationFixture(t, domain.StatusRolledBack)
	svc.failTask(id, errors.New("delayed worker failure"))
	task, err := store.GetMigration(context.Background(), id)
	if err != nil || task.Status != domain.StatusRolledBack {
		t.Fatalf("rollback outcome overwritten: %+v %v", task, err)
	}
}

func finalizationFixture(t *testing.T, status domain.MigrationStatus) (*Service, *memory.Store, string) {
	t.Helper()
	ctx := context.Background()
	repo := memory.New()
	reg := connector.NewRegistry()
	reg.Register(domain.DataSourceMySQL, fakeFactory{})
	for _, id := range []string{"source", "target"} {
		if err := repo.CreateDataSource(ctx, &domain.DataSource{ID: id, Type: domain.DataSourceMySQL}); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.CreateMigration(ctx, &domain.MigrationTask{ID: "task", SourceID: "source", TargetID: "target", Mode: domain.ModeFullAndIncremental, Status: status}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(repo, reg)
	if status == domain.StatusReadyCutover || status == domain.StatusRollbackReady {
		direction := "forward"
		if status == domain.StatusRollbackReady {
			direction = "reverse"
		}
		seedTestFreeze(t, svc, repo, "task", direction)
	}
	return svc, repo, "task"
}

func seedTestFreeze(t *testing.T, svc *Service, repo *memory.Store, id, direction string) {
	t.Helper()
	ctx := context.Background()
	task, err := repo.GetMigration(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := svc.finalSourcePosition(ctx, task, direction)
	if err != nil {
		t.Fatal(err)
	}
	p.ID = "initial-" + direction
	p.TaskID = id
	p.Direction = direction
	p.RecordedAt = time.Now()
	if err := repo.CreateCDCPosition(ctx, &p); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ConfirmWriteFreeze(ctx, id, direction, "test-admin", domain.WriteFreezeRequest{WritesFrozen: true, Evidence: "test business write freeze", IdempotencyKey: "test-freeze-" + direction}); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackDrainsFinalReverseCheckpointBeforeSuccess(t *testing.T) {
	ctx := context.Background()
	svc, repo, id := finalizationFixture(t, domain.StatusRollbackReady)
	if _, err := svc.stageCDCEvents(ctx, id, "reverse", []domain.CDCEvent{{ID: "final", Operation: domain.CDCCheckpoint, PositionType: "BINLOG", PositionValue: "mysql-bin.000001:200"}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateEngineJob(ctx, &domain.EngineJob{ID: "reader", TaskID: id, Kind: "CDC", Direction: "reverse", Status: domain.EngineJobPending}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Rollback(ctx, id); err != nil {
		t.Fatal(err)
	}
	stats, err := repo.CDCSpoolStats(ctx, id, "reverse")
	if err != nil || stats.PendingTransactions != 0 {
		t.Fatalf("rollback succeeded with undrained events: %+v %v", stats, err)
	}
	latest, err := svc.latestCDC(ctx, id, "reverse")
	if err != nil || latest == nil || latest.PositionValue != "mysql-bin.000001:200" {
		t.Fatalf("final reverse checkpoint missing: %+v %v", latest, err)
	}
	job, _ := repo.GetEngineJob(ctx, "reader")
	task, _ := repo.GetMigration(ctx, id)
	if job.Status != domain.EngineJobStopped || task.Status != domain.StatusRolledBack {
		t.Fatalf("job=%s task=%s", job.Status, task.Status)
	}
}

func TestFinalizationRejectsFailedReadersAndUnresolvedDeadLetters(t *testing.T) {
	for _, direction := range []string{"forward", "reverse"} {
		for _, blocker := range []string{"failed-reader", "OPEN", "COMMIT_UNCERTAIN", "REPLAY_REQUIRED"} {
			t.Run(direction+"/"+blocker, func(t *testing.T) {
				ctx := context.Background()
				status := domain.StatusReadyCutover
				if direction == "reverse" {
					status = domain.StatusRollbackReady
				}
				svc, repo, id := finalizationFixture(t, status)
				if blocker == "failed-reader" {
					if err := repo.CreateEngineJob(ctx, &domain.EngineJob{ID: "failed", TaskID: id, Kind: "CDC", Direction: direction, Status: domain.EngineJobFailed, LastError: "flush failed"}); err != nil {
						t.Fatal(err)
					}
				} else if err := repo.CreateCDCDeadLetter(ctx, &domain.CDCDeadLetter{ID: "gap", TaskID: id, Direction: direction, Status: domain.CDCDeadLetterStatus(blocker)}); err != nil {
					t.Fatal(err)
				}
				var err error
				if direction == "forward" {
					err = svc.Cutover(ctx, id)
				} else {
					err = svc.Rollback(ctx, id)
				}
				if err == nil {
					t.Fatal("finalization accepted unresolved CDC work")
				}
				task, _ := repo.GetMigration(ctx, id)
				if task.Status != status {
					t.Fatalf("must remain retryable, got %s: %v", task.Status, err)
				}
			})
		}
	}
}

func TestRollbackWaitsForReaderAndHonorsRequestCancellation(t *testing.T) {
	svc, repo, id := finalizationFixture(t, domain.StatusRollbackReady)
	if err := repo.CreateEngineJob(context.Background(), &domain.EngineJob{ID: "active", TaskID: id, Kind: "CDC", Direction: "reverse", Status: domain.EngineJobRunning}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := svc.Rollback(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("must wait for reader before returning: %v", err)
	}
	task, _ := repo.GetMigration(context.Background(), id)
	job, _ := repo.GetEngineJob(context.Background(), "active")
	if task.Status != domain.StatusRollbackReady || job.Status != domain.EngineJobStopRequested {
		t.Fatalf("task=%s job=%s", task.Status, job.Status)
	}
}

func TestFinalizationLeaseExcludesOtherControlOperations(t *testing.T) {
	ctx := context.Background()
	svc, repo, id := finalizationFixture(t, domain.StatusReadyCutover)
	ok, err := repo.AcquireControlOperation(ctx, id, "validation", "other-server", time.Minute)
	if err != nil || !ok {
		t.Fatal(err)
	}
	for _, operation := range []func(context.Context, string) error{svc.Cutover, svc.Rollback, svc.PrepareRollback} {
		if err := operation(ctx, id); err == nil || !strings.Contains(err.Error(), "another control operation") {
			t.Fatalf("must reject conflicting control operation: %v", err)
		}
	}
	task, _ := repo.GetMigration(ctx, id)
	if task.Status != domain.StatusReadyCutover {
		t.Fatal(task.Status)
	}
}

func TestRecoverInterruptedFinalizationRespectsLeaseAndFailsClosed(t *testing.T) {
	for _, status := range []domain.MigrationStatus{domain.StatusCutoverRunning, domain.StatusRollbackRunning} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			svc, repo, id := finalizationFixture(t, status)
			ok, err := repo.AcquireControlOperation(ctx, id, "finalize", "owner", time.Minute)
			if err != nil || !ok {
				t.Fatal(err)
			}
			if err := svc.RecoverInterrupted(ctx); err != nil {
				t.Fatal(err)
			}
			task, _ := repo.GetMigration(ctx, id)
			if task.Status != status {
				t.Fatal("live owner was interrupted")
			}
			if err := repo.ReleaseControlOperation(ctx, id, "finalize", "owner"); err != nil {
				t.Fatal(err)
			}
			if err := svc.RecoverInterrupted(ctx); err != nil {
				t.Fatal(err)
			}
			task, _ = repo.GetMigration(ctx, id)
			if task.Status != domain.StatusFailed || !strings.Contains(task.LastError, "manual recovery") {
				t.Fatalf("interrupted finalization must fail closed: %+v", task)
			}
		})
	}
}

func TestManagedCDCRejectsTerminalTaskWithoutAcknowledging(t *testing.T) {
	for _, status := range []domain.MigrationStatus{domain.StatusCutoverRunning, domain.StatusFinished, domain.StatusRollbackRunning, domain.StatusRolledBack, domain.StatusFailed, domain.StatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			svc, repo, id := finalizationFixture(t, status)
			if err := repo.CreateEngineJob(ctx, &domain.EngineJob{ID: "late", TaskID: id, Kind: "CDC", Direction: "forward", Status: domain.EngineJobRunning, WorkerID: "worker"}); err != nil {
				t.Fatal(err)
			}
			_, err := svc.ApplyEngineJobCDCEvents(ctx, "worker", "late", domain.CDCApplyRequest{Events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "BINLOG", PositionValue: "mysql-bin.000001:300"}}})
			if err == nil {
				t.Fatal("late CDC must not be acknowledged or spooled for a terminal task")
			}
			stats, _ := repo.CDCSpoolStats(ctx, id, "forward")
			if stats.PendingTransactions != 0 {
				t.Fatal("late CDC was retained as undrainable work")
			}
		})
	}
}

type failFinalizationTablesRepo struct{ repository.Repository }

func (r failFinalizationTablesRepo) ListMigrationTables(context.Context, string) ([]domain.MigrationTable, error) {
	return nil, errors.New("metadata read failed")
}

func TestCutoverMetadataFailureDoesNotStayRunning(t *testing.T) {
	svc, base, id := finalizationFixture(t, domain.StatusReadyCutover)
	svc.repo = failFinalizationTablesRepo{Repository: base}
	if err := svc.Cutover(context.Background(), id); err == nil {
		t.Fatal("expected metadata error")
	}
	task, _ := base.GetMigration(context.Background(), id)
	if task.Status != domain.StatusFailed || !strings.Contains(task.LastError, "metadata read failed") {
		t.Fatalf("failed cutover must have actionable state: %+v", task)
	}
}

type advanceOnAcquireRepo struct{ *memory.Store }

func (r advanceOnAcquireRepo) AcquireControlOperation(ctx context.Context, id, operation, owner string, ttl time.Duration) (bool, error) {
	task, err := r.Store.GetMigration(ctx, id)
	if err != nil {
		return false, err
	}
	task.Status = domain.StatusFullMigrating
	if err := r.Store.UpdateMigration(ctx, task); err != nil {
		return false, err
	}
	return r.Store.AcquireControlOperation(ctx, id, operation, owner, ttl)
}

func TestStartRereadsStateAfterLeaseAcquisition(t *testing.T) {
	svc, base, id := finalizationFixture(t, domain.StatusCreated)
	svc.repo = advanceOnAcquireRepo{Store: base}
	if err := svc.Start(context.Background(), id); err == nil || !strings.Contains(err.Error(), "FULL_MIGRATING") {
		t.Fatalf("stale start must be rejected after acquiring lease: %v", err)
	}
	task, _ := base.GetMigration(context.Background(), id)
	if task.Status != domain.StatusFullMigrating {
		t.Fatalf("start overwrote newer task state: %s", task.Status)
	}
}

type trackingCheckpointFactory struct{ drops *[]string }
type trackingCheckpointConnector struct {
	fakeConnector
	drops *[]string
}

func (f trackingCheckpointFactory) New(ds domain.DataSource) (connector.Connector, error) {
	return trackingCheckpointConnector{fakeConnector: fakeConnector{typ: ds.Type}, drops: f.drops}, nil
}

func (c trackingCheckpointConnector) DropCDCCheckpoint(_ context.Context, resource string) error {
	*c.drops = append(*c.drops, resource)
	return nil
}

func TestPausePreservesReplicationSlotUntilSuccessfulCutover(t *testing.T) {
	ctx := context.Background()
	svc, repo, id := finalizationFixture(t, domain.StatusCDCCatchingUp)
	var drops []string
	svc.connectors.Register(domain.DataSourceMySQL, trackingCheckpointFactory{drops: &drops})
	task, _ := repo.GetMigration(ctx, id)
	task.CDCStartResource = "retained-slot"
	if err := repo.UpdateMigration(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateEngineJob(ctx, &domain.EngineJob{ID: "reader", TaskID: id, Kind: "CDC", Direction: "forward", Status: domain.EngineJobRunning, WorkerID: "worker"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Pause(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := svc.CompleteEngineJob(ctx, "worker", "reader", domain.EngineJobResult{}); err != nil {
		t.Fatal(err)
	}
	if len(drops) != 0 {
		t.Fatal("pausing CDC dropped the source replication slot")
	}
	if err := svc.Resume(ctx, id); err != nil {
		t.Fatal(err)
	}
	task, _ = repo.GetMigration(ctx, id)
	if task.Status != domain.StatusCDCCatchingUp || task.CDCStartResource != "retained-slot" {
		t.Fatalf("resume did not preserve checkpoint: %+v", task)
	}
	task.Status = domain.StatusReadyCutover
	if err := repo.UpdateMigration(ctx, task); err != nil {
		t.Fatal(err)
	}
	seedTestFreeze(t, svc, repo, id, "forward")
	if err := svc.Cutover(ctx, id); err != nil {
		t.Fatal(err)
	}
	if len(drops) != 1 || drops[0] != "retained-slot" {
		t.Fatalf("successful cutover must clean up source retention: %v", drops)
	}
}

func TestStoppedReaderWithErrorDoesNotReportSuccess(t *testing.T) {
	ctx := context.Background()
	svc, repo, id := finalizationFixture(t, domain.StatusReadyCutover)
	if err := repo.CreateEngineJob(ctx, &domain.EngineJob{ID: "reader", TaskID: id, Kind: "CDC", Direction: "forward", Status: domain.EngineJobStopRequested, WorkerID: "worker"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.CompleteEngineJob(ctx, "worker", "reader", domain.EngineJobResult{Error: "flush failed"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Cutover(ctx, id); err == nil || !strings.Contains(err.Error(), "flush failed") {
		t.Fatalf("reader stop error must block cutover: %v", err)
	}
}

func TestPausedFullAndValidationContinueCaptureWithoutApply(t *testing.T) {
	for _, pausedFrom := range []domain.MigrationStatus{domain.StatusFullMigrating, domain.StatusValidating} {
		t.Run(string(pausedFrom), func(t *testing.T) {
			ctx := context.Background()
			svc, repo, id := finalizationFixture(t, domain.StatusPaused)
			task, _ := repo.GetMigration(ctx, id)
			task.PausedFromStatus = pausedFrom
			if err := repo.UpdateMigration(ctx, task); err != nil {
				t.Fatal(err)
			}
			if err := repo.CreateEngineJob(ctx, &domain.EngineJob{ID: "reader", TaskID: id, Kind: "CDC", Direction: "forward", Status: domain.EngineJobRunning, WorkerID: "worker"}); err != nil {
				t.Fatal(err)
			}
			result, err := svc.ApplyEngineJobCDCEvents(ctx, "worker", "reader", domain.CDCApplyRequest{Events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "BINLOG", PositionValue: "mysql-bin.000001:400"}}})
			if err != nil || !result.Staged || result.Applied != 0 {
				t.Fatalf("paused full+CDC must spool without target apply: %+v %v", result, err)
			}
		})
	}
}
