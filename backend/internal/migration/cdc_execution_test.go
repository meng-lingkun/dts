package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
	"dts/backend/internal/repository/memory"
)

func cdcExecutionFixture(t *testing.T) (*memory.Store, *connector.Registry, *cdcApplyState, domain.CDCApplyRequest) {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	state := &cdcApplyState{}
	registry := connector.NewRegistry()
	registry.Register(domain.DataSourceMySQL, cdcApplyFactory{state: state})
	for _, id := range []string{"src", "dst"} {
		if err := store.CreateDataSource(ctx, &domain.DataSource{ID: id, Type: domain.DataSourceMySQL, Database: "app"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.CreateMigration(ctx, &domain.MigrationTask{ID: "task", SourceID: "src", TargetID: "dst", Mode: domain.ModeIncremental, Status: domain.StatusCDCCatchingUp}); err != nil {
		t.Fatal(err)
	}
	columns := []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}
	if err := store.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "table", TaskID: "task", SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", TargetPrimaryKey: "id", Columns: columns, TargetColumns: columns}); err != nil {
		t.Fatal(err)
	}
	request := domain.CDCApplyRequest{Events: []domain.CDCEvent{{ID: "event", Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: "1"}}, PositionType: "BINLOG", PositionValue: "mysql-bin.000001:100", Resource: "stream-a"}}}
	return store, registry, state, request
}

type checkpointFailureRepo struct {
	*memory.Store
	checkpointErr, spoolErr error
}

func (r *checkpointFailureRepo) LatestCDCPosition(ctx context.Context, taskID, direction string) (*domain.CDCPosition, error) {
	if r.checkpointErr != nil {
		return nil, r.checkpointErr
	}
	return r.Store.LatestCDCPosition(ctx, taskID, direction)
}

func (r *checkpointFailureRepo) LatestPendingCDCSpool(ctx context.Context, taskID, direction string) (*domain.CDCSpoolRecord, error) {
	if r.spoolErr != nil {
		return nil, r.spoolErr
	}
	return r.Store.LatestPendingCDCSpool(ctx, taskID, direction)
}

func TestCDCCheckpointReadFailureCannotWriteOrSilentlyRestart(t *testing.T) {
	ctx := context.Background()
	store, registry, state, req := cdcExecutionFixture(t)
	r := &checkpointFailureRepo{Store: store, checkpointErr: errors.New("metadata unavailable")}
	svc := NewService(r, registry)
	if _, err := svc.ApplyCDCEvents(ctx, "task", req); err == nil || !strings.Contains(err.Error(), "read durable CDC checkpoint") {
		t.Fatalf("checkpoint failure must fail closed: %v", err)
	}
	if state.writes != 0 || state.begins != 0 {
		t.Fatalf("target touched without checkpoint knowledge: %+v", state)
	}
	task, _ := store.GetMigration(ctx, "task")
	if _, _, _, _, err := svc.cdcRenderTask(ctx, task, "forward"); err == nil || !strings.Contains(err.Error(), "resume checkpoint") {
		t.Fatalf("restart silently fell back to initial checkpoint: %v", err)
	}
	r.checkpointErr = nil
	r.spoolErr = errors.New("spool metadata unavailable")
	if _, _, _, _, err := svc.cdcRenderTask(ctx, task, "forward"); err == nil || !strings.Contains(err.Error(), "resume spool") {
		t.Fatalf("restart ignored durable spool read failure: %v", err)
	}
}

func TestCDCDedupUsesDirectionAndResourceWithoutHistoryLimit(t *testing.T) {
	for _, direction := range []string{"forward", ""} {
		t.Run("persisted-direction="+direction, func(t *testing.T) {
			ctx := context.Background()
			store, registry, state, req := cdcExecutionFixture(t)
			last := req.Events[0]
			if err := store.CreateCDCPosition(ctx, &domain.CDCPosition{ID: "forward-head", TaskID: "task", Direction: direction, PositionType: last.PositionType, PositionValue: last.PositionValue, Resource: last.Resource}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 250; i++ {
				if err := store.CreateCDCPosition(ctx, &domain.CDCPosition{ID: fmt.Sprint(i), TaskID: "task", Direction: "reverse", PositionValue: fmt.Sprint(i)}); err != nil {
					t.Fatal(err)
				}
			}
			svc := NewService(store, registry)
			result, err := svc.ApplyCDCEvents(ctx, "task", req)
			if err != nil || !result.Duplicate || state.writes != 0 {
				t.Fatalf("forward head hidden by reverse history: result=%+v state=%+v err=%v", result, state, err)
			}
			req.Events[0].Resource = "stream-b"
			result, err = svc.ApplyCDCEvents(ctx, "task", req)
			if err != nil || result.Duplicate || state.writes != 1 {
				t.Fatalf("different stream mistaken for duplicate: result=%+v state=%+v err=%v", result, state, err)
			}
		})
	}
}

type blockingCheckpointRepo struct {
	*memory.Store
	once            sync.Once
	entered, resume chan struct{}
}

func (r *blockingCheckpointRepo) LatestCDCPosition(ctx context.Context, taskID, direction string) (*domain.CDCPosition, error) {
	r.once.Do(func() {
		close(r.entered)
		select {
		case <-r.resume:
		case <-ctx.Done():
		}
	})
	return r.Store.LatestCDCPosition(ctx, taskID, direction)
}

func TestCDCExecutionLeaseCoversEveryWriterAcrossServers(t *testing.T) {
	ctx := context.Background()
	store, registry, state, req := cdcExecutionFixture(t)
	r := &blockingCheckpointRepo{Store: store, entered: make(chan struct{}), resume: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(r.resume) })
	defer unblock()
	serverA, serverB := NewService(r, registry), NewService(r, registry)
	if err := store.CreateEngineJob(ctx, &domain.EngineJob{ID: "job", TaskID: "task", Direction: "forward", WorkerID: "worker", Status: domain.EngineJobRunning}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateCDCDeadLetter(ctx, &domain.CDCDeadLetter{ID: "dlq", TaskID: "task", Direction: "forward", PositionType: req.Events[0].PositionType, PositionValue: req.Events[0].PositionValue, Resource: req.Events[0].Resource, Status: domain.CDCDeadLetterOpen, Events: req.Events}); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { _, err := serverA.ApplyCDCEvents(ctx, "task", req); finished <- err }()
	select {
	case <-r.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first server did not enter target execution")
	}
	checks := map[string]func() error{
		"direct":     func() error { _, err := serverB.ApplyCDCEvents(ctx, "task", req); return err },
		"managed":    func() error { _, err := serverB.ApplyEngineJobCDCEvents(ctx, "worker", "job", req); return err },
		"replay":     func() error { _, err := serverB.ReplayCDCDeadLetter(ctx, "task", "dlq"); return err },
		"resolve":    func() error { _, err := serverB.ResolveCDCCommitUncertain(ctx, "task", "dlq", "COMMITTED"); return err },
		"progress":   func() error { return serverB.RecordCDCProgress(ctx, "task", &domain.CDCPosition{}) },
		"validation": func() error { return serverB.ValidateNow(ctx, "task") },
		"ready":      func() error { return serverB.ReadyForCutover(ctx, "task", 5000) },
	}
	for name, check := range checks {
		if err := check(); !errors.Is(err, ErrCDCExecutionBusy) {
			t.Errorf("%s bypassed another server's execution lease: %v", name, err)
		}
	}
	unblock()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first execution did not finish")
	}
	result, err := serverB.ApplyCDCEvents(ctx, "task", req)
	if err != nil || !result.Duplicate || state.writes != 1 || state.commits != 1 {
		t.Fatalf("retry after lease release must deduplicate: result=%+v state=%+v err=%v", result, state, err)
	}
}

type lostCDCLeaseRepo struct {
	*memory.Store
	renews atomic.Int32
}

func (r *lostCDCLeaseRepo) RenewCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string, ttl time.Duration) error {
	if r.renews.Add(1) == 2 {
		return repository.ErrLeaseOwner
	}
	return r.Store.RenewCDCSpoolDrainLease(ctx, taskID, direction, owner, ttl)
}

func TestCDCExecutionLeaseLossBeforeCommitDoesNotAdvanceCheckpoint(t *testing.T) {
	ctx := context.Background()
	store, registry, state, req := cdcExecutionFixture(t)
	svc := NewService(&lostCDCLeaseRepo{Store: store}, registry)
	if _, err := svc.ApplyCDCEvents(ctx, "task", req); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("lost lease did not abort execution: %v", err)
	}
	position, _ := store.LatestCDCPosition(ctx, "task", "forward")
	if state.commits != 0 || state.rollbacks != 1 || position != nil {
		t.Fatalf("lost owner committed or acknowledged source: state=%+v position=%+v", state, position)
	}
	items, _ := store.ListCDCDeadLetters(ctx, "task")
	if len(items) != 1 || items[0].Status != domain.CDCDeadLetterCommitUncertain {
		t.Fatalf("lost lease must preserve pre-commit recovery evidence: %+v", items)
	}
}

func TestCDCExecutionLocalWaitHonorsCancellation(t *testing.T) {
	store, registry, _, _ := cdcExecutionFixture(t)
	svc := NewService(store, registry)
	_, release, err := svc.lockCDCExecution(context.Background(), "task", "forward")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := svc.lockCDCExecution(ctx, "task", "forward"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter did not exit: %v", err)
	}
}

func TestDirectCDCBridgeDrainsEarlierTransactionsBeforeNewEvents(t *testing.T) {
	ctx := context.Background()
	store, registry, state, req := cdcExecutionFixture(t)
	svc := NewService(store, registry)
	earlier := req.Events[0]
	earlier.PositionValue = "mysql-bin.000001:90"
	earlier.After = []domain.CDCField{{Column: "id", Value: "0"}}
	if _, err := svc.stageCDCEvents(ctx, "task", "forward", []domain.CDCEvent{earlier}); err != nil {
		t.Fatal(err)
	}
	if result, err := svc.ApplyCDCEvents(ctx, "task", req); err != nil || result.Applied != 1 {
		t.Fatalf("direct bridge failed: %+v %v", result, err)
	}
	stats, _ := store.CDCSpoolStats(ctx, "task", "forward")
	if stats.PendingTransactions != 0 || state.writes != 2 || len(state.lastWrite) != 1 || state.lastWrite[0] != "1" {
		t.Fatalf("earlier spool overtaken: stats=%+v state=%+v", stats, state)
	}
	positions, _ := store.ListCDCPositions(ctx, "task", 10)
	if len(positions) != 2 || positions[0].PositionValue != req.Events[0].PositionValue || positions[1].PositionValue != earlier.PositionValue {
		t.Fatalf("CDC checkpoint order regressed: %+v", positions)
	}
}

func TestFinalizationPreservesFinalDrainedProgress(t *testing.T) {
	for _, direction := range []string{"forward", "reverse"} {
		t.Run(direction, func(t *testing.T) {
			ctx := context.Background()
			status := domain.StatusReadyCutover
			if direction == "reverse" {
				status = domain.StatusRollbackReady
			}
			svc, store, id := finalizationFixture(t, status)
			task, _ := store.GetMigration(ctx, id)
			task.CDCLagMS = 999999
			if err := store.UpdateMigration(ctx, task); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.stageCDCEvents(ctx, id, direction, []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "BINLOG", PositionValue: "mysql-bin.000001:200"}}); err != nil {
				t.Fatal(err)
			}
			var err error
			if direction == "forward" {
				err = svc.Cutover(ctx, id)
			} else {
				err = svc.Rollback(ctx, id)
			}
			if err != nil {
				t.Fatal(err)
			}
			task, _ = store.GetMigration(ctx, id)
			latest, _ := store.LatestCDCPosition(ctx, id, direction)
			if latest == nil || task.CDCLagMS != latest.LagMS {
				t.Fatalf("finalization overwrote final apply with stale task: task=%+v checkpoint=%+v", task, latest)
			}
		})
	}
}

func TestOpenCDCDeadLetterBlocksNewerEventsAndResolvedReplay(t *testing.T) {
	ctx := context.Background()
	store, registry, state, req := cdcExecutionFixture(t)
	svc := NewService(store, registry)
	last := req.Events[0]
	id := cdcDeadLetterID("task", "forward", last)
	now := time.Now()
	if err := store.CreateCDCDeadLetter(ctx, &domain.CDCDeadLetter{ID: id, TaskID: "task", Direction: "forward", PositionType: last.PositionType, PositionValue: last.PositionValue, Resource: last.Resource, Status: domain.CDCDeadLetterOpen, Events: req.Events, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	later := last
	later.PositionValue = "mysql-bin.000001:110"
	laterID := cdcDeadLetterID("task", "forward", later)
	if err := store.CreateCDCDeadLetter(ctx, &domain.CDCDeadLetter{ID: laterID, TaskID: "task", Direction: "forward", PositionType: later.PositionType, PositionValue: later.PositionValue, Resource: later.Resource, Status: domain.CDCDeadLetterOpen, Events: []domain.CDCEvent{later}, CreatedAt: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	newer := domain.CDCApplyRequest{Events: append([]domain.CDCEvent(nil), req.Events...)}
	newer.Events[0].PositionValue = "mysql-bin.000001:120"
	if _, err := svc.ApplyCDCEvents(ctx, "task", newer); err == nil || !strings.Contains(err.Error(), "earlier unresolved DLQ") || state.writes != 0 {
		t.Fatalf("newer event overtook unresolved transaction: state=%+v err=%v", state, err)
	}
	if _, err := svc.ReplayCDCDeadLetter(ctx, "task", id); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ReplayCDCDeadLetter(ctx, "task", id); err == nil || !strings.Contains(err.Error(), "already RESOLVED") {
		t.Fatalf("resolved historical transaction could be replayed: %v", err)
	}
	if _, err := svc.ApplyCDCEvents(ctx, "task", newer); err == nil {
		t.Fatal("newer event overtook the second retained gap")
	}
	if _, err := svc.ReplayCDCDeadLetter(ctx, "task", laterID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ApplyCDCEvents(ctx, "task", newer); err != nil || state.writes != 3 {
		t.Fatalf("stream did not recover after closing original gap: state=%+v err=%v", state, err)
	}
}
