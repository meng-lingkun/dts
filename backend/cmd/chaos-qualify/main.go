package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/faultinject"
	"qmigration/backend/internal/migration"
	"qmigration/backend/internal/repository/memory"
)

const toolVersion = "0.15.0-rc49"

type state struct {
	writes, deletes, begins, commits, rollbacks int
	pendingWrites, pendingDeletes               int
	commitUnknown                               bool // target committed, response lost
	commitUnknownBefore                         bool // response lost/error before target commit
	writeFailOnce                               bool // fail one target write during controlled replay
}
type factory struct{ s *state }
type conn struct{ s *state }

func (f factory) Capabilities(t domain.DataSourceType) connector.Descriptor {
	return connector.Descriptor{Type: t, Protocol: "synthetic-chaos", Native: true, Maturity: connector.MaturityNative, Capabilities: []connector.Capability{connector.CapabilityMetadata, connector.CapabilityFullRead, connector.CapabilityFullWrite, connector.CapabilityCDCApply, connector.CapabilityCDCTransactional}}
}
func (f factory) New(domain.DataSource) (connector.Connector, error)         { return &conn{s: f.s}, nil }
func (*conn) TestConnection(context.Context) error                           { return nil }
func (*conn) GetVersion(context.Context) (string, error)                     { return "synthetic", nil }
func (*conn) ListSchemas(context.Context) ([]domain.SchemaInfo, error)       { return nil, nil }
func (*conn) ListTables(context.Context, string) ([]domain.TableInfo, error) { return nil, nil }
func (*conn) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, errors.New("not used")
}
func (*conn) Close() error { return nil }
func (*conn) ReadBatch(context.Context, connector.ReadBatchRequest) (*connector.RowBatch, error) {
	return &connector.RowBatch{}, nil
}
func (c *conn) WriteBatch(_ context.Context, req connector.WriteBatchRequest) (int64, error) {
	if c.s.writeFailOnce {
		c.s.writeFailOnce = false
		return 0, errors.New("synthetic target write failure before commit")
	}
	c.s.pendingWrites += len(req.Rows)
	return int64(len(req.Rows)), nil
}
func (c *conn) DeleteByKey(context.Context, connector.DeleteByKeyRequest) error {
	c.s.pendingDeletes++
	return nil
}
func (c *conn) BeginCDCTransaction(context.Context) error {
	c.s.begins++
	c.s.pendingWrites, c.s.pendingDeletes = 0, 0
	return nil
}
func (c *conn) CommitCDCTransaction(context.Context) error {
	c.s.commits++
	if c.s.commitUnknownBefore {
		c.s.commitUnknownBefore = false
		c.s.pendingWrites, c.s.pendingDeletes = 0, 0
		return errors.New("synthetic connection lost before target COMMIT took effect")
	}
	c.s.writes += c.s.pendingWrites
	c.s.deletes += c.s.pendingDeletes
	c.s.pendingWrites, c.s.pendingDeletes = 0, 0
	if c.s.commitUnknown {
		c.s.commitUnknown = false
		return errors.New("synthetic connection lost after target COMMIT")
	}
	return nil
}
func (c *conn) RollbackCDCTransaction(context.Context) error {
	c.s.rollbacks++
	c.s.pendingWrites, c.s.pendingDeletes = 0, 0
	return nil
}

type check struct {
	Name, Status, Message string
	Details               map[string]any `json:"details,omitempty"`
}
type report struct {
	ToolVersion string  `json:"tool_version"`
	GeneratedAt string  `json:"generated_at_utc"`
	Checks      []check `json:"checks"`
	Qualified   bool    `json:"qualified"`
}

func setup(id string) (*migration.Service, *memory.Store, *state, domain.MigrationTask, domain.EngineJob) {
	ctx := context.Background()
	repo := memory.New()
	st := &state{}
	reg := connector.NewRegistry()
	f := factory{s: st}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	now := time.Now()
	src := domain.DataSource{ID: id + "-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now}
	dst := domain.DataSource{ID: id + "-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now}
	_ = repo.CreateDataSource(ctx, &src)
	_ = repo.CreateDataSource(ctx, &dst)
	task := domain.MigrationTask{ID: id, SourceID: src.ID, TargetID: dst.ID, Mode: domain.ModeFullAndIncremental, Status: domain.StatusFullMigrating, CreatedAt: now, UpdatedAt: now}
	_ = repo.CreateMigration(ctx, &task)
	_ = repo.CreateMigrationTable(ctx, &domain.MigrationTable{ID: id + "-table", TaskID: id, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}, TargetColumns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}})
	job := domain.EngineJob{ID: id + "-job", TaskID: id, Kind: "CDC", Direction: "forward", Engine: "synthetic", Status: domain.EngineJobRunning, WorkerID: "chaos-worker", UpdatedAt: now}
	_ = repo.CreateEngineJob(ctx, &job)
	return migration.NewService(repo, reg), repo, st, task, job
}

func event(id, pos, val string) domain.CDCApplyRequest {
	return domain.CDCApplyRequest{Events: []domain.CDCEvent{{ID: id, Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "orders", After: []domain.CDCField{{Column: "id", Value: val}}, PositionType: "GTID", PositionValue: pos}}}
}

func runCheck(name string, fn func() (map[string]any, error)) check {
	d, e := fn()
	if e != nil {
		return check{Name: name, Status: "FAIL", Message: e.Error(), Details: d}
	}
	return check{Name: name, Status: "PASS", Details: d}
}

func main() {
	if runProcessChaosChildIfRequested() {
		return
	}
	ctx := context.Background()
	rep := report{ToolVersion: toolVersion, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	rep.Checks = append(rep.Checks, runCheck("spool-persist-before-ack", func() (map[string]any, error) {
		svc, repo, _, task, job := setup("chaos-spool")
		os.Setenv(faultinject.EnvEnable, "1")
		os.Setenv(faultinject.EnvPlan, "cdc.spool.after_persist_before_ack=1")
		faultinject.ResetForTest()
		_, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, event("e1", "uuid:1", "1"))
		if err == nil || !strings.Contains(err.Error(), "after_persist_before_ack") {
			return nil, fmt.Errorf("expected injected failure, got %v", err)
		}
		st, _ := svc.CDCSpoolStats(ctx, task.ID, "forward")
		if st.PendingTransactions != 1 {
			return nil, fmt.Errorf("pending spool=%d want 1", st.PendingTransactions)
		}
		os.Setenv(faultinject.EnvEnable, "")
		_, err = svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, event("e1", "uuid:1", "1"))
		if err != nil {
			return nil, err
		}
		st, _ = svc.CDCSpoolStats(ctx, task.ID, "forward")
		_ = repo
		if st.PendingTransactions != 1 {
			return nil, fmt.Errorf("retry duplicated spool: %+v", st)
		}
		return map[string]any{"pending_transactions": st.PendingTransactions}, nil
	}))

	rep.Checks = append(rep.Checks, runCheck("apply-before-spool-mark", func() (map[string]any, error) {
		svc, repo, st, task, job := setup("chaos-mark")
		if _, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, event("e2", "uuid:2", "2")); err != nil {
			return nil, err
		}
		cur, _ := repo.GetMigration(ctx, task.ID)
		cur.Status = domain.StatusCDCCatchingUp
		cur.UpdatedAt = time.Now()
		_ = repo.UpdateMigration(ctx, cur)
		os.Setenv(faultinject.EnvEnable, "1")
		os.Setenv(faultinject.EnvPlan, "cdc.spool.after_target_apply_before_mark=1")
		faultinject.ResetForTest()
		if _, err := svc.DrainCDCSpool(ctx, task.ID, "forward", 10); err == nil || !strings.Contains(err.Error(), "after_target_apply_before_mark") {
			return nil, fmt.Errorf("expected injected failure, got %v", err)
		}
		if st.writes != 1 {
			return nil, fmt.Errorf("writes=%d want 1", st.writes)
		}
		os.Setenv(faultinject.EnvEnable, "")
		stats, err := svc.DrainCDCSpool(ctx, task.ID, "forward", 10)
		if err != nil {
			return nil, err
		}
		if stats.PendingTransactions != 0 || st.writes != 1 {
			return nil, fmt.Errorf("retry mismatch stats=%+v writes=%d", stats, st.writes)
		}
		return map[string]any{"writes": st.writes, "commits": st.commits, "pending_transactions": stats.PendingTransactions}, nil
	}))

	rep.Checks = append(rep.Checks, runCheck("checkpoint-before-source-ack", func() (map[string]any, error) {
		svc, repo, st, task, _ := setup("chaos-ack")
		cur, _ := repo.GetMigration(ctx, task.ID)
		cur.Status = domain.StatusCDCCatchingUp
		_ = repo.UpdateMigration(ctx, cur)
		req := event("e3", "uuid:3", "3")
		os.Setenv(faultinject.EnvEnable, "1")
		os.Setenv(faultinject.EnvPlan, "cdc.apply.after_checkpoint_before_source_ack=1")
		faultinject.ResetForTest()
		if _, err := svc.ApplyCDCEvents(ctx, task.ID, req); err == nil || !strings.Contains(err.Error(), "after_checkpoint_before_source_ack") {
			return nil, fmt.Errorf("expected injected failure, got %v", err)
		}
		pos, _ := svc.CDCPositions(ctx, task.ID)
		if len(pos) == 0 || pos[0].PositionValue != "uuid:3" {
			return nil, fmt.Errorf("checkpoint missing: %+v", pos)
		}
		os.Setenv(faultinject.EnvEnable, "")
		res, err := svc.ApplyCDCEvents(ctx, task.ID, req)
		if err != nil {
			return nil, err
		}
		if !res.Duplicate || st.writes != 1 {
			return nil, fmt.Errorf("retry re-applied: res=%+v writes=%d", res, st.writes)
		}
		return map[string]any{"writes": st.writes, "checkpoint": pos[0].PositionValue, "duplicate_retry": res.Duplicate}, nil
	}))

	rep.Checks = append(rep.Checks, runCheck("target-commit-unknown-guard", func() (map[string]any, error) {
		svc, repo, st, task, job := setup("chaos-commit-unknown")
		cur, _ := repo.GetMigration(ctx, task.ID)
		cur.Status = domain.StatusCDCCatchingUp
		_ = repo.UpdateMigration(ctx, cur)
		st.commitUnknown = true
		req := event("e4", "uuid:4", "4")
		if _, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, req); err == nil || !strings.Contains(strings.ToLower(err.Error()), "commit outcome is unknown") {
			return nil, fmt.Errorf("expected commit-unknown failure, got %v", err)
		}
		items, err := svc.CDCDeadLetters(ctx, task.ID)
		if err != nil || len(items) != 1 || items[0].Status != domain.CDCDeadLetterCommitUncertain {
			return nil, fmt.Errorf("commit-unknown DLQ mismatch: items=%+v err=%v", items, err)
		}
		if _, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, req); err == nil || !strings.Contains(strings.ToLower(err.Error()), "blocked by unresolved") {
			return nil, fmt.Errorf("expected automatic retry block, got %v", err)
		}
		if st.writes != 1 {
			return nil, fmt.Errorf("uncertain retry wrote target again: writes=%d", st.writes)
		}
		if _, err := svc.ResolveCDCCommitUncertain(ctx, task.ID, items[0].ID, "COMMITTED"); err != nil {
			return nil, err
		}
		res, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, req)
		if err != nil {
			return nil, err
		}
		if !res.Duplicate || st.writes != 1 {
			return nil, fmt.Errorf("confirmed commit was replayed: res=%+v writes=%d", res, st.writes)
		}
		return map[string]any{"writes": st.writes, "commits": st.commits, "dlq_status": domain.CDCDeadLetterResolved, "duplicate_after_resolution": res.Duplicate}, nil
	}))

	rep.Checks = append(rep.Checks, runCheck("target-commit-not-committed-replay", func() (map[string]any, error) {
		svc, repo, st, task, job := setup("chaos-commit-not-committed")
		cur, _ := repo.GetMigration(ctx, task.ID)
		cur.Status = domain.StatusCDCCatchingUp
		_ = repo.UpdateMigration(ctx, cur)
		st.commitUnknownBefore = true
		req := event("e5", "uuid:5", "5")
		if _, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, req); err == nil || !strings.Contains(strings.ToLower(err.Error()), "commit outcome is unknown") {
			return nil, fmt.Errorf("expected commit-unknown failure, got %v", err)
		}
		if st.writes != 0 {
			return nil, fmt.Errorf("synthetic target unexpectedly committed before operator decision: writes=%d", st.writes)
		}
		items, err := svc.CDCDeadLetters(ctx, task.ID)
		if err != nil || len(items) != 1 || items[0].Status != domain.CDCDeadLetterCommitUncertain {
			return nil, fmt.Errorf("commit-unknown DLQ mismatch: items=%+v err=%v", items, err)
		}
		res, err := svc.ResolveCDCCommitUncertain(ctx, task.ID, items[0].ID, "NOT_COMMITTED")
		if err != nil {
			return nil, err
		}
		items, err = svc.CDCDeadLetters(ctx, task.ID)
		if err != nil || len(items) != 1 || items[0].Status != domain.CDCDeadLetterResolved {
			return nil, fmt.Errorf("NOT_COMMITTED replay did not resolve DLQ: items=%+v err=%v", items, err)
		}
		if st.writes != 1 || res.Duplicate {
			return nil, fmt.Errorf("NOT_COMMITTED replay mismatch: writes=%d result=%+v", st.writes, res)
		}
		dup, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, req)
		if err != nil {
			return nil, err
		}
		if !dup.Duplicate || st.writes != 1 {
			return nil, fmt.Errorf("post-replay source redelivery was not suppressed: writes=%d result=%+v", st.writes, dup)
		}
		return map[string]any{"writes": st.writes, "commits": st.commits, "dlq_status": items[0].Status, "duplicate_after_replay": dup.Duplicate}, nil
	}))

	rep.Checks = append(rep.Checks, runCheck("spooled-target-commit-unknown-guard", func() (map[string]any, error) {
		svc, repo, st, task, job := setup("chaos-spool-commit-unknown")
		req := event("e6", "uuid:6", "6")
		// FULL_MIGRATING stages the source transaction durably without touching the target.
		if _, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, req); err != nil {
			return nil, err
		}
		stats, _ := svc.CDCSpoolStats(ctx, task.ID, "forward")
		if stats.PendingTransactions != 1 || st.writes != 0 {
			return nil, fmt.Errorf("spool setup mismatch: stats=%+v writes=%d", stats, st.writes)
		}
		cur, _ := repo.GetMigration(ctx, task.ID)
		cur.Status = domain.StatusCDCCatchingUp
		_ = repo.UpdateMigration(ctx, cur)
		st.commitUnknown = true
		if _, err := svc.DrainCDCSpool(ctx, task.ID, "forward", 10); err == nil || !strings.Contains(strings.ToLower(err.Error()), "commit outcome is unknown") {
			return nil, fmt.Errorf("expected spool commit-unknown failure, got %v", err)
		}
		items, err := svc.CDCDeadLetters(ctx, task.ID)
		if err != nil || len(items) != 1 || items[0].Status != domain.CDCDeadLetterCommitUncertain || items[0].PositionValue != "uuid:6" {
			return nil, fmt.Errorf("spool commit-unknown DLQ mismatch: items=%+v err=%v", items, err)
		}
		if st.writes != 1 {
			return nil, fmt.Errorf("spool uncertain target writes=%d want 1", st.writes)
		}
		if _, err := svc.DrainCDCSpool(ctx, task.ID, "forward", 10); err == nil || !strings.Contains(strings.ToLower(err.Error()), "blocked by unresolved") {
			return nil, fmt.Errorf("expected subsequent spool drain block, got %v", err)
		}
		if st.writes != 1 {
			return nil, fmt.Errorf("blocked spool drain wrote target again: writes=%d", st.writes)
		}
		if _, err := svc.ResolveCDCCommitUncertain(ctx, task.ID, items[0].ID, "COMMITTED"); err != nil {
			return nil, err
		}
		stats, err = svc.DrainCDCSpool(ctx, task.ID, "forward", 10)
		if err != nil {
			return nil, err
		}
		if stats.PendingTransactions != 0 || st.writes != 1 {
			return nil, fmt.Errorf("resolved spool did not drain as duplicate: stats=%+v writes=%d", stats, st.writes)
		}
		return map[string]any{"writes": st.writes, "commits": st.commits, "dlq_position": items[0].PositionValue, "pending_transactions": stats.PendingTransactions}, nil
	}))

	rep.Checks = append(rep.Checks, runCheck("target-commit-before-checkpoint-crash-fence", func() (map[string]any, error) {
		svc, repo, st, task, job := setup("chaos-commit-before-checkpoint")
		cur, _ := repo.GetMigration(ctx, task.ID)
		cur.Status = domain.StatusCDCCatchingUp
		_ = repo.UpdateMigration(ctx, cur)
		req := event("e7", "uuid:7", "7")
		os.Setenv(faultinject.EnvEnable, "1")
		os.Setenv(faultinject.EnvPlan, "cdc.apply.after_target_before_checkpoint=1")
		faultinject.ResetForTest()
		if _, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, req); err == nil || !strings.Contains(err.Error(), "after_target_before_checkpoint") {
			return nil, fmt.Errorf("expected after-target-before-checkpoint failure, got %v", err)
		}
		os.Setenv(faultinject.EnvEnable, "")
		items, err := svc.CDCDeadLetters(ctx, task.ID)
		if err != nil || len(items) != 1 || items[0].Status != domain.CDCDeadLetterCommitUncertain || items[0].PositionValue != "uuid:7" {
			return nil, fmt.Errorf("pre-COMMIT crash fence mismatch: items=%+v err=%v", items, err)
		}
		if st.writes != 1 {
			return nil, fmt.Errorf("target commit was not simulated exactly once: writes=%d", st.writes)
		}
		if _, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, req); err == nil || !strings.Contains(strings.ToLower(err.Error()), "blocked by unresolved") {
			return nil, fmt.Errorf("expected crash-recovery replay block, got %v", err)
		}
		if st.writes != 1 {
			return nil, fmt.Errorf("crash-recovery retry duplicated target write: writes=%d", st.writes)
		}
		if _, err := svc.ResolveCDCCommitUncertain(ctx, task.ID, items[0].ID, "COMMITTED"); err != nil {
			return nil, err
		}
		dup, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, req)
		if err != nil {
			return nil, err
		}
		if !dup.Duplicate || st.writes != 1 {
			return nil, fmt.Errorf("resolved crash fence did not suppress redelivery: writes=%d result=%+v", st.writes, dup)
		}
		return map[string]any{"writes": st.writes, "commits": st.commits, "dlq_position": items[0].PositionValue, "duplicate_after_resolution": dup.Duplicate}, nil
	}))

	rep.Checks = append(rep.Checks, runCheck("not-committed-replay-required-fence", func() (map[string]any, error) {
		svc, repo, st, task, job := setup("chaos-replay-required")
		cur, _ := repo.GetMigration(ctx, task.ID)
		cur.Status = domain.StatusCDCCatchingUp
		_ = repo.UpdateMigration(ctx, cur)
		req := event("e8", "uuid:8", "8")
		st.commitUnknownBefore = true
		if _, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, req); err == nil {
			return nil, errors.New("expected initial commit-unknown failure")
		}
		items, _ := svc.CDCDeadLetters(ctx, task.ID)
		if len(items) != 1 || items[0].Status != domain.CDCDeadLetterCommitUncertain {
			return nil, fmt.Errorf("initial uncertain DLQ mismatch: %+v", items)
		}
		st.writeFailOnce = true
		if _, err := svc.ResolveCDCCommitUncertain(ctx, task.ID, items[0].ID, "NOT_COMMITTED"); err == nil || !strings.Contains(err.Error(), "synthetic target write failure") {
			return nil, fmt.Errorf("expected controlled replay failure, got %v", err)
		}
		items, _ = svc.CDCDeadLetters(ctx, task.ID)
		if len(items) != 1 || items[0].Status != domain.CDCDeadLetterReplayRequired {
			return nil, fmt.Errorf("failed NOT_COMMITTED replay must stay REPLAY_REQUIRED: %+v", items)
		}
		if _, err := svc.ApplyEngineJobCDCEvents(ctx, "chaos-worker", job.ID, req); err == nil || !strings.Contains(strings.ToLower(err.Error()), "explicit replay") {
			return nil, fmt.Errorf("expected automatic source-flow block while replay required, got %v", err)
		}
		if st.writes != 0 {
			return nil, fmt.Errorf("blocked source flow wrote target: writes=%d", st.writes)
		}
		res, err := svc.ReplayCDCDeadLetter(ctx, task.ID, items[0].ID)
		if err != nil {
			return nil, err
		}
		items, _ = svc.CDCDeadLetters(ctx, task.ID)
		if len(items) != 1 || items[0].Status != domain.CDCDeadLetterResolved || st.writes != 1 || res.Duplicate {
			return nil, fmt.Errorf("explicit recovery replay mismatch: items=%+v writes=%d result=%+v", items, st.writes, res)
		}
		return map[string]any{"writes": st.writes, "dlq_status": items[0].Status, "source_flow_blocked_until_replay": true}, nil
	}))

	rep.Checks = append(rep.Checks, networkChaosCheck())
	rep.Checks = append(rep.Checks, networkBlackholeCheck())
	rep.Checks = append(rep.Checks, storageENOSPCCheck())
	rep.Checks = append(rep.Checks, processSIGKILLChecks()...)

	rep.Qualified = true
	for _, c := range rep.Checks {
		if c.Status != "PASS" {
			rep.Qualified = false
		}
	}
	os.Unsetenv(faultinject.EnvEnable)
	os.Unsetenv(faultinject.EnvPlan)
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if !rep.Qualified {
		os.Exit(1)
	}
}
