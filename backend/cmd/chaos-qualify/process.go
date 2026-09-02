package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/faultinject"
	"qmigration/backend/internal/migration"
	"qmigration/backend/internal/repository/memory"
)

const (
	chaosChildEnv      = "QMIGRATION_CHAOS_CHILD"
	chaosRepoPathEnv   = "QMIGRATION_CHAOS_REPO_PATH"
	chaosTargetPathEnv = "QMIGRATION_CHAOS_TARGET_PATH"
	chaosJobIDEnv      = "QMIGRATION_CHAOS_JOB_ID"
)

type persistentTargetState struct {
	Writes  int `json:"writes"`
	Deletes int `json:"deletes"`
}

type persistentFactory struct{ targetPath string }
type persistentConn struct {
	targetPath                    string
	pendingWrites, pendingDeletes int
}

func (f persistentFactory) Capabilities(t domain.DataSourceType) connector.Descriptor {
	return connector.Descriptor{Type: t, Protocol: "synthetic-process-chaos", Native: true, Maturity: connector.MaturityNative, Capabilities: []connector.Capability{connector.CapabilityMetadata, connector.CapabilityFullRead, connector.CapabilityFullWrite, connector.CapabilityCDCApply, connector.CapabilityCDCTransactional}}
}
func (f persistentFactory) New(domain.DataSource) (connector.Connector, error) {
	return &persistentConn{targetPath: f.targetPath}, nil
}
func (*persistentConn) TestConnection(context.Context) error                     { return nil }
func (*persistentConn) GetVersion(context.Context) (string, error)               { return "synthetic-process", nil }
func (*persistentConn) ListSchemas(context.Context) ([]domain.SchemaInfo, error) { return nil, nil }
func (*persistentConn) ListTables(context.Context, string) ([]domain.TableInfo, error) {
	return nil, nil
}
func (*persistentConn) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, errors.New("not used")
}
func (*persistentConn) Close() error { return nil }
func (*persistentConn) ReadBatch(context.Context, connector.ReadBatchRequest) (*connector.RowBatch, error) {
	return &connector.RowBatch{}, nil
}
func (c *persistentConn) WriteBatch(_ context.Context, req connector.WriteBatchRequest) (int64, error) {
	c.pendingWrites += len(req.Rows)
	return int64(len(req.Rows)), nil
}
func (c *persistentConn) DeleteByKey(context.Context, connector.DeleteByKeyRequest) error {
	c.pendingDeletes++
	return nil
}
func (c *persistentConn) BeginCDCTransaction(context.Context) error {
	c.pendingWrites, c.pendingDeletes = 0, 0
	return nil
}
func (c *persistentConn) RollbackCDCTransaction(context.Context) error {
	c.pendingWrites, c.pendingDeletes = 0, 0
	return nil
}
func (c *persistentConn) CommitCDCTransaction(context.Context) error {
	st, err := readPersistentTarget(c.targetPath)
	if err != nil {
		return err
	}
	st.Writes += c.pendingWrites
	st.Deletes += c.pendingDeletes
	c.pendingWrites, c.pendingDeletes = 0, 0
	return writePersistentTarget(c.targetPath, st)
}

func readPersistentTarget(path string) (persistentTargetState, error) {
	var st persistentTargetState
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	if len(b) == 0 {
		return st, nil
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, err
	}
	return st, nil
}
func writePersistentTarget(path string, st persistentTargetState) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func persistentRegistry(targetPath string) *connector.Registry {
	reg := connector.NewRegistry()
	f := persistentFactory{targetPath: targetPath}
	reg.Register(domain.DataSourceMySQL, f)
	reg.Register(domain.DataSourcePolarDBX, f)
	return reg
}

func setupPersistent(repoPath, targetPath, id string, status domain.MigrationStatus) (*migration.Service, *memory.Store, domain.MigrationTask, domain.EngineJob, error) {
	ctx := context.Background()
	repo, err := memory.NewPersistent(repoPath)
	if err != nil {
		return nil, nil, domain.MigrationTask{}, domain.EngineJob{}, err
	}
	now := time.Now()
	src := domain.DataSource{ID: id + "-src", Type: domain.DataSourceMySQL, Database: "app", CreatedAt: now}
	dst := domain.DataSource{ID: id + "-dst", Type: domain.DataSourcePolarDBX, Database: "app", CreatedAt: now}
	if err := repo.CreateDataSource(ctx, &src); err != nil {
		return nil, nil, domain.MigrationTask{}, domain.EngineJob{}, err
	}
	if err := repo.CreateDataSource(ctx, &dst); err != nil {
		return nil, nil, domain.MigrationTask{}, domain.EngineJob{}, err
	}
	task := domain.MigrationTask{ID: id, SourceID: src.ID, TargetID: dst.ID, Mode: domain.ModeFullAndIncremental, Status: status, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateMigration(ctx, &task); err != nil {
		return nil, nil, domain.MigrationTask{}, domain.EngineJob{}, err
	}
	table := domain.MigrationTable{ID: id + "-table", TaskID: id, SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", PrimaryKey: "id", PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}, TargetColumns: []domain.ColumnInfo{{Name: "id", DataType: "bigint", PrimaryKey: true}}}
	if err := repo.CreateMigrationTable(ctx, &table); err != nil {
		return nil, nil, domain.MigrationTask{}, domain.EngineJob{}, err
	}
	job := domain.EngineJob{ID: id + "-job", TaskID: id, Kind: "CDC", Direction: "forward", Engine: "synthetic", Status: domain.EngineJobRunning, WorkerID: "process-chaos-worker", UpdatedAt: now}
	if err := repo.CreateEngineJob(ctx, &job); err != nil {
		return nil, nil, domain.MigrationTask{}, domain.EngineJob{}, err
	}
	return migration.NewService(repo, persistentRegistry(targetPath)), repo, task, job, nil
}

func loadPersistentService(repoPath, targetPath string) (*migration.Service, *memory.Store, error) {
	repo, err := memory.NewPersistent(repoPath)
	if err != nil {
		return nil, nil, err
	}
	return migration.NewService(repo, persistentRegistry(targetPath)), repo, nil
}

// runProcessChaosChildIfRequested executes one real crash window in a child
// process. The parent sets a SIGKILL failpoint; reaching it terminates this
// process without defer/rollback cleanup, exactly as an abrupt Worker/Server
// death would.
func runProcessChaosChildIfRequested() bool {
	scenario := strings.TrimSpace(os.Getenv(chaosChildEnv))
	if scenario == "" {
		return false
	}
	repoPath := os.Getenv(chaosRepoPathEnv)
	targetPath := os.Getenv(chaosTargetPathEnv)
	jobID := os.Getenv(chaosJobIDEnv)
	svc, _, err := loadPersistentService(repoPath, targetPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(31)
	}
	switch scenario {
	case "commit-before-checkpoint":
		_, err = svc.ApplyEngineJobCDCEvents(context.Background(), "process-chaos-worker", jobID, event("process-e1", "process:1", "1"))
	case "spool-before-ack":
		_, err = svc.ApplyEngineJobCDCEvents(context.Background(), "process-chaos-worker", jobID, event("process-e2", "process:2", "2"))
	default:
		fmt.Fprintln(os.Stderr, "unknown process chaos scenario", scenario)
		os.Exit(32)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(33)
	}
	// A SIGKILL scenario returning normally means the requested crash window
	// was not reached, which must fail qualification.
	fmt.Fprintln(os.Stderr, "process chaos child returned without SIGKILL")
	os.Exit(34)
	return true
}

func runKilledChild(scenario, repoPath, targetPath, jobID, plan string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(),
		chaosChildEnv+"="+scenario,
		chaosRepoPathEnv+"="+repoPath,
		chaosTargetPathEnv+"="+targetPath,
		chaosJobIDEnv+"="+jobID,
		faultinject.EnvEnable+"=1",
		faultinject.EnvPlan+"="+plan,
	)
	err = cmd.Run()
	if err == nil {
		return errors.New("child completed normally; SIGKILL was not triggered")
	}
	if exit, ok := err.(*exec.ExitError); ok {
		if exit.ProcessState.ExitCode() == -1 || strings.Contains(strings.ToLower(exit.ProcessState.String()), "signal") {
			return nil
		}
		return fmt.Errorf("child exited instead of being killed: %s", exit.ProcessState.String())
	}
	return err
}

func processSIGKILLChecks() []check {
	out := []check{}
	out = append(out, runCheck("process-sigkill-target-commit-before-checkpoint", func() (map[string]any, error) {
		dir, err := os.MkdirTemp("", "qmigration-process-chaos-commit-")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(dir)
		repoPath, targetPath := filepath.Join(dir, "repo.json"), filepath.Join(dir, "target.json")
		_, _, task, job, err := setupPersistent(repoPath, targetPath, "process-commit", domain.StatusCDCCatchingUp)
		if err != nil {
			return nil, err
		}
		if err := runKilledChild("commit-before-checkpoint", repoPath, targetPath, job.ID, "cdc.apply.after_target_before_checkpoint=1@SIGKILL"); err != nil {
			return nil, err
		}
		st, err := readPersistentTarget(targetPath)
		if err != nil {
			return nil, err
		}
		if st.Writes != 1 {
			return nil, fmt.Errorf("target writes after SIGKILL=%d want 1", st.Writes)
		}
		svc, _, err := loadPersistentService(repoPath, targetPath)
		if err != nil {
			return nil, err
		}
		items, err := svc.CDCDeadLetters(context.Background(), task.ID)
		if err != nil || len(items) != 1 || items[0].Status != domain.CDCDeadLetterCommitUncertain {
			return nil, fmt.Errorf("durable pre-COMMIT fence missing after process death: items=%+v err=%v", items, err)
		}
		if _, err := svc.ApplyEngineJobCDCEvents(context.Background(), "process-chaos-worker", job.ID, event("process-e1", "process:1", "1")); err == nil || !strings.Contains(strings.ToLower(err.Error()), "blocked by unresolved") {
			return nil, fmt.Errorf("restart did not block ambiguous replay: %v", err)
		}
		st, _ = readPersistentTarget(targetPath)
		if st.Writes != 1 {
			return nil, fmt.Errorf("restart duplicated target before operator decision: %d", st.Writes)
		}
		if _, err := svc.ResolveCDCCommitUncertain(context.Background(), task.ID, items[0].ID, "COMMITTED"); err != nil {
			return nil, err
		}
		res, err := svc.ApplyEngineJobCDCEvents(context.Background(), "process-chaos-worker", job.ID, event("process-e1", "process:1", "1"))
		if err != nil {
			return nil, err
		}
		st, _ = readPersistentTarget(targetPath)
		if !res.Duplicate || st.Writes != 1 {
			return nil, fmt.Errorf("resolved process crash was replayed: result=%+v writes=%d", res, st.Writes)
		}
		return map[string]any{"target_writes": st.Writes, "durable_fence": true, "duplicate_after_resolution": res.Duplicate}, nil
	}))

	out = append(out, runCheck("process-sigkill-spool-persist-before-source-ack", func() (map[string]any, error) {
		dir, err := os.MkdirTemp("", "qmigration-process-chaos-spool-")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(dir)
		repoPath, targetPath := filepath.Join(dir, "repo.json"), filepath.Join(dir, "target.json")
		_, _, task, job, err := setupPersistent(repoPath, targetPath, "process-spool", domain.StatusFullMigrating)
		if err != nil {
			return nil, err
		}
		if err := runKilledChild("spool-before-ack", repoPath, targetPath, job.ID, "cdc.spool.after_persist_before_ack=1@SIGKILL"); err != nil {
			return nil, err
		}
		svc, _, err := loadPersistentService(repoPath, targetPath)
		if err != nil {
			return nil, err
		}
		stats, err := svc.CDCSpoolStats(context.Background(), task.ID, "forward")
		if err != nil {
			return nil, err
		}
		if stats.PendingTransactions != 1 {
			return nil, fmt.Errorf("durable spool after SIGKILL=%d want 1", stats.PendingTransactions)
		}
		st, _ := readPersistentTarget(targetPath)
		if st.Writes != 0 {
			return nil, fmt.Errorf("FULL stage unexpectedly wrote target: %d", st.Writes)
		}
		if _, err := svc.ApplyEngineJobCDCEvents(context.Background(), "process-chaos-worker", job.ID, event("process-e2", "process:2", "2")); err != nil {
			return nil, err
		}
		stats, _ = svc.CDCSpoolStats(context.Background(), task.ID, "forward")
		if stats.PendingTransactions != 1 {
			return nil, fmt.Errorf("source redelivery duplicated spool after restart: %+v", stats)
		}
		return map[string]any{"pending_transactions": stats.PendingTransactions, "target_writes": st.Writes}, nil
	}))
	return out
}
