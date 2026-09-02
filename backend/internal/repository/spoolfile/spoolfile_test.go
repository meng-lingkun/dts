package spoolfile

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/faultinject"
	"qmigration/backend/internal/repository"
	"qmigration/backend/internal/repository/memory"
	securerepo "qmigration/backend/internal/repository/secure"
	"qmigration/backend/internal/security"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestEncryptedPayloadLivesOutsideMetadataAndHydrates(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	state := filepath.Join(t.TempDir(), "state.json")
	base, err := memory.NewPersistent(state)
	if err != nil {
		t.Fatal(err)
	}
	files, err := New(base, Config{Root: root, WarnUsedPct: 80, CriticalUsedPct: 90, AppliedRetention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	cipher, err := security.New("spoolfile-test-master")
	if err != nil {
		t.Fatal(err)
	}
	repo := securerepo.New(files, cipher)
	now := time.Now()
	if err := repo.CreateMigration(ctx, &domain.MigrationTask{ID: "task1", Name: "t", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	rec := &domain.CDCSpoolRecord{ID: "spool1", TaskID: "task1", Direction: "forward", PositionType: "GTID", PositionValue: "uuid:1", Events: []domain.CDCEvent{{Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "customer", After: []domain.CDCField{{Column: "secret", Value: "sensitive-value"}}}}, Status: domain.CDCSpoolPending, CreatedAt: now}
	if err := repo.CreateCDCSpool(ctx, rec); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(state)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "sensitive-value") {
		t.Fatal("plaintext leaked into metadata")
	}
	if !strings.Contains(string(raw), refPrefix) {
		t.Fatalf("metadata did not contain external spool reference: %s", raw)
	}
	foundBlob := false
	err = filepath.WalkDir(filepath.Join(root, "pending"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		foundBlob = true
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), "sensitive-value") {
			t.Fatal("plaintext leaked into file spool")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !foundBlob {
		t.Fatal("external spool blob not created")
	}
	items, err := repo.ListCDCSpool(ctx, "task1", "forward", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || len(items[0].Events) != 1 || items[0].Events[0].After[0].Value != "sensitive-value" {
		t.Fatalf("hydrate/decrypt failed: %+v", items)
	}
	stats, err := repo.CDCSpoolStats(ctx, "task1", "forward")
	if err != nil {
		t.Fatal(err)
	}
	if stats.StorageBackend != "file" || stats.StorageCapacityBytes <= 0 {
		t.Fatalf("missing file storage stats: %+v", stats)
	}
}

func TestCriticalWatermarkRejectsBeforeMetadataCommit(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "fill"), make([]byte, 950), 0600); err != nil {
		t.Fatal(err)
	}
	base := memory.New()
	files, err := New(base, Config{Root: root, WarnUsedPct: 80, CriticalUsedPct: 90, AppliedRetention: time.Hour, CapacityBytesOverride: 1000})
	if err != nil {
		t.Fatal(err)
	}
	cipher, _ := security.New("master")
	repo := securerepo.New(files, cipher)
	rec := &domain.CDCSpoolRecord{ID: "crit1", TaskID: "task", Direction: "forward", PositionType: "LSN", PositionValue: "0/100", Events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "LSN", PositionValue: "0/100"}}, Status: domain.CDCSpoolPending, CreatedAt: time.Now()}
	err = repo.CreateCDCSpool(ctx, rec)
	if err == nil || !errors.Is(err, ErrStorageCritical) {
		t.Fatalf("expected critical watermark failure, got %v", err)
	}
	stats, err := base.CDCSpoolStats(ctx, "task", "forward")
	if err != nil {
		t.Fatal(err)
	}
	if stats.PendingTransactions != 0 {
		t.Fatalf("critical write reached metadata: %+v", stats)
	}
}

func TestAppliedPayloadLeavesPendingPool(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	base := memory.New()
	now := time.Now()
	if err := base.CreateMigration(ctx, &domain.MigrationTask{ID: "task1", Name: "t", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	files, err := New(base, Config{Root: root, WarnUsedPct: 80, CriticalUsedPct: 90, AppliedRetention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	cipher, _ := security.New("master")
	repo := securerepo.New(files, cipher)
	rec := &domain.CDCSpoolRecord{ID: "move1", TaskID: "task1", Direction: "forward", PositionType: "BINLOG", PositionValue: "bin.1:4", Events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "BINLOG", PositionValue: "bin.1:4"}}, Status: domain.CDCSpoolPending, CreatedAt: now}
	if err := repo.CreateCDCSpool(ctx, rec); err != nil {
		t.Fatal(err)
	}
	pendingBefore, _ := dirBytes(filepath.Join(root, "pending"))
	if pendingBefore == 0 {
		t.Fatal("expected pending file")
	}
	if err := repo.MarkCDCSpoolApplied(ctx, rec.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	pendingAfter, _ := dirBytes(filepath.Join(root, "pending"))
	applied, _ := dirBytes(filepath.Join(root, "applied"))
	if pendingAfter != 0 || applied == 0 {
		t.Fatalf("file not moved out of pending: pending=%d applied=%d", pendingAfter, applied)
	}
}

func TestInjectedSpoolFileFailureBeforeWriteLeavesNoPayload(t *testing.T) {
	ctx := context.Background()
	base := memory.New()
	root := t.TempDir()
	repo, err := New(base, Config{Root: root, WarnUsedPct: 99, CriticalUsedPct: 100, AppliedRetention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(faultinject.EnvEnable, "1")
	t.Setenv(faultinject.EnvPlan, "cdc.spool.file.before_write=1")
	faultinject.ResetForTest()
	err = repo.CreateCDCSpool(ctx, &domain.CDCSpoolRecord{ID: "disk-fail-before", TaskID: "task", Direction: "forward", EventsCiphertext: "cipher", EventCount: 1, Status: domain.CDCSpoolPending, CreatedAt: time.Now()})
	if err == nil || !strings.Contains(err.Error(), "pre-write fault") {
		t.Fatalf("expected pre-write fault, got %v", err)
	}
	var files int
	_ = filepath.WalkDir(filepath.Join(root, "pending"), func(_ string, d fs.DirEntry, e error) error {
		if e == nil && d != nil && !d.IsDir() {
			files++
		}
		return nil
	})
	if files != 0 {
		t.Fatalf("pre-write fault left %d pending files", files)
	}
}

func TestInjectedSpoolFileENOSPCKeepsMetadataClean(t *testing.T) {
	ctx := context.Background()
	base := memory.New()
	root := t.TempDir()
	repo, err := New(base, Config{Root: root, WarnUsedPct: 99, CriticalUsedPct: 100, AppliedRetention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(faultinject.EnvEnable, "1")
	t.Setenv(faultinject.EnvPlan, "cdc.spool.file.before_write=1@ENOSPC")
	faultinject.ResetForTest()
	err = repo.CreateCDCSpool(ctx, &domain.CDCSpoolRecord{ID: "disk-enospc", TaskID: "task", Direction: "forward", EventsCiphertext: "cipher", EventCount: 1, Status: domain.CDCSpoolPending, CreatedAt: time.Now()})
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("expected syscall.ENOSPC identity, got %v", err)
	}
	stats, statErr := base.CDCSpoolStats(ctx, "task", "forward")
	if statErr != nil || stats.PendingTransactions != 0 {
		t.Fatalf("metadata committed despite ENOSPC: stats=%+v err=%v", stats, statErr)
	}
	var files int
	_ = filepath.WalkDir(filepath.Join(root, "pending"), func(_ string, d fs.DirEntry, e error) error {
		if e == nil && d != nil && !d.IsDir() {
			files++
		}
		return nil
	})
	if files != 0 {
		t.Fatalf("ENOSPC left %d pending files", files)
	}
}

func TestInjectedSpoolFileCrashWindowIsReconciledAsOrphan(t *testing.T) {
	ctx := context.Background()
	base := memory.New()
	root := t.TempDir()
	repo, err := New(base, Config{Root: root, WarnUsedPct: 99, CriticalUsedPct: 100, AppliedRetention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(faultinject.EnvEnable, "1")
	t.Setenv(faultinject.EnvPlan, "cdc.spool.file.after_persist_before_metadata=1")
	faultinject.ResetForTest()
	err = repo.CreateCDCSpool(ctx, &domain.CDCSpoolRecord{ID: "disk-orphan", TaskID: "task", Direction: "forward", EventsCiphertext: "cipher", EventCount: 1, Status: domain.CDCSpoolPending, CreatedAt: time.Now()})
	if err == nil || !strings.Contains(err.Error(), "persisted before metadata") {
		t.Fatalf("expected after-persist fault, got %v", err)
	}
	stats, err := base.CDCSpoolStats(ctx, "task", "forward")
	if err != nil || stats.PendingTransactions != 0 {
		t.Fatalf("metadata committed despite fault: stats=%+v err=%v", stats, err)
	}
	t.Setenv(faultinject.EnvEnable, "")
	if err := repo.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var pending, recovered int
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, e error) error {
		if e != nil || d == nil || d.IsDir() {
			return nil
		}
		if strings.Contains(path, string(filepath.Separator)+"pending"+string(filepath.Separator)) {
			pending++
		}
		if strings.Contains(path, "recovered-orphans") {
			recovered++
		}
		return nil
	})
	if pending != 0 || recovered != 1 {
		t.Fatalf("orphan reconcile mismatch pending=%d recovered=%d", pending, recovered)
	}
}

type rc42CapabilityRepo struct{ repository.Repository }

func (r *rc42CapabilityRepo) SummarizeTaskChunks(context.Context, string) (repository.TaskChunkSummary, error) {
	return repository.TaskChunkSummary{Total: 42}, nil
}
func (r *rc42CapabilityRepo) MaxTaskChunkNo(context.Context, string) (int, error) { return 77, nil }
func (r *rc42CapabilityRepo) CountTableRunnable(context.Context, string, string) (repository.TableRunnableCounts, error) {
	return repository.TableRunnableCounts{Pending: 3, Running: 2}, nil
}
func (r *rc42CapabilityRepo) ListPendingTableChunks(context.Context, string, string) ([]domain.MigrationChunk, error) {
	return []domain.MigrationChunk{{ID: "pending-hot"}}, nil
}
func (r *rc42CapabilityRepo) ListRunningTopologyChunks(context.Context, string, string) ([]domain.MigrationChunk, error) {
	return []domain.MigrationChunk{{ID: "topology-hot"}}, nil
}
func (r *rc42CapabilityRepo) ListRunningFaultDomainChunks(context.Context, string, string, string) ([]domain.MigrationChunk, error) {
	return []domain.MigrationChunk{{ID: "domain-hot"}}, nil
}
func (r *rc42CapabilityRepo) MetadataStorageStats(context.Context) (repository.MetadataStorageStats, error) {
	return repository.MetadataStorageStats{TotalBytes: 1234, Relations: []repository.MetadataRelationStats{{Relation: "migration_chunks", TotalBytes: 1000, LiveRows: 90, DeadRows: 10}}}, nil
}
func (r *rc42CapabilityRepo) PruneMetadata(context.Context, repository.MetadataRetentionPolicy) (repository.MetadataPruneResult, error) {
	return repository.MetadataPruneResult{ValidationDeleted: 7}, nil
}

func TestRC42OptionalHotPathCapabilitiesSurviveSpoolAndSecureDecorators(t *testing.T) {
	ctx := context.Background()
	files, err := New(&rc42CapabilityRepo{}, Config{Root: t.TempDir(), WarnUsedPct: 80, CriticalUsedPct: 90, AppliedRetention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	cipher, _ := security.New("rc42-capability-master")
	wrapped := securerepo.New(files, cipher)

	summary, err := repository.SummarizeChunks(ctx, wrapped, "task")
	if err != nil || summary.Total != 42 {
		t.Fatalf("summary capability lost: %+v err=%v", summary, err)
	}
	maxNo, err := repository.MaxTaskChunkNo(ctx, wrapped, "task")
	if err != nil || maxNo != 77 {
		t.Fatalf("max chunk capability lost: %d err=%v", maxNo, err)
	}
	counts, err := repository.CountTableRunnable(ctx, wrapped, "task", "table")
	if err != nil || counts.Pending != 3 || counts.Running != 2 {
		t.Fatalf("count capability lost: %+v err=%v", counts, err)
	}
	pending, err := repository.ListPendingTableChunks(ctx, wrapped, "task", "table")
	if err != nil || len(pending) != 1 || pending[0].ID != "pending-hot" {
		t.Fatalf("pending capability lost: %+v err=%v", pending, err)
	}
	topology, err := repository.ListRunningTopologyChunks(ctx, wrapped, "task", "dn")
	if err != nil || len(topology) != 1 || topology[0].ID != "topology-hot" {
		t.Fatalf("topology capability lost: %+v err=%v", topology, err)
	}
	domainChunks, err := repository.ListRunningFaultDomainChunks(ctx, wrapped, "task", "zone", "a")
	if err != nil || len(domainChunks) != 1 || domainChunks[0].ID != "domain-hot" {
		t.Fatalf("fault domain capability lost: %+v err=%v", domainChunks, err)
	}
	storage, err := repository.ReadMetadataStorageStats(ctx, wrapped)
	if err != nil || storage.TotalBytes != 1234 || len(storage.Relations) != 1 {
		t.Fatalf("metadata stats capability lost: %+v err=%v", storage, err)
	}
	maintenanceProvider, ok := any(wrapped).(repository.MetadataMaintenance)
	if !ok {
		t.Fatal("metadata maintenance capability lost through secure/file-spool decorators")
	}
	pruned, err := maintenanceProvider.PruneMetadata(ctx, repository.MetadataRetentionPolicy{ValidationMaxAttemptsPerChunk: 1})
	if err != nil || pruned.ValidationDeleted != 7 {
		t.Fatalf("metadata maintenance capability lost: %+v err=%v", pruned, err)
	}
}
