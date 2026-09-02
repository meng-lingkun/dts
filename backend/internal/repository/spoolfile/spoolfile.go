package spoolfile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/faultinject"
	"qmigration/backend/internal/repository"
	"strconv"
	"strings"
	"sync"
	"time"
)

const refPrefix = "spoolfile:v1:"

var ErrStorageCritical = errors.New("CDC spool filesystem is above critical watermark; source position is not acknowledged")

type Config struct {
	Root             string
	WarnUsedPct      float64
	CriticalUsedPct  float64
	AppliedRetention time.Duration
	// CapacityBytesOverride exists for deterministic tests and embedded deployments
	// that expose a quota smaller than the underlying filesystem.
	CapacityBytesOverride int64
}

type Store struct {
	repository.Repository
	cfg  Config
	mu   sync.Mutex
	refs map[string]string
}

func (s *Store) PruneMetadata(ctx context.Context, policy repository.MetadataRetentionPolicy) (repository.MetadataPruneResult, error) {
	m, ok := s.Repository.(repository.MetadataMaintenance)
	if !ok {
		return repository.MetadataPruneResult{}, nil
	}
	return m.PruneMetadata(ctx, policy)
}

var _ repository.MetadataMaintenance = (*Store)(nil)

func (s *Store) GetValidationArchive(ctx context.Context, taskID string) (*domain.ValidationArchive, error) {
	p, ok := s.Repository.(repository.ValidationArchiveProvider)
	if !ok {
		return nil, nil
	}
	return p.GetValidationArchive(ctx, taskID)
}

func (s *Store) CreateValidationArchive(ctx context.Context, a *domain.ValidationArchive) (bool, error) {
	p, ok := s.Repository.(repository.ValidationArchiveProvider)
	if !ok {
		return false, nil
	}
	return p.CreateValidationArchive(ctx, a)
}

func (s *Store) ListValidationEvidencePage(ctx context.Context, taskID, tableID string, afterChunkNo int, afterID string, limit int) ([]repository.ValidationEvidenceRow, error) {
	p, ok := s.Repository.(repository.ValidationArchiveProvider)
	if !ok {
		return nil, nil
	}
	return p.ListValidationEvidencePage(ctx, taskID, tableID, afterChunkNo, afterID, limit)
}

func (s *Store) LatestValidationStatusCounts(ctx context.Context, taskID string) (success, mismatch, validationError, missing int, err error) {
	p, ok := s.Repository.(repository.ValidationArchiveProvider)
	if !ok {
		return 0, 0, 0, 0, nil
	}
	return p.LatestValidationStatusCounts(ctx, taskID)
}

var _ repository.ValidationArchiveProvider = (*Store)(nil)

func (s *Store) GetValidationReportArchive(ctx context.Context, taskID, evidenceDigest string) (*domain.ValidationReportArchiveRecord, error) {
	p, ok := s.Repository.(repository.ValidationReportArchiveProvider)
	if !ok {
		return nil, nil
	}
	return p.GetValidationReportArchive(ctx, taskID, evidenceDigest)
}

func (s *Store) CreateValidationReportArchive(ctx context.Context, a *domain.ValidationReportArchiveRecord) (bool, error) {
	p, ok := s.Repository.(repository.ValidationReportArchiveProvider)
	if !ok {
		return false, nil
	}
	return p.CreateValidationReportArchive(ctx, a)
}

var _ repository.ValidationReportArchiveProvider = (*Store)(nil)

func ConfigFromEnv() Config {
	cfg := Config{
		Root:             filepath.Clean(env("QMIGRATION_CDC_SPOOL_DIR", filepath.Join("data", "cdc-spool"))),
		WarnUsedPct:      floatEnv("QMIGRATION_CDC_SPOOL_DISK_WARN_PCT", 80),
		CriticalUsedPct:  floatEnv("QMIGRATION_CDC_SPOOL_DISK_CRITICAL_PCT", 90),
		AppliedRetention: time.Duration(intEnv("QMIGRATION_CDC_SPOOL_APPLIED_FILE_RETENTION_HOURS", 24)) * time.Hour,
	}
	return cfg
}

func env(k, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return fallback
}
func floatEnv(k string, fallback float64) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(os.Getenv(k)), 64)
	if err != nil || v <= 0 || v > 100 {
		return fallback
	}
	return v
}
func intEnv(k string, fallback int) int {
	v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(k)))
	if err != nil || v < 0 {
		return fallback
	}
	return v
}

func New(inner repository.Repository, cfg Config) (*Store, error) {
	if inner == nil {
		return nil, errors.New("nil repository")
	}
	if strings.TrimSpace(cfg.Root) == "" {
		return nil, errors.New("CDC spool file root is empty")
	}
	if cfg.WarnUsedPct <= 0 {
		cfg.WarnUsedPct = 80
	}
	if cfg.CriticalUsedPct <= 0 {
		cfg.CriticalUsedPct = 90
	}
	if cfg.CriticalUsedPct <= cfg.WarnUsedPct {
		return nil, errors.New("CDC spool critical watermark must be greater than warn watermark")
	}
	if cfg.AppliedRetention <= 0 {
		cfg.AppliedRetention = 24 * time.Hour
	}
	if err := os.MkdirAll(filepath.Join(cfg.Root, "pending"), 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(cfg.Root, "applied"), 0700); err != nil {
		return nil, err
	}
	return &Store{Repository: inner, cfg: cfg, refs: map[string]string{}}, nil
}

func (s *Store) Root() string { return s.cfg.Root }

func (s *Store) AcquireCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string, ttl time.Duration) (bool, error) {
	type provider interface {
		AcquireCDCSpoolDrainLease(context.Context, string, string, string, time.Duration) (bool, error)
	}
	p, ok := s.Repository.(provider)
	if !ok {
		return true, nil
	}
	return p.AcquireCDCSpoolDrainLease(ctx, taskID, direction, owner, ttl)
}
func (s *Store) ReleaseCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string) error {
	type provider interface {
		ReleaseCDCSpoolDrainLease(context.Context, string, string, string) error
	}
	p, ok := s.Repository.(provider)
	if !ok {
		return nil
	}
	return p.ReleaseCDCSpoolDrainLease(ctx, taskID, direction, owner)
}

func (s *Store) CDCSpoolStorageReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	stats, err := s.storageStats()
	if err != nil {
		return err
	}
	if strings.EqualFold(stats.StorageLevel, "CRITICAL") {
		return fmt.Errorf("CDC spool filesystem critical at %.1f%% used", stats.StorageUsedPct)
	}
	probeDir := filepath.Join(s.cfg.Root, ".ready")
	if err := os.MkdirAll(probeDir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(probeDir, ".probe-*")
	if err != nil {
		return err
	}
	name := f.Name()
	if _, err := f.Write([]byte("ok")); err != nil {
		_ = f.Close()
		_ = os.Remove(name)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(name)
		return err
	}
	_ = f.Close()
	return os.Remove(name)
}

func (s *Store) MetadataSchemaVersion(ctx context.Context) (string, error) {
	type provider interface {
		MetadataSchemaVersion(context.Context) (string, error)
	}
	p, ok := s.Repository.(provider)
	if !ok {
		return "", nil
	}
	return p.MetadataSchemaVersion(ctx)
}

func shard(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:1])
}
func cleanID(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:16])
}
func (s *Store) pendingRel(id string) string {
	return filepath.Join("pending", shard(id), cleanID(id)+".blob")
}
func (s *Store) appliedRel(id string, at time.Time) string {
	return filepath.Join("applied", at.UTC().Format("20060102"), shard(id), cleanID(id)+".blob")
}
func (s *Store) abs(rel string) (string, error) {
	rel = filepath.Clean(rel)
	if rel == "." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || rel == ".." {
		return "", errors.New("invalid CDC spool file reference")
	}
	root, err := filepath.Abs(s.cfg.Root)
	if err != nil {
		return "", err
	}
	p, err := filepath.Abs(filepath.Join(root, rel))
	if err != nil {
		return "", err
	}
	if p != root && !strings.HasPrefix(p, root+string(os.PathSeparator)) {
		return "", errors.New("CDC spool reference escapes root")
	}
	return p, nil
}
func ref(rel string) string { return refPrefix + filepath.ToSlash(rel) }
func refRel(v string) (string, bool) {
	if !strings.HasPrefix(v, refPrefix) {
		return "", false
	}
	rel := filepath.FromSlash(strings.TrimPrefix(v, refPrefix))
	rel = filepath.Clean(rel)
	if rel == "." || filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", false
	}
	return rel, true
}

func (s *Store) storageStats() (domain.CDCSpoolStats, error) {
	capacity, free, err := diskCapacity(s.cfg.Root)
	if err != nil {
		return domain.CDCSpoolStats{}, err
	}
	if s.cfg.CapacityBytesOverride > 0 && s.cfg.CapacityBytesOverride < capacity {
		// Treat current QMigration spool bytes as the used portion of an explicit quota.
		used, err := dirBytes(s.cfg.Root)
		if err != nil {
			return domain.CDCSpoolStats{}, err
		}
		capacity = s.cfg.CapacityBytesOverride
		if used > capacity {
			used = capacity
		}
		free = capacity - used
	}
	used := capacity - free
	pct := 0.0
	if capacity > 0 {
		pct = float64(used) * 100 / float64(capacity)
	}
	level := "NORMAL"
	if pct >= s.cfg.CriticalUsedPct {
		level = "CRITICAL"
	} else if pct >= s.cfg.WarnUsedPct {
		level = "WARN"
	}
	return domain.CDCSpoolStats{StorageBackend: "file", StorageLevel: level, StorageCapacityBytes: capacity, StorageUsedBytes: used, StorageFreeBytes: free, StorageUsedPct: pct}, nil
}

func dirBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func writeAtomic(path string, payload []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".spool-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0600); err != nil {
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	ok = true
	return nil
}

func (s *Store) CreateCDCSpool(ctx context.Context, v *domain.CDCSpoolRecord) error {
	if v == nil {
		return errors.New("nil CDC spool record")
	}
	if strings.TrimSpace(v.EventsCiphertext) == "" {
		// Plain repositories and unit tests can still use metadata payload storage.
		return s.Repository.CreateCDCSpool(ctx, v)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stat, err := s.storageStats()
	if err != nil {
		return err
	}
	if strings.EqualFold(stat.StorageLevel, "CRITICAL") {
		return fmt.Errorf("%w (used=%.1f%% critical=%.1f%% root=%s)", ErrStorageCritical, stat.StorageUsedPct, s.cfg.CriticalUsedPct, s.cfg.Root)
	}
	rel := s.pendingRel(v.ID)
	path, err := s.abs(rel)
	if err != nil {
		return err
	}
	if err := faultinject.Check("cdc.spool.file.before_write"); err != nil {
		return fmt.Errorf("CDC spool file pre-write fault: %w", err)
	}
	if err := writeAtomic(path, []byte(v.EventsCiphertext)); err != nil {
		return fmt.Errorf("write CDC spool file: %w", err)
	}
	if err := faultinject.Check("cdc.spool.file.after_persist_before_metadata"); err != nil {
		return fmt.Errorf("CDC spool file persisted before metadata fault: %w", err)
	}
	c := *v
	c.EventsCiphertext = ref(rel)
	if err := s.Repository.CreateCDCSpool(ctx, &c); err != nil {
		return err
	}
	s.refs[v.ID] = rel
	v.Sequence = c.Sequence
	return nil
}

func (s *Store) hydrate(items []domain.CDCSpoolRecord) error {
	for i := range items {
		rel, ok := refRel(items[i].EventsCiphertext)
		if !ok {
			continue
		}
		s.mu.Lock()
		s.refs[items[i].ID] = rel
		s.mu.Unlock()
		path, err := s.abs(rel)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read CDC spool file %s: %w", rel, err)
		}
		items[i].EventsCiphertext = string(raw)
	}
	return nil
}
func (s *Store) ListCDCSpool(ctx context.Context, taskID, direction string, n int) ([]domain.CDCSpoolRecord, error) {
	items, err := s.Repository.ListCDCSpool(ctx, taskID, direction, n)
	if err != nil {
		return nil, err
	}
	if err := s.hydrate(items); err != nil {
		return nil, err
	}
	return items, nil
}
func (s *Store) LatestPendingCDCSpool(ctx context.Context, taskID, direction string) (*domain.CDCSpoolRecord, error) {
	v, err := s.Repository.LatestPendingCDCSpool(ctx, taskID, direction)
	if err != nil {
		return nil, err
	}
	items := []domain.CDCSpoolRecord{*v}
	if err := s.hydrate(items); err != nil {
		return nil, err
	}
	return &items[0], nil
}
func (s *Store) MarkCDCSpoolApplied(ctx context.Context, id string, at time.Time) error {
	// Prefer the in-process reference cache; fall back to repository discovery
	// after restart. This keeps MarkApplied O(1) on the normal path.
	s.mu.Lock()
	rel, found := s.refs[id]
	s.mu.Unlock()
	migrations, _ := s.Repository.ListMigrations(ctx)
	for _, m := range migrations {
		for _, direction := range []string{"forward", "reverse"} {
			rows, err := s.Repository.ListCDCSpool(ctx, m.ID, direction, 10000)
			if err != nil {
				continue
			}
			for _, row := range rows {
				if row.ID == id {
					rel, found = refRel(row.EventsCiphertext)
					break
				}
			}
			if found {
				break
			}
		}
		if found {
			break
		}
	}
	if err := s.Repository.MarkCDCSpoolApplied(ctx, id, at); err != nil {
		return err
	}
	if !found {
		return nil
	}
	s.mu.Lock()
	delete(s.refs, id)
	s.mu.Unlock()
	oldPath, err := s.abs(rel)
	if err != nil {
		return err
	}
	newRel := s.appliedRel(id, at)
	newPath, err := s.abs(newRel)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0700); err != nil {
		return err
	}
	// Target apply + metadata APPLIED are the correctness boundary. Moving the
	// already-consumed blob is best-effort GC; a failure must never turn a
	// committed target transaction into an apparent CDC apply failure. Reconcile
	// will recover the orphan on restart.
	_ = os.Rename(oldPath, newPath)
	return nil
}
func (s *Store) DeleteAppliedCDCSpool(ctx context.Context, taskID, direction string, keep int) error {
	if err := s.Repository.DeleteAppliedCDCSpool(ctx, taskID, direction, keep); err != nil {
		return err
	}
	return s.PurgeApplied(time.Now())
}
func (s *Store) CDCSpoolStats(ctx context.Context, taskID, direction string) (domain.CDCSpoolStats, error) {
	st, err := s.Repository.CDCSpoolStats(ctx, taskID, direction)
	if err != nil {
		return st, err
	}
	disk, err := s.storageStats()
	if err != nil {
		return st, err
	}
	st.StorageBackend = disk.StorageBackend
	st.StorageLevel = disk.StorageLevel
	st.StorageCapacityBytes = disk.StorageCapacityBytes
	st.StorageUsedBytes = disk.StorageUsedBytes
	st.StorageFreeBytes = disk.StorageFreeBytes
	st.StorageUsedPct = disk.StorageUsedPct
	return st, nil
}

func (s *Store) PurgeApplied(now time.Time) error {
	root := filepath.Join(s.cfg.Root, "applied")
	cutoff := now.Add(-s.cfg.AppliedRetention)
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(path)
		}
		return nil
	})
}

// Reconcile removes pending-file orphans left by crashes between atomic file
// persistence and metadata commit, or between metadata APPLIED and file move.
// It never removes a file referenced by a PENDING metadata row.
func (s *Store) Reconcile(ctx context.Context) error {
	referenced := map[string]struct{}{}
	migrations, err := s.Repository.ListMigrations(ctx)
	if err != nil {
		return err
	}
	completeIndex := true
	for _, m := range migrations {
		for _, direction := range []string{"forward", "reverse"} {
			stats, statErr := s.Repository.CDCSpoolStats(ctx, m.ID, direction)
			if statErr == nil && stats.PendingTransactions > 10000 {
				completeIndex = false
			}
			rows, err := s.Repository.ListCDCSpool(ctx, m.ID, direction, 10000)
			if err != nil {
				continue
			}
			for _, row := range rows {
				if rel, ok := refRel(row.EventsCiphertext); ok {
					referenced[filepath.Clean(rel)] = struct{}{}
				}
			}
		}
	}
	if !completeIndex {
		// The compact Repository API caps a single pending listing at 10k rows.
		// When backlog exceeds that, orphan inference would be unsafe; leave all
		// pending files untouched until the backlog falls below the reconciliation
		// window.
		return s.PurgeApplied(time.Now())
	}
	pendingRoot := filepath.Join(s.cfg.Root, "pending")
	return filepath.WalkDir(pendingRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.cfg.Root, path)
		if err != nil {
			return err
		}
		rel = filepath.Clean(rel)
		if _, ok := referenced[rel]; ok {
			return nil
		}
		orphanRel := filepath.Join("applied", "recovered-orphans", filepath.Base(filepath.Dir(path)), filepath.Base(path))
		orphan, err := s.abs(orphanRel)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(orphan), 0700); err != nil {
			return err
		}
		if err := os.Rename(path, orphan); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	})
}

// RC42 delegates optional chunk aggregation/hot-path capabilities through the
// CDC spool decorator so upper repository wrappers retain bounded queries.
func (s *Store) SummarizeTaskChunks(ctx context.Context, taskID string) (repository.TaskChunkSummary, error) {
	return repository.SummarizeChunks(ctx, s.Repository, taskID)
}
func (s *Store) MaxTaskChunkNo(ctx context.Context, taskID string) (int, error) {
	return repository.MaxTaskChunkNo(ctx, s.Repository, taskID)
}
func (s *Store) CountTableRunnable(ctx context.Context, taskID, tableID string) (repository.TableRunnableCounts, error) {
	return repository.CountTableRunnable(ctx, s.Repository, taskID, tableID)
}
func (s *Store) ListPendingTableChunks(ctx context.Context, taskID, tableID string) ([]domain.MigrationChunk, error) {
	return repository.ListPendingTableChunks(ctx, s.Repository, taskID, tableID)
}
func (s *Store) ListRunningTopologyChunks(ctx context.Context, taskID, topologyID string) ([]domain.MigrationChunk, error) {
	return repository.ListRunningTopologyChunks(ctx, s.Repository, taskID, topologyID)
}
func (s *Store) ListRunningFaultDomainChunks(ctx context.Context, taskID, scope, value string) ([]domain.MigrationChunk, error) {
	return repository.ListRunningFaultDomainChunks(ctx, s.Repository, taskID, scope, value)
}

var _ repository.ChunkSummaryProvider = (*Store)(nil)
var _ repository.ChunkHotPathProvider = (*Store)(nil)

func (s *Store) MetadataStorageStats(ctx context.Context) (repository.MetadataStorageStats, error) {
	return repository.ReadMetadataStorageStats(ctx, s.Repository)
}

var _ repository.MetadataStatsProvider = (*Store)(nil)

func (s *Store) ListTableChunksPage(c context.Context, taskID, tableID string, afterChunkNo int, afterID string, limit int) ([]domain.MigrationChunk, error) {
	return repository.ListTableChunksPage(c, s.Repository, taskID, tableID, afterChunkNo, afterID, limit)
}
func (s *Store) HasValidationResults(c context.Context, taskID string) (bool, error) {
	return repository.HasValidationResults(c, s.Repository, taskID)
}
func (s *Store) FirstInvalidSuccessfulChunk(c context.Context, taskID string) (string, domain.ValidationStatus, error) {
	return repository.FirstInvalidSuccessfulChunk(c, s.Repository, taskID)
}
func (s *Store) ListRepairableValidationChunkIDs(c context.Context, taskID string, limit int) ([]string, error) {
	return repository.ListRepairableValidationChunkIDs(c, s.Repository, taskID, limit)
}

var _ repository.ValidationHotPathProvider = (*Store)(nil)
