package spools3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/repository"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	refPrefixV1 = "spools3:v1:"
	refPrefixV2 = "spools3:v2:"
)

type Config struct {
	Endpoint            string
	Bucket              string
	Prefix              string
	Region              string
	AccessKey           string
	SecretKey           string
	SessionToken        string
	PathStyle           bool
	AppliedRetention    time.Duration
	HTTPClient          *http.Client
	MaxPendingBytes     int64
	WarnUsedPct         float64
	CriticalUsedPct     float64
	CACert              string
	TLSServerName       string
	TLSClientCert       string
	TLSClientKey        string
	MultipartThreshold  int64
	MultipartPartSize   int64
	MultipartAbortAfter time.Duration
}

type Store struct {
	repository.Repository
	cfg            Config
	s3             *s3Client
	mu             sync.Mutex
	refs           map[string]string
	readyMu        sync.Mutex
	lastWriteProbe time.Time
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
	return Config{
		Endpoint:  strings.TrimRight(strings.TrimSpace(os.Getenv("QMIGRATION_CDC_SPOOL_S3_ENDPOINT")), "/"),
		Bucket:    strings.TrimSpace(os.Getenv("QMIGRATION_CDC_SPOOL_S3_BUCKET")),
		Prefix:    strings.Trim(strings.TrimSpace(env("QMIGRATION_CDC_SPOOL_S3_PREFIX", "qmigration/cdc-spool")), "/"),
		Region:    strings.TrimSpace(env("QMIGRATION_CDC_SPOOL_S3_REGION", "us-east-1")),
		AccessKey: os.Getenv("QMIGRATION_CDC_SPOOL_S3_ACCESS_KEY"), SecretKey: os.Getenv("QMIGRATION_CDC_SPOOL_S3_SECRET_KEY"), SessionToken: os.Getenv("QMIGRATION_CDC_SPOOL_S3_SESSION_TOKEN"),
		PathStyle:           boolEnv("QMIGRATION_CDC_SPOOL_S3_PATH_STYLE", true),
		AppliedRetention:    time.Duration(intEnv("QMIGRATION_CDC_SPOOL_APPLIED_FILE_RETENTION_HOURS", 24)) * time.Hour,
		MaxPendingBytes:     int64Env("QMIGRATION_CDC_SPOOL_MAX_PENDING_BYTES", 64<<30),
		WarnUsedPct:         floatEnv("QMIGRATION_CDC_SPOOL_DISK_WARN_PCT", 80),
		CriticalUsedPct:     floatEnv("QMIGRATION_CDC_SPOOL_DISK_CRITICAL_PCT", 90),
		CACert:              os.Getenv("QMIGRATION_CDC_SPOOL_S3_CA_CERT"),
		TLSServerName:       strings.TrimSpace(os.Getenv("QMIGRATION_CDC_SPOOL_S3_TLS_SERVER_NAME")),
		TLSClientCert:       os.Getenv("QMIGRATION_CDC_SPOOL_S3_TLS_CLIENT_CERT"),
		TLSClientKey:        os.Getenv("QMIGRATION_CDC_SPOOL_S3_TLS_CLIENT_KEY"),
		MultipartThreshold:  int64Env("QMIGRATION_CDC_SPOOL_S3_MULTIPART_THRESHOLD_BYTES", 8<<20),
		MultipartPartSize:   int64Env("QMIGRATION_CDC_SPOOL_S3_MULTIPART_PART_BYTES", 8<<20),
		MultipartAbortAfter: time.Duration(intEnv("QMIGRATION_CDC_SPOOL_S3_MULTIPART_ABORT_AFTER_HOURS", 6)) * time.Hour,
	}
}
func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
func intEnv(k string, d int) int {
	v, e := strconv.Atoi(strings.TrimSpace(os.Getenv(k)))
	if e != nil || v < 0 {
		return d
	}
	return v
}
func int64Env(k string, d int64) int64 {
	v, e := strconv.ParseInt(strings.TrimSpace(os.Getenv(k)), 10, 64)
	if e != nil || v < 0 {
		return d
	}
	return v
}
func floatEnv(k string, d float64) float64 {
	v, e := strconv.ParseFloat(strings.TrimSpace(os.Getenv(k)), 64)
	if e != nil || v <= 0 || v > 100 {
		return d
	}
	return v
}
func boolEnv(k string, d bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(k)))
	if v == "" {
		return d
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return d
}

func New(inner repository.Repository, cfg Config) (*Store, error) {
	if inner == nil {
		return nil, errors.New("nil repository")
	}
	c, err := newS3Client(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.AppliedRetention <= 0 {
		cfg.AppliedRetention = 24 * time.Hour
	}
	if cfg.MaxPendingBytes <= 0 {
		cfg.MaxPendingBytes = 64 << 30
	}
	if cfg.WarnUsedPct <= 0 {
		cfg.WarnUsedPct = 80
	}
	if cfg.CriticalUsedPct <= cfg.WarnUsedPct {
		cfg.CriticalUsedPct = 90
	}
	const minMultipartPart = int64(5 << 20)
	if cfg.MultipartPartSize <= 0 {
		cfg.MultipartPartSize = 8 << 20
	}
	if cfg.MultipartPartSize < minMultipartPart {
		return nil, fmt.Errorf("S3 multipart part size must be at least %d bytes", minMultipartPart)
	}
	if cfg.MultipartThreshold <= 0 {
		cfg.MultipartThreshold = 8 << 20
	}
	if cfg.MultipartAbortAfter <= 0 {
		cfg.MultipartAbortAfter = 6 * time.Hour
	}
	return &Store{Repository: inner, cfg: cfg, s3: c, refs: map[string]string{}}, nil
}
func (s *Store) Backend() string { return "s3" }
func hashID(id string) string    { h := sha256.Sum256([]byte(id)); return hex.EncodeToString(h[:16]) }
func shard(id string) string     { h := sha256.Sum256([]byte(id)); return hex.EncodeToString(h[:1]) }
func (s *Store) key(parts ...string) string {
	p := append([]string{}, parts...)
	if s.cfg.Prefix != "" {
		p = append([]string{s.cfg.Prefix}, p...)
	}
	return path.Join(p...)
}
func (s *Store) pendingKey(id string) string { return s.key("pending", shard(id), hashID(id)+".blob") }
func (s *Store) appliedKey(id string, at time.Time) string {
	return s.key("applied", at.UTC().Format("20060102"), shard(id), hashID(id)+".blob")
}

type objectRef struct {
	Key    string
	SHA256 string
}

func ref(key string, body []byte) string {
	h := sha256.Sum256(body)
	return refPrefixV2 + hex.EncodeToString(h[:]) + ":" + key
}
func parseRef(v string) (objectRef, bool) {
	if strings.HasPrefix(v, refPrefixV2) {
		rest := strings.TrimPrefix(v, refPrefixV2)
		i := strings.IndexByte(rest, ':')
		if i != 64 {
			return objectRef{}, false
		}
		digest, key := rest[:i], rest[i+1:]
		if _, err := hex.DecodeString(digest); err != nil || key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "../") {
			return objectRef{}, false
		}
		return objectRef{Key: key, SHA256: digest}, true
	}
	if strings.HasPrefix(v, refPrefixV1) {
		k := strings.TrimPrefix(v, refPrefixV1)
		if k == "" || strings.HasPrefix(k, "/") || strings.Contains(k, "../") {
			return objectRef{}, false
		}
		return objectRef{Key: k}, true
	}
	return objectRef{}, false
}
func refKey(v string) (string, bool) {
	r, ok := parseRef(v)
	return r.Key, ok
}

func (s *Store) CDCSpoolStorageReady(ctx context.Context) error {
	if err := s.s3.HeadBucket(ctx); err != nil {
		return err
	}
	// HEAD verifies endpoint/authentication but not write permission. Periodically
	// create and remove a tiny object so Kubernetes readiness detects credentials
	// or bucket policies that became read-only without issuing one PUT per probe.
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	if time.Since(s.lastWriteProbe) < time.Minute {
		return nil
	}
	probeKey := s.key(".ready", fmt.Sprintf("probe-%d", time.Now().UnixNano()))
	if err := s.s3.Put(ctx, probeKey, []byte("ok")); err != nil {
		return fmt.Errorf("S3 spool write readiness: %w", err)
	}
	if err := s.s3.Delete(ctx, probeKey); err != nil {
		return fmt.Errorf("S3 spool cleanup readiness: %w", err)
	}
	s.lastWriteProbe = time.Now()
	return nil
}
func (s *Store) MetadataSchemaVersion(ctx context.Context) (string, error) {
	type p interface {
		MetadataSchemaVersion(context.Context) (string, error)
	}
	x, ok := s.Repository.(p)
	if !ok {
		return "", nil
	}
	return x.MetadataSchemaVersion(ctx)
}
func (s *Store) AcquireCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string, ttl time.Duration) (bool, error) {
	type p interface {
		AcquireCDCSpoolDrainLease(context.Context, string, string, string, time.Duration) (bool, error)
	}
	x, ok := s.Repository.(p)
	if !ok {
		return true, nil
	}
	return x.AcquireCDCSpoolDrainLease(ctx, taskID, direction, owner, ttl)
}
func (s *Store) ReleaseCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string) error {
	type p interface {
		ReleaseCDCSpoolDrainLease(context.Context, string, string, string) error
	}
	x, ok := s.Repository.(p)
	if !ok {
		return nil
	}
	return x.ReleaseCDCSpoolDrainLease(ctx, taskID, direction, owner)
}

func (s *Store) CreateCDCSpool(ctx context.Context, v *domain.CDCSpoolRecord) error {
	if v == nil {
		return errors.New("nil CDC spool record")
	}
	if strings.TrimSpace(v.EventsCiphertext) == "" {
		return s.Repository.CreateCDCSpool(ctx, v)
	}
	key := s.pendingKey(v.ID)
	payload := []byte(v.EventsCiphertext)
	if err := s.s3.PutAuto(ctx, key, payload, s.cfg.MultipartThreshold, s.cfg.MultipartPartSize); err != nil {
		return fmt.Errorf("write S3 CDC spool object: %w", err)
	}
	c := *v
	c.EventsCiphertext = ref(key, payload)
	if err := s.Repository.CreateCDCSpool(ctx, &c); err != nil {
		_ = s.s3.Delete(context.Background(), key)
		return err
	}
	s.mu.Lock()
	s.refs[v.ID] = key
	s.mu.Unlock()
	v.Sequence = c.Sequence
	return nil
}
func (s *Store) hydrate(items []domain.CDCSpoolRecord) error {
	for i := range items {
		r, ok := parseRef(items[i].EventsCiphertext)
		if !ok {
			continue
		}
		b, err := s.s3.Get(context.Background(), r.Key)
		if err != nil {
			return err
		}
		if r.SHA256 != "" {
			h := sha256.Sum256(b)
			got := hex.EncodeToString(h[:])
			if !strings.EqualFold(got, r.SHA256) {
				return fmt.Errorf("S3 CDC spool integrity check failed for %s: sha256=%s expected=%s", r.Key, got, r.SHA256)
			}
		}
		items[i].EventsCiphertext = string(b)
		s.mu.Lock()
		s.refs[items[i].ID] = r.Key
		s.mu.Unlock()
	}
	return nil
}
func (s *Store) ListCDCSpool(ctx context.Context, taskID, direction string, n int) ([]domain.CDCSpoolRecord, error) {
	items, err := s.Repository.ListCDCSpool(ctx, taskID, direction, n)
	if err != nil {
		return nil, err
	}
	if err = s.hydrate(items); err != nil {
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
	if err = s.hydrate(items); err != nil {
		return nil, err
	}
	return &items[0], nil
}

func (s *Store) findRef(ctx context.Context, id string) (string, bool) {
	s.mu.Lock()
	k, ok := s.refs[id]
	s.mu.Unlock()
	if ok {
		return k, true
	}
	ms, _ := s.Repository.ListMigrations(ctx)
	for _, m := range ms {
		for _, d := range []string{"forward", "reverse"} {
			rows, e := s.Repository.ListCDCSpool(ctx, m.ID, d, 10000)
			if e != nil {
				continue
			}
			for _, r := range rows {
				if r.ID == id {
					k, ok = refKey(r.EventsCiphertext)
					if ok {
						return k, true
					}
				}
			}
		}
	}
	return "", false
}
func (s *Store) MarkCDCSpoolApplied(ctx context.Context, id string, at time.Time) error {
	key, found := s.findRef(ctx, id)
	if err := s.Repository.MarkCDCSpoolApplied(ctx, id, at); err != nil {
		return err
	}
	if !found {
		return nil
	}
	dst := s.appliedKey(id, at)
	// Target commit + metadata APPLIED is the correctness boundary. Object move is best-effort GC.
	if err := s.s3.Copy(ctx, key, dst); err == nil {
		_ = s.s3.Delete(ctx, key)
	}
	s.mu.Lock()
	delete(s.refs, id)
	s.mu.Unlock()
	return nil
}
func (s *Store) DeleteAppliedCDCSpool(ctx context.Context, taskID, direction string, keep int) error {
	if err := s.Repository.DeleteAppliedCDCSpool(ctx, taskID, direction, keep); err != nil {
		return err
	}
	return s.PurgeApplied(ctx, time.Now())
}
func (s *Store) CDCSpoolStats(ctx context.Context, taskID, direction string) (domain.CDCSpoolStats, error) {
	st, err := s.Repository.CDCSpoolStats(ctx, taskID, direction)
	if err != nil {
		return st, err
	}
	st.StorageBackend = "s3"
	st.StorageCapacityBytes = s.cfg.MaxPendingBytes
	st.StorageUsedBytes = st.PendingBytes
	if st.StorageUsedBytes > st.StorageCapacityBytes {
		st.StorageUsedBytes = st.StorageCapacityBytes
	}
	st.StorageFreeBytes = st.StorageCapacityBytes - st.StorageUsedBytes
	if st.StorageCapacityBytes > 0 {
		st.StorageUsedPct = float64(st.StorageUsedBytes) * 100 / float64(st.StorageCapacityBytes)
	}
	st.StorageLevel = "NORMAL"
	if st.StorageUsedPct >= s.cfg.CriticalUsedPct {
		st.StorageLevel = "CRITICAL"
	} else if st.StorageUsedPct >= s.cfg.WarnUsedPct {
		st.StorageLevel = "WARN"
	}
	return st, nil
}

// PurgeApplied removes encrypted objects that have exceeded the configured
// retention. Correctness does not depend on this cleanup: the target commit
// and metadata APPLIED state are already durable before an object is moved.
func (s *Store) PurgeApplied(ctx context.Context, now time.Time) error {
	prefix := s.key("applied") + "/"
	items, err := s.s3.ListPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	cutoff := now.Add(-s.cfg.AppliedRetention)
	for _, item := range items {
		if !item.LastModified.IsZero() && item.LastModified.Before(cutoff) {
			if err := s.s3.Delete(ctx, item.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

// Reconcile moves unreferenced pending objects to a recovered-orphans prefix.
// It refuses orphan inference when any task has more pending metadata rows than
// the compact Repository listing window, because deleting/moving an object in
// that situation could discard a legitimate transaction.
func (s *Store) Reconcile(ctx context.Context) error {
	// Multipart uploads can survive a worker/server crash before Complete/Abort.
	// Abort only stale uploads under QMigration pending/ so fresh uploads from
	// another live server are never touched. This cleanup is storage hygiene;
	// source ACK correctness still depends on completed object + metadata commit.
	if s.cfg.MultipartAbortAfter > 0 {
		if err := s.s3.AbortStaleMultipartUploads(ctx, s.key("pending")+"/", time.Now().Add(-s.cfg.MultipartAbortAfter)); err != nil {
			return fmt.Errorf("reconcile stale S3 multipart uploads: %w", err)
		}
	}
	referenced := map[string]struct{}{}
	migrations, err := s.Repository.ListMigrations(ctx)
	if err != nil {
		return err
	}
	complete := true
	for _, m := range migrations {
		for _, direction := range []string{"forward", "reverse"} {
			stats, e := s.Repository.CDCSpoolStats(ctx, m.ID, direction)
			if e == nil && stats.PendingTransactions > 10000 {
				complete = false
			}
			rows, e := s.Repository.ListCDCSpool(ctx, m.ID, direction, 10000)
			if e != nil {
				continue
			}
			for _, r := range rows {
				if k, ok := refKey(r.EventsCiphertext); ok {
					referenced[k] = struct{}{}
				}
			}
		}
	}
	if !complete {
		return s.PurgeApplied(ctx, time.Now())
	}
	items, err := s.s3.ListPrefix(ctx, s.key("pending")+"/")
	if err != nil {
		return err
	}
	for _, item := range items {
		if _, ok := referenced[item.Key]; ok {
			continue
		}
		dst := s.key("applied", "recovered-orphans", path.Base(path.Dir(item.Key)), path.Base(item.Key))
		if err := s.s3.Copy(ctx, item.Key, dst); err != nil {
			return err
		}
		if err := s.s3.Delete(ctx, item.Key); err != nil {
			return err
		}
	}
	return s.PurgeApplied(ctx, time.Now())
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
