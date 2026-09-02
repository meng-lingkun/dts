package secure

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/repository"
	"qmigration/backend/internal/security"
	"strings"
	"time"
)

type Store struct {
	inner  repository.Repository
	cipher *security.Cipher
}

var _ repository.ControlOperationLeaser = (*Store)(nil)

func New(inner repository.Repository, cipher *security.Cipher) *Store {
	return &Store{inner: inner, cipher: cipher}
}

func (s *Store) AcquireControlOperation(ctx context.Context, taskID, operation, owner string, lease time.Duration) (bool, error) {
	leaser, ok := s.inner.(repository.ControlOperationLeaser)
	if !ok {
		return false, errors.New("repository does not support control-operation leases")
	}
	return leaser.AcquireControlOperation(ctx, taskID, operation, owner, lease)
}

func (s *Store) RenewControlOperation(ctx context.Context, taskID, operation, owner string, lease time.Duration) error {
	leaser, ok := s.inner.(repository.ControlOperationLeaser)
	if !ok {
		return errors.New("repository does not support control-operation leases")
	}
	return leaser.RenewControlOperation(ctx, taskID, operation, owner, lease)
}

func (s *Store) ReleaseControlOperation(ctx context.Context, taskID, operation, owner string) error {
	leaser, ok := s.inner.(repository.ControlOperationLeaser)
	if !ok {
		return errors.New("repository does not support control-operation leases")
	}
	return leaser.ReleaseControlOperation(ctx, taskID, operation, owner)
}

// MetadataSchemaVersion preserves the optional PostgreSQL repository schema
// health check through the encryption wrapper without extending the core
// Repository interface used by the in-memory store.
func (s *Store) CDCSpoolStorageReady(ctx context.Context) error {
	type provider interface{ CDCSpoolStorageReady(context.Context) error }
	p, ok := s.inner.(provider)
	if !ok {
		return nil
	}
	return p.CDCSpoolStorageReady(ctx)
}

func (s *Store) MetadataSchemaVersion(ctx context.Context) (string, error) {
	type provider interface {
		MetadataSchemaVersion(context.Context) (string, error)
	}
	p, ok := s.inner.(provider)
	if !ok {
		return "", nil
	}
	return p.MetadataSchemaVersion(ctx)
}

func (s *Store) PruneMetadata(ctx context.Context, policy repository.MetadataRetentionPolicy) (repository.MetadataPruneResult, error) {
	m, ok := s.inner.(repository.MetadataMaintenance)
	if !ok {
		return repository.MetadataPruneResult{}, nil
	}
	return m.PruneMetadata(ctx, policy)
}

var _ repository.MetadataMaintenance = (*Store)(nil)

func (s *Store) GetValidationArchive(ctx context.Context, taskID string) (*domain.ValidationArchive, error) {
	p, ok := s.inner.(repository.ValidationArchiveProvider)
	if !ok {
		return nil, nil
	}
	return p.GetValidationArchive(ctx, taskID)
}

func (s *Store) CreateValidationArchive(ctx context.Context, a *domain.ValidationArchive) (bool, error) {
	p, ok := s.inner.(repository.ValidationArchiveProvider)
	if !ok {
		return false, nil
	}
	return p.CreateValidationArchive(ctx, a)
}

func (s *Store) ListValidationEvidencePage(ctx context.Context, taskID, tableID string, afterChunkNo int, afterID string, limit int) ([]repository.ValidationEvidenceRow, error) {
	p, ok := s.inner.(repository.ValidationArchiveProvider)
	if !ok {
		return nil, nil
	}
	return p.ListValidationEvidencePage(ctx, taskID, tableID, afterChunkNo, afterID, limit)
}

func (s *Store) LatestValidationStatusCounts(ctx context.Context, taskID string) (success, mismatch, validationError, missing int, err error) {
	p, ok := s.inner.(repository.ValidationArchiveProvider)
	if !ok {
		return 0, 0, 0, 0, nil
	}
	return p.LatestValidationStatusCounts(ctx, taskID)
}

var _ repository.ValidationArchiveProvider = (*Store)(nil)

func (s *Store) GetValidationReportArchive(ctx context.Context, taskID, evidenceDigest string) (*domain.ValidationReportArchiveRecord, error) {
	p, ok := s.inner.(repository.ValidationReportArchiveProvider)
	if !ok {
		return nil, nil
	}
	return p.GetValidationReportArchive(ctx, taskID, evidenceDigest)
}

func (s *Store) CreateValidationReportArchive(ctx context.Context, a *domain.ValidationReportArchiveRecord) (bool, error) {
	p, ok := s.inner.(repository.ValidationReportArchiveProvider)
	if !ok {
		return false, nil
	}
	return p.CreateValidationReportArchive(ctx, a)
}

var _ repository.ValidationReportArchiveProvider = (*Store)(nil)

func (s *Store) CreateDataSource(ctx context.Context, d *domain.DataSource) error {
	c := *d
	enc, err := s.cipher.Encrypt(d.Password)
	if err != nil {
		return err
	}
	c.Password = ""
	c.PasswordCiphertext = enc
	if d.TLSClientKey != "" {
		keyEnc, e := s.cipher.Encrypt(d.TLSClientKey)
		if e != nil {
			return e
		}
		c.TLSClientKeyCiphertext = keyEnc
	}
	c.TLSClientKey = ""
	return s.inner.CreateDataSource(ctx, &c)
}
func (s *Store) ListDataSources(ctx context.Context) ([]domain.DataSource, error) {
	xs, err := s.inner.ListDataSources(ctx)
	if err != nil {
		return nil, err
	}
	for i := range xs {
		xs[i].Password = ""
		xs[i].TLSClientKey = ""
	}
	return xs, nil
}
func (s *Store) GetDataSource(ctx context.Context, id string) (*domain.DataSource, error) {
	d, err := s.inner.GetDataSource(ctx, id)
	if err != nil {
		return nil, err
	}
	if d.PasswordCiphertext != "" {
		p, e := s.cipher.Decrypt(d.PasswordCiphertext)
		if e != nil {
			return nil, e
		}
		d.Password = p
	}
	if d.TLSClientKeyCiphertext != "" {
		k, e := s.cipher.Decrypt(d.TLSClientKeyCiphertext)
		if e != nil {
			return nil, e
		}
		d.TLSClientKey = k
	}
	return d, nil
}

func (s *Store) CreateMigration(c context.Context, v *domain.MigrationTask) error {
	return s.inner.CreateMigration(c, v)
}
func (s *Store) ListMigrations(c context.Context) ([]domain.MigrationTask, error) {
	return s.inner.ListMigrations(c)
}
func (s *Store) GetMigration(c context.Context, id string) (*domain.MigrationTask, error) {
	return s.inner.GetMigration(c, id)
}
func (s *Store) UpdateMigration(c context.Context, v *domain.MigrationTask) error {
	return s.inner.UpdateMigration(c, v)
}
func (s *Store) CreateMigrationTable(c context.Context, v *domain.MigrationTable) error {
	return s.inner.CreateMigrationTable(c, v)
}
func (s *Store) ListMigrationTables(c context.Context, id string) ([]domain.MigrationTable, error) {
	return s.inner.ListMigrationTables(c, id)
}
func (s *Store) GetMigrationTable(c context.Context, id string) (*domain.MigrationTable, error) {
	return s.inner.GetMigrationTable(c, id)
}
func (s *Store) UpdateMigrationTable(c context.Context, v *domain.MigrationTable) error {
	return s.inner.UpdateMigrationTable(c, v)
}
func (s *Store) FindMigrationTableProfile(c context.Context, sourceID, targetID, schema, table string) (*domain.MigrationTable, error) {
	return s.inner.FindMigrationTableProfile(c, sourceID, targetID, schema, table)
}
func (s *Store) CreateChunks(c context.Context, v []domain.MigrationChunk) error {
	return s.inner.CreateChunks(c, v)
}
func (s *Store) ListChunks(c context.Context, id string) ([]domain.MigrationChunk, error) {
	return s.inner.ListChunks(c, id)
}
func (s *Store) GetChunk(c context.Context, id string) (*domain.MigrationChunk, error) {
	return s.inner.GetChunk(c, id)
}
func (s *Store) UpdateChunk(c context.Context, v *domain.MigrationChunk) error {
	return s.inner.UpdateChunk(c, v)
}
func (s *Store) ClaimChunk(c context.Context, id string, d time.Duration, capabilities []string) (*domain.MigrationChunk, error) {
	return s.inner.ClaimChunk(c, id, d, capabilities)
}
func (s *Store) RenewChunkLease(c context.Context, a, b string, d time.Duration) error {
	return s.inner.RenewChunkLease(c, a, b, d)
}
func (s *Store) UpdateChunkProgress(c context.Context, id, worker string, progress domain.ChunkProgress) error {
	return s.inner.UpdateChunkProgress(c, id, worker, progress)
}
func (s *Store) YieldChunk(c context.Context, worker string, completed *domain.MigrationChunk, created []domain.MigrationChunk) error {
	return s.inner.YieldChunk(c, worker, completed, created)
}
func (s *Store) CreateEngineJob(c context.Context, v *domain.EngineJob) error {
	return s.inner.CreateEngineJob(c, v)
}
func (s *Store) GetEngineJob(c context.Context, id string) (*domain.EngineJob, error) {
	return s.inner.GetEngineJob(c, id)
}
func (s *Store) ListEngineJobs(c context.Context, taskID string) ([]domain.EngineJob, error) {
	return s.inner.ListEngineJobs(c, taskID)
}
func (s *Store) UpdateEngineJob(c context.Context, v *domain.EngineJob) error {
	return s.inner.UpdateEngineJob(c, v)
}
func (s *Store) ClaimEngineJob(c context.Context, worker string, d time.Duration, caps []string) (*domain.EngineJob, error) {
	return s.inner.ClaimEngineJob(c, worker, d, caps)
}
func (s *Store) RenewEngineJobLease(c context.Context, id, worker string, d time.Duration) error {
	return s.inner.RenewEngineJobLease(c, id, worker, d)
}
func (s *Store) UpsertWorker(c context.Context, v *domain.Worker) error {
	return s.inner.UpsertWorker(c, v)
}
func (s *Store) ListWorkers(c context.Context) ([]domain.Worker, error) {
	return s.inner.ListWorkers(c)
}
func (s *Store) GetWorker(c context.Context, id string) (*domain.Worker, error) {
	return s.inner.GetWorker(c, id)
}
func (s *Store) CreateCDCPosition(c context.Context, v *domain.CDCPosition) error {
	return s.inner.CreateCDCPosition(c, v)
}
func (s *Store) ListCDCPositions(c context.Context, id string, n int) ([]domain.CDCPosition, error) {
	return s.inner.ListCDCPositions(c, id, n)
}

func (s *Store) encryptSpool(v *domain.CDCSpoolRecord) (*domain.CDCSpoolRecord, error) {
	c := *v
	c.EventCount = len(v.Events)
	if len(v.Events) > 0 {
		b, err := json.Marshal(v.Events)
		if err != nil {
			return nil, err
		}
		var compressed bytes.Buffer
		zw := gzip.NewWriter(&compressed)
		if _, err := zw.Write(b); err != nil {
			return nil, err
		}
		if err := zw.Close(); err != nil {
			return nil, err
		}
		packed := "gzip64:" + base64.RawStdEncoding.EncodeToString(compressed.Bytes())
		enc, err := s.cipher.Encrypt(packed)
		if err != nil {
			return nil, err
		}
		c.PayloadBytes = int64(len(enc))
		c.EventsCiphertext = enc
		c.Events = nil
	}
	return &c, nil
}

func (s *Store) decryptSpool(v *domain.CDCSpoolRecord) error {
	if v.EventsCiphertext == "" {
		return nil
	}
	raw, err := s.cipher.Decrypt(v.EventsCiphertext)
	if err != nil {
		return err
	}
	payload := []byte(raw)
	if strings.HasPrefix(raw, "gzip64:") {
		packed, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(raw, "gzip64:"))
		if err != nil {
			return err
		}
		zr, err := gzip.NewReader(bytes.NewReader(packed))
		if err != nil {
			return err
		}
		payload, err = io.ReadAll(zr)
		closeErr := zr.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	if err := json.Unmarshal(payload, &v.Events); err != nil {
		return err
	}
	v.EventsCiphertext = ""
	return nil
}

func (s *Store) CreateCDCSpool(c context.Context, v *domain.CDCSpoolRecord) error {
	enc, err := s.encryptSpool(v)
	if err != nil {
		return err
	}
	if err := s.inner.CreateCDCSpool(c, enc); err != nil {
		return err
	}
	v.Sequence = enc.Sequence
	return nil
}

func (s *Store) ListCDCSpool(c context.Context, taskID, direction string, n int) ([]domain.CDCSpoolRecord, error) {
	items, err := s.inner.ListCDCSpool(c, taskID, direction, n)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if err := s.decryptSpool(&items[i]); err != nil {
			return nil, err
		}
	}
	return items, nil
}
func (s *Store) LatestPendingCDCSpool(c context.Context, taskID, direction string) (*domain.CDCSpoolRecord, error) {
	v, err := s.inner.LatestPendingCDCSpool(c, taskID, direction)
	if err != nil {
		return nil, err
	}
	if err := s.decryptSpool(v); err != nil {
		return nil, err
	}
	return v, nil
}
func (s *Store) MarkCDCSpoolApplied(c context.Context, id string, at time.Time) error {
	return s.inner.MarkCDCSpoolApplied(c, id, at)
}
func (s *Store) DeleteAppliedCDCSpool(c context.Context, taskID, direction string, keep int) error {
	return s.inner.DeleteAppliedCDCSpool(c, taskID, direction, keep)
}
func (s *Store) CDCSpoolStats(c context.Context, taskID, direction string) (domain.CDCSpoolStats, error) {
	return s.inner.CDCSpoolStats(c, taskID, direction)
}
func (s *Store) AcquireCDCSpoolDrainLease(c context.Context, taskID, direction, owner string, ttl time.Duration) (bool, error) {
	type provider interface {
		AcquireCDCSpoolDrainLease(context.Context, string, string, string, time.Duration) (bool, error)
	}
	p, ok := s.inner.(provider)
	if !ok {
		return true, nil
	}
	return p.AcquireCDCSpoolDrainLease(c, taskID, direction, owner, ttl)
}
func (s *Store) ReleaseCDCSpoolDrainLease(c context.Context, taskID, direction, owner string) error {
	type provider interface {
		ReleaseCDCSpoolDrainLease(context.Context, string, string, string) error
	}
	p, ok := s.inner.(provider)
	if !ok {
		return nil
	}
	return p.ReleaseCDCSpoolDrainLease(c, taskID, direction, owner)
}

func (s *Store) encryptDeadLetter(v *domain.CDCDeadLetter) (*domain.CDCDeadLetter, error) {
	c := *v
	if len(v.Events) > 0 {
		b, err := json.Marshal(v.Events)
		if err != nil {
			return nil, err
		}
		enc, err := s.cipher.Encrypt(string(b))
		if err != nil {
			return nil, err
		}
		c.EventsCiphertext = enc
		c.Events = nil
	}
	return &c, nil
}
func (s *Store) decryptDeadLetter(v *domain.CDCDeadLetter) error {
	if v.EventsCiphertext == "" {
		return nil
	}
	raw, err := s.cipher.Decrypt(v.EventsCiphertext)
	if err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(raw), &v.Events); err != nil {
		return err
	}
	v.EventsCiphertext = ""
	return nil
}
func (s *Store) CreateCDCDeadLetter(c context.Context, v *domain.CDCDeadLetter) error {
	enc, err := s.encryptDeadLetter(v)
	if err != nil {
		return err
	}
	return s.inner.CreateCDCDeadLetter(c, enc)
}
func (s *Store) UpdateCDCDeadLetter(c context.Context, v *domain.CDCDeadLetter) error {
	enc, err := s.encryptDeadLetter(v)
	if err != nil {
		return err
	}
	return s.inner.UpdateCDCDeadLetter(c, enc)
}
func (s *Store) GetCDCDeadLetter(c context.Context, id string) (*domain.CDCDeadLetter, error) {
	v, err := s.inner.GetCDCDeadLetter(c, id)
	if err != nil {
		return nil, err
	}
	if err := s.decryptDeadLetter(v); err != nil {
		return nil, err
	}
	return v, nil
}
func (s *Store) ListCDCDeadLetters(c context.Context, taskID string) ([]domain.CDCDeadLetter, error) {
	items, err := s.inner.ListCDCDeadLetters(c, taskID)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if err := s.decryptDeadLetter(&items[i]); err != nil {
			return nil, err
		}
	}
	return items, nil
}
func (s *Store) DeleteCDCDeadLetter(c context.Context, id string) error {
	return s.inner.DeleteCDCDeadLetter(c, id)
}
func (s *Store) CreateCDCConflict(c context.Context, v *domain.CDCConflictRecord) error {
	return s.inner.CreateCDCConflict(c, v)
}
func (s *Store) ListCDCConflicts(c context.Context, taskID string, n int) ([]domain.CDCConflictRecord, error) {
	return s.inner.ListCDCConflicts(c, taskID, n)
}
func (s *Store) CreateValidationResult(c context.Context, v *domain.ValidationResult) error {
	return s.inner.CreateValidationResult(c, v)
}
func (s *Store) ListValidationResults(c context.Context, id string) ([]domain.ValidationResult, error) {
	return s.inner.ListValidationResults(c, id)
}
func (s *Store) DeleteValidationResults(c context.Context, id string) error {
	return s.inner.DeleteValidationResults(c, id)
}
func (s *Store) CreateAlert(c context.Context, v *domain.Alert) error {
	return s.inner.CreateAlert(c, v)
}
func (s *Store) ListAlerts(c context.Context) ([]domain.Alert, error) { return s.inner.ListAlerts(c) }
func (s *Store) AcknowledgeAlert(c context.Context, id string) error {
	return s.inner.AcknowledgeAlert(c, id)
}
func (s *Store) CreateAuditEvent(c context.Context, v *domain.AuditEvent) error {
	return s.inner.CreateAuditEvent(c, v)
}
func (s *Store) ListAuditEvents(c context.Context, n int) ([]domain.AuditEvent, error) {
	return s.inner.ListAuditEvents(c, n)
}
func (s *Store) CreateTaskLog(c context.Context, v *domain.TaskLog) error {
	return s.inner.CreateTaskLog(c, v)
}
func (s *Store) ListTaskLogs(c context.Context, taskID string, n int) ([]domain.TaskLog, error) {
	return s.inner.ListTaskLogs(c, taskID, n)
}

var _ repository.Repository = (*Store)(nil)

func (s *Store) UpdateDataSource(ctx context.Context, d *domain.DataSource) error {
	c := *d
	old, err := s.inner.GetDataSource(ctx, d.ID)
	if err != nil {
		return err
	}
	if d.Password != "" {
		enc, e := s.cipher.Encrypt(d.Password)
		if e != nil {
			return e
		}
		c.PasswordCiphertext = enc
	} else {
		c.PasswordCiphertext = old.PasswordCiphertext
	}
	if d.TLSClientKey != "" {
		enc, e := s.cipher.Encrypt(d.TLSClientKey)
		if e != nil {
			return e
		}
		c.TLSClientKeyCiphertext = enc
	} else {
		c.TLSClientKeyCiphertext = old.TLSClientKeyCiphertext
	}
	c.Password = ""
	c.TLSClientKey = ""
	return s.inner.UpdateDataSource(ctx, &c)
}
func (s *Store) DeleteDataSource(ctx context.Context, id string) error {
	return s.inner.DeleteDataSource(ctx, id)
}

func (s *Store) CreateUser(c context.Context, u *domain.User) error { return s.inner.CreateUser(c, u) }
func (s *Store) UpdateUser(c context.Context, u *domain.User) error { return s.inner.UpdateUser(c, u) }
func (s *Store) GetUser(c context.Context, id string) (*domain.User, error) {
	return s.inner.GetUser(c, id)
}
func (s *Store) GetUserByUsername(c context.Context, name string) (*domain.User, error) {
	return s.inner.GetUserByUsername(c, name)
}
func (s *Store) ListUsers(c context.Context) ([]domain.User, error) { return s.inner.ListUsers(c) }

// RC42 preserves optional control-plane aggregation/hot-path capabilities
// through the encryption wrapper. Without these delegates production would
// silently fall back to Repository.ListChunks and re-introduce O(N) scans.
func (s *Store) SummarizeTaskChunks(ctx context.Context, taskID string) (repository.TaskChunkSummary, error) {
	return repository.SummarizeChunks(ctx, s.inner, taskID)
}
func (s *Store) MaxTaskChunkNo(ctx context.Context, taskID string) (int, error) {
	return repository.MaxTaskChunkNo(ctx, s.inner, taskID)
}
func (s *Store) CountTableRunnable(ctx context.Context, taskID, tableID string) (repository.TableRunnableCounts, error) {
	return repository.CountTableRunnable(ctx, s.inner, taskID, tableID)
}
func (s *Store) ListPendingTableChunks(ctx context.Context, taskID, tableID string) ([]domain.MigrationChunk, error) {
	return repository.ListPendingTableChunks(ctx, s.inner, taskID, tableID)
}
func (s *Store) ListRunningTopologyChunks(ctx context.Context, taskID, topologyID string) ([]domain.MigrationChunk, error) {
	return repository.ListRunningTopologyChunks(ctx, s.inner, taskID, topologyID)
}
func (s *Store) ListRunningFaultDomainChunks(ctx context.Context, taskID, scope, value string) ([]domain.MigrationChunk, error) {
	return repository.ListRunningFaultDomainChunks(ctx, s.inner, taskID, scope, value)
}

var _ repository.ChunkSummaryProvider = (*Store)(nil)
var _ repository.ChunkHotPathProvider = (*Store)(nil)

func (s *Store) MetadataStorageStats(ctx context.Context) (repository.MetadataStorageStats, error) {
	return repository.ReadMetadataStorageStats(ctx, s.inner)
}

var _ repository.MetadataStatsProvider = (*Store)(nil)

func (s *Store) ListTableChunksPage(c context.Context, taskID, tableID string, afterChunkNo int, afterID string, limit int) ([]domain.MigrationChunk, error) {
	return repository.ListTableChunksPage(c, s.inner, taskID, tableID, afterChunkNo, afterID, limit)
}
func (s *Store) HasValidationResults(c context.Context, taskID string) (bool, error) {
	return repository.HasValidationResults(c, s.inner, taskID)
}
func (s *Store) FirstInvalidSuccessfulChunk(c context.Context, taskID string) (string, domain.ValidationStatus, error) {
	return repository.FirstInvalidSuccessfulChunk(c, s.inner, taskID)
}
func (s *Store) ListRepairableValidationChunkIDs(c context.Context, taskID string, limit int) ([]string, error) {
	return repository.ListRepairableValidationChunkIDs(c, s.inner, taskID, limit)
}

var _ repository.ValidationHotPathProvider = (*Store)(nil)
