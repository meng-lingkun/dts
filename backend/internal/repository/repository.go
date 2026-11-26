package repository

import (
	"context"
	"dts/backend/internal/domain"
	"errors"
	"time"
)

var ErrNoChunk = errors.New("no chunk available")
var ErrNotFound = errors.New("not found")
var ErrLeaseOwner = errors.New("chunk lease owner mismatch")
var ErrConflict = errors.New("resource revision changed; reload before retrying")
var ErrLastAdmin = errors.New("at least one enabled admin user is required")
var ErrTokenLimit = errors.New("at most 100 active personal API tokens per user")

const MaxPersonalTokens = 100

// ControlOperationLeaser serializes long-running control-plane operations
// across server replicas. Implementations must make acquisition atomic and
// allow a lease to be reclaimed after LeaseUntil expires.
type ControlOperationLeaser interface {
	AcquireControlOperation(context.Context, string, string, string, time.Duration) (bool, error)
	RenewControlOperation(context.Context, string, string, string, time.Duration) error
	ReleaseControlOperation(context.Context, string, string, string) error
}

// CDCExecutionLeaser coordinates target apply, spool drain, replay and validation
// barriers across servers. Each acquisition must use a fresh owner token. Renew
// must reject expired leases even when the owner has not changed.
type CDCExecutionLeaser interface {
	CDCExecutionEpoch(context.Context, string, string, string) (int64, error)
	AcquireCDCSpoolDrainLease(context.Context, string, string, string, time.Duration) (bool, error)
	RenewCDCSpoolDrainLease(context.Context, string, string, string, time.Duration) error
	ReleaseCDCSpoolDrainLease(context.Context, string, string, string) error
}

type Repository interface {
	CreateDataSource(context.Context, *domain.DataSource) error
	UpdateDataSource(context.Context, *domain.DataSource) error
	DeleteDataSource(context.Context, string) error
	ListDataSources(context.Context) ([]domain.DataSource, error)
	GetDataSource(context.Context, string) (*domain.DataSource, error)

	CreateMigration(context.Context, *domain.MigrationTask) error
	ListMigrations(context.Context) ([]domain.MigrationTask, error)
	GetMigration(context.Context, string) (*domain.MigrationTask, error)
	UpdateMigration(context.Context, *domain.MigrationTask) error

	CreateMigrationTable(context.Context, *domain.MigrationTable) error
	ListMigrationTables(context.Context, string) ([]domain.MigrationTable, error)
	GetMigrationTable(context.Context, string) (*domain.MigrationTable, error)
	UpdateMigrationTable(context.Context, *domain.MigrationTable) error
	FindMigrationTableProfile(context.Context, string, string, string, string) (*domain.MigrationTable, error)

	CreateChunks(context.Context, []domain.MigrationChunk) error
	// SplitPendingChunk replaces expected with an entire partition, atomically.
	// The first piece retains the existing ID. A moved snapshot returns ErrConflict.
	SplitPendingChunk(context.Context, domain.MigrationChunk, []domain.MigrationChunk) error
	NumericRangeCoverage(context.Context, domain.MigrationTable) (bool, error)
	ListChunks(context.Context, string) ([]domain.MigrationChunk, error)
	GetChunk(context.Context, string) (*domain.MigrationChunk, error)
	UpdateChunk(context.Context, *domain.MigrationChunk) error
	UpdateOwnedChunk(context.Context, string, *domain.MigrationChunk) error
	ClaimChunk(context.Context, string, time.Duration, []string) (*domain.MigrationChunk, error)
	RenewChunkLease(context.Context, string, string, time.Duration) error
	UpdateChunkProgress(context.Context, string, string, domain.ChunkProgress) error
	YieldChunk(context.Context, string, *domain.MigrationChunk, []domain.MigrationChunk) error

	CreateEngineJob(context.Context, *domain.EngineJob) error
	GetEngineJob(context.Context, string) (*domain.EngineJob, error)
	ListEngineJobs(context.Context, string) ([]domain.EngineJob, error)
	UpdateEngineJob(context.Context, *domain.EngineJob) error
	UpdateOwnedEngineJob(context.Context, string, *domain.EngineJob) error
	ClaimEngineJob(context.Context, string, time.Duration, []string) (*domain.EngineJob, error)
	RenewEngineJobLease(context.Context, string, string, time.Duration) error

	UpsertWorker(context.Context, *domain.Worker) error
	ListWorkers(context.Context) ([]domain.Worker, error)
	GetWorker(context.Context, string) (*domain.Worker, error)

	CreateCDCPosition(context.Context, *domain.CDCPosition) error
	ListCDCPositions(context.Context, string, int) ([]domain.CDCPosition, error)
	// Latest lookups return (nil, nil) when the stream has no matching record.
	LatestCDCPosition(context.Context, string, string) (*domain.CDCPosition, error)
	CreateCDCSpool(context.Context, *domain.CDCSpoolRecord) error
	ListCDCSpool(context.Context, string, string, int) ([]domain.CDCSpoolRecord, error)
	LatestPendingCDCSpool(context.Context, string, string) (*domain.CDCSpoolRecord, error)
	MarkCDCSpoolApplied(context.Context, string, time.Time) error
	DeleteAppliedCDCSpool(context.Context, string, string, int) error
	CDCSpoolStats(context.Context, string, string) (domain.CDCSpoolStats, error)
	CreateCDCDeadLetter(context.Context, *domain.CDCDeadLetter) error
	UpdateCDCDeadLetter(context.Context, *domain.CDCDeadLetter) error
	GetCDCDeadLetter(context.Context, string) (*domain.CDCDeadLetter, error)
	ListCDCDeadLetters(context.Context, string) ([]domain.CDCDeadLetter, error)
	DeleteCDCDeadLetter(context.Context, string) error
	CreateCDCConflict(context.Context, *domain.CDCConflictRecord) error
	ListCDCConflicts(context.Context, string, int) ([]domain.CDCConflictRecord, error)

	CreateValidationResult(context.Context, *domain.ValidationResult) error
	ListValidationResults(context.Context, string) ([]domain.ValidationResult, error)
	DeleteValidationResults(context.Context, string) error

	CreateAlert(context.Context, *domain.Alert) error
	ListAlerts(context.Context) ([]domain.Alert, error)
	AcknowledgeAlert(context.Context, string) error

	CreateAuditEvent(context.Context, *domain.AuditEvent) error
	ListAuditEvents(context.Context, int) ([]domain.AuditEvent, error)
	CreateTaskLog(context.Context, *domain.TaskLog) error
	ListTaskLogs(context.Context, string, int) ([]domain.TaskLog, error)

	CreateUser(context.Context, *domain.User) error
	// CreateInitialUser commits only while the shared user registry is empty.
	CreateInitialUser(context.Context, *domain.User) error
	UpdateUser(context.Context, *domain.User) error
	UpdateUserLastLogin(context.Context, string, time.Time) error
	GetUser(context.Context, string) (*domain.User, error)
	GetUserByUsername(context.Context, string) (*domain.User, error)
	ListUsers(context.Context) ([]domain.User, error)
	RecordSession(context.Context, domain.UserSession) error
	SessionState(ctx context.Context, userID, id string) (exists, active bool, err error)
	RevokeSession(ctx context.Context, userID, id string) (bool, error)
	ListSessions(context.Context, string) ([]domain.UserSession, error)
	CreatePersonalToken(ctx context.Context, v domain.PersonalToken, userRevision int64) error
	PersonalTokenUser(ctx context.Context, id string) (*domain.User, error)
	ListPersonalTokens(ctx context.Context, userID string) ([]domain.PersonalToken, error)
	DeletePersonalToken(ctx context.Context, userID, id string) (bool, error)
}
