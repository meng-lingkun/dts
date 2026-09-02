package repository

import (
	"context"
	"errors"
	"qmigration/backend/internal/domain"
	"time"
)

var ErrNoChunk = errors.New("no chunk available")
var ErrLeaseOwner = errors.New("chunk lease owner mismatch")

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
	ListChunks(context.Context, string) ([]domain.MigrationChunk, error)
	GetChunk(context.Context, string) (*domain.MigrationChunk, error)
	UpdateChunk(context.Context, *domain.MigrationChunk) error
	ClaimChunk(context.Context, string, time.Duration, []string) (*domain.MigrationChunk, error)
	RenewChunkLease(context.Context, string, string, time.Duration) error
	UpdateChunkProgress(context.Context, string, string, domain.ChunkProgress) error
	YieldChunk(context.Context, string, *domain.MigrationChunk, []domain.MigrationChunk) error

	CreateEngineJob(context.Context, *domain.EngineJob) error
	GetEngineJob(context.Context, string) (*domain.EngineJob, error)
	ListEngineJobs(context.Context, string) ([]domain.EngineJob, error)
	UpdateEngineJob(context.Context, *domain.EngineJob) error
	ClaimEngineJob(context.Context, string, time.Duration, []string) (*domain.EngineJob, error)
	RenewEngineJobLease(context.Context, string, string, time.Duration) error

	UpsertWorker(context.Context, *domain.Worker) error
	ListWorkers(context.Context) ([]domain.Worker, error)
	GetWorker(context.Context, string) (*domain.Worker, error)

	CreateCDCPosition(context.Context, *domain.CDCPosition) error
	ListCDCPositions(context.Context, string, int) ([]domain.CDCPosition, error)
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
	UpdateUser(context.Context, *domain.User) error
	GetUser(context.Context, string) (*domain.User, error)
	GetUserByUsername(context.Context, string) (*domain.User, error)
	ListUsers(context.Context) ([]domain.User, error)
}
