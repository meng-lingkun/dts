package maintenance

import (
	"context"
	"log"
	"os"
	"qmigration/backend/internal/repository"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Config struct {
	Enabled  bool
	Interval time.Duration
	Policy   repository.MetadataRetentionPolicy
}

type Snapshot struct {
	Runs                      int64
	Failures                  int64
	LastSuccessUnix           int64
	TaskLogsDeleted           int64
	AuditEventsDeleted        int64
	CDCPositionsDeleted       int64
	ValidationDeleted         int64
	ValidationArchivesCreated int64
}

var stats struct {
	runs, failures, lastSuccess    atomic.Int64
	taskLogs, audits, cdcPositions atomic.Int64
	validations                    atomic.Int64
	validationArchives             atomic.Int64
}

func Current() Snapshot {
	return Snapshot{
		Runs: stats.runs.Load(), Failures: stats.failures.Load(), LastSuccessUnix: stats.lastSuccess.Load(),
		TaskLogsDeleted: stats.taskLogs.Load(), AuditEventsDeleted: stats.audits.Load(), CDCPositionsDeleted: stats.cdcPositions.Load(), ValidationDeleted: stats.validations.Load(), ValidationArchivesCreated: stats.validationArchives.Load(),
	}
}

func boolEnv(name string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return v
}
func intEnv(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return fallback
	}
	return v
}

func ConfigFromEnv() Config {
	return Config{
		Enabled:  boolEnv("QMIGRATION_METADATA_MAINTENANCE_ENABLED", true),
		Interval: time.Duration(intEnv("QMIGRATION_METADATA_MAINTENANCE_INTERVAL_MINUTES", 10)) * time.Minute,
		Policy: repository.MetadataRetentionPolicy{
			TaskLogMaxAge:                 time.Duration(intEnv("QMIGRATION_TASK_LOG_RETENTION_HOURS", 168)) * time.Hour,
			TaskLogMaxRowsPerTask:         intEnv("QMIGRATION_TASK_LOG_MAX_ROWS_PER_TASK", 20000),
			AuditMaxAge:                   time.Duration(intEnv("QMIGRATION_AUDIT_RETENTION_HOURS", 2160)) * time.Hour,
			AuditMaxRows:                  intEnv("QMIGRATION_AUDIT_MAX_ROWS", 100000),
			CDCPositionMaxAge:             time.Duration(intEnv("QMIGRATION_CDC_POSITION_RETENTION_HOURS", 168)) * time.Hour,
			CDCPositionMaxRowsPerStream:   intEnv("QMIGRATION_CDC_POSITION_MAX_ROWS_PER_STREAM", 4096),
			ValidationMaxAttemptsPerChunk: intEnv("QMIGRATION_VALIDATION_MAX_ATTEMPTS_PER_CHUNK", 1),
			ValidationAttemptMaxAge:       time.Duration(intEnv("QMIGRATION_VALIDATION_ATTEMPT_RETENTION_HOURS", 24)) * time.Hour,
			ValidationTerminalMaxAge:      time.Duration(intEnv("QMIGRATION_VALIDATION_TERMINAL_RETENTION_HOURS", 2160)) * time.Hour,
			ValidationArchivePageSize:     intEnv("QMIGRATION_VALIDATION_ARCHIVE_PAGE_SIZE", 512),
			ValidationArchiveTasksPerRun:  intEnv("QMIGRATION_VALIDATION_ARCHIVE_TASKS_PER_RUN", 8),
			BatchRows:                     intEnv("QMIGRATION_METADATA_PRUNE_BATCH_ROWS", 5000),
			MaxBatches:                    intEnv("QMIGRATION_METADATA_PRUNE_MAX_BATCHES", 4),
		},
	}
}

func normalize(cfg Config) Config {
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Minute
	}
	if cfg.Policy.BatchRows <= 0 {
		cfg.Policy.BatchRows = 5000
	}
	if cfg.Policy.MaxBatches <= 0 {
		cfg.Policy.MaxBatches = 4
	}
	return cfg
}

// Start launches a bounded metadata janitor. It runs once immediately so a
// server restart can reclaim accumulated history, then at the configured cadence.
// PostgreSQL prune implementations are batch bounded to avoid long delete locks.
func Start(ctx context.Context, repo repository.Repository, cfg Config) {
	m, ok := repo.(repository.MetadataMaintenance)
	if !ok || !cfg.Enabled {
		return
	}
	cfg = normalize(cfg)
	go func() {
		run := func() {
			stats.runs.Add(1)
			runCtx, cancel := context.WithTimeout(ctx, minDuration(cfg.Interval/2, 2*time.Minute))
			defer cancel()
			res, err := m.PruneMetadata(runCtx, cfg.Policy)
			if err != nil {
				stats.failures.Add(1)
				log.Printf("metadata maintenance failed: %v", err)
				return
			}
			stats.lastSuccess.Store(time.Now().Unix())
			stats.taskLogs.Add(res.TaskLogsDeleted)
			stats.audits.Add(res.AuditEventsDeleted)
			stats.cdcPositions.Add(res.CDCPositionsDeleted)
			stats.validations.Add(res.ValidationDeleted)
			stats.validationArchives.Add(res.ValidationArchivesCreated)
			if res.TotalDeleted() > 0 || res.ValidationArchivesCreated > 0 {
				log.Printf("metadata maintenance pruned task_logs=%d audit_events=%d cdc_positions=%d validation_results=%d validation_archives_created=%d", res.TaskLogsDeleted, res.AuditEventsDeleted, res.CDCPositionsDeleted, res.ValidationDeleted, res.ValidationArchivesCreated)
			}
		}
		run()
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				run()
			}
		}
	}()
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 || a > b {
		return b
	}
	return a
}
