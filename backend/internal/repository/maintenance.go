package repository

import (
	"context"
	"time"
)

// MetadataRetentionPolicy bounds operational history without touching durable
// correctness state such as pending CDC spool records or unresolved DLQs.
// CDC position pruning must always retain the newest checkpoint for every
// task+direction stream, even when it is older than MaxAge.
type MetadataRetentionPolicy struct {
	TaskLogMaxAge               time.Duration
	TaskLogMaxRowsPerTask       int
	AuditMaxAge                 time.Duration
	AuditMaxRows                int
	CDCPositionMaxAge           time.Duration
	CDCPositionMaxRowsPerStream int
	// ValidationMaxAttemptsPerChunk compacts repeated validation/repair attempts
	// while retaining the newest result for every active task+chunk.
	ValidationMaxAttemptsPerChunk int
	// ValidationAttemptMaxAge applies only to superseded attempts (rank > 1).
	// The latest result remains durable regardless of this age limit.
	ValidationAttemptMaxAge time.Duration
	// ValidationTerminalMaxAge allows detailed per-chunk validation history to
	// expire after a task has been terminal for the configured period. Set to 0
	// to retain terminal-task latest results indefinitely.
	ValidationTerminalMaxAge time.Duration
	// ValidationArchivePageSize bounds the heap used while constructing the
	// permanent terminal-task validation proof.
	ValidationArchivePageSize int
	// ValidationArchiveTasksPerRun bounds expensive archive construction per
	// janitor cycle before detail deletion is allowed.
	ValidationArchiveTasksPerRun int
	BatchRows                    int
	MaxBatches                   int
}

type MetadataPruneResult struct {
	TaskLogsDeleted           int64
	AuditEventsDeleted        int64
	CDCPositionsDeleted       int64
	ValidationDeleted         int64
	ValidationArchivesCreated int64
}

func (r MetadataPruneResult) TotalDeleted() int64 {
	return r.TaskLogsDeleted + r.AuditEventsDeleted + r.CDCPositionsDeleted + r.ValidationDeleted
}

// MetadataMaintenance is intentionally optional so external repository wrappers
// and older embedded repositories are not forced to implement retention. The
// server starts the janitor only when the concrete metadata store supports it.
type MetadataMaintenance interface {
	PruneMetadata(context.Context, MetadataRetentionPolicy) (MetadataPruneResult, error)
}
