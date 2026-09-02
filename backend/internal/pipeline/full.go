// Package pipeline contains QMigration's built-in data-plane runtime.
//
// The design intentionally fuses the useful execution ideas of mature data
// migration/streaming systems without depending on their runtimes:
//   - staged Reader -> Channel -> Transformer -> Writer execution;
//   - bounded queues for natural backpressure;
//   - adaptive batch sizing;
//   - apply-before-checkpoint durability;
//   - cancellation-safe prefetch (uncommitted prefetched data is discarded).
package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// Batch is an immutable unit produced by a Reader. Cursor is opaque to the
// pipeline; the connector/worker encodes a vendor-independent durable cursor.
type Batch struct {
	Rows          int
	Bytes         int64
	RequestedRows int
	Cursor        string
	ReadMS        int64
	Payload       any
}

// Stats are cumulative and are reported only after a sink write succeeds.
type Stats struct {
	RowsRead      int64
	RowsWritten   int64
	BytesRead     int64
	BytesWritten  int64
	LastReadMS    int64
	LastWriteMS   int64
	LastBatchRows int
}

// Control is returned by the control plane after a durable checkpoint update.
// It is deliberately small enough to be interpreted by every connector.
type Control struct {
	Level             string
	MaxBatchRows      int
	TargetBatchRows   int
	TargetBytesPerSec int64
	Pause             time.Duration
	StopAfterCommit   bool
}

type ReadFunc func(context.Context, int) (*Batch, error)
type TransformFunc func(context.Context, *Batch) (*Batch, error)
type WriteFunc func(context.Context, *Batch) (rowsWritten int64, bytesWritten int64, writeMS int64, err error)
type CommitFunc func(context.Context, *Batch, Stats) (Control, error)

type Config struct {
	InitialBatchRows         int
	MinBatchRows             int
	MaxBatchRows             int
	BufferBatches            int
	Adaptive                 bool
	InitialStats             Stats
	InitialTargetBytesPerSec int64
}

type Runner struct {
	Read      ReadFunc
	Transform TransformFunc
	Write     WriteFunc
	Commit    CommitFunc
}

type readResult struct {
	batch *Batch
	err   error
}

func normalizeConfig(c Config) Config {
	if c.InitialBatchRows <= 0 {
		c.InitialBatchRows = 500
	}
	if c.MinBatchRows <= 0 {
		c.MinBatchRows = 50
	}
	if c.MaxBatchRows <= 0 {
		c.MaxBatchRows = 5000
	}
	if c.MinBatchRows > c.MaxBatchRows {
		c.MinBatchRows = c.MaxBatchRows
	}
	if c.InitialBatchRows < c.MinBatchRows {
		c.InitialBatchRows = c.MinBatchRows
	}
	if c.InitialBatchRows > c.MaxBatchRows {
		c.InitialBatchRows = c.MaxBatchRows
	}
	if c.BufferBatches <= 0 {
		c.BufferBatches = 2
	}
	if c.BufferBatches > 32 {
		c.BufferBatches = 32
	}
	return c
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// targetRatePause returns the additional delay required to keep one worker
// at or below its assigned byte budget. The control plane divides a task-global
// throughput target by effective parallelism, so independently pacing workers
// converges on the task target without a shared hot-path token bucket.
func targetRatePause(bytesWritten, targetBytesPerSec int64, elapsed time.Duration) time.Duration {
	if bytesWritten <= 0 || targetBytesPerSec <= 0 {
		return 0
	}
	target := time.Duration(float64(bytesWritten) / float64(targetBytesPerSec) * float64(time.Second))
	if target <= elapsed {
		return 0
	}
	return target - elapsed
}

// Run executes one migration chunk. The reader may prefetch a small number of
// batches, but Commit is invoked strictly after Write succeeds. Therefore a
// crash can replay prefetched data, never skip it.
func (r Runner) Run(parent context.Context, cfg Config) (Stats, error) {
	if r.Read == nil || r.Write == nil || r.Commit == nil {
		return cfg.InitialStats, errors.New("pipeline requires read, write and commit hooks")
	}
	cfg = normalizeConfig(cfg)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var batchRows atomic.Int64
	batchRows.Store(int64(cfg.InitialBatchRows))
	var targetBytesPerSec atomic.Int64
	targetBytesPerSec.Store(cfg.InitialTargetBytesPerSec)
	ch := make(chan readResult, cfg.BufferBatches)

	go func() {
		defer close(ch)
		for {
			limit := clamp(int(batchRows.Load()), cfg.MinBatchRows, cfg.MaxBatchRows)
			b, err := r.Read(ctx, limit)
			if err != nil {
				select {
				case ch <- readResult{err: err}:
				case <-ctx.Done():
				}
				return
			}
			if b == nil || b.Rows == 0 {
				return
			}
			b.RequestedRows = limit
			select {
			case ch <- readResult{batch: b}:
			case <-ctx.Done():
				return
			}
			if b.Rows < limit {
				return
			}
		}
	}()

	stats := cfg.InitialStats
	for item := range ch {
		if item.err != nil {
			return stats, item.err
		}
		b := item.batch
		batchStarted := time.Now()
		if r.Transform != nil {
			var err error
			b, err = r.Transform(ctx, b)
			if err != nil {
				return stats, err
			}
			if b == nil {
				return stats, errors.New("transform returned nil batch")
			}
		}
		rowsWritten, bytesWritten, writeMS, err := r.Write(ctx, b)
		if err != nil {
			return stats, err
		}

		stats.RowsRead += int64(b.Rows)
		stats.RowsWritten += rowsWritten
		stats.BytesRead += b.Bytes
		stats.BytesWritten += bytesWritten
		stats.LastReadMS = b.ReadMS
		stats.LastWriteMS = writeMS
		stats.LastBatchRows = b.Rows

		control, err := r.Commit(ctx, b, stats)
		if err != nil {
			return stats, err
		}
		targetBytesPerSec.Store(control.TargetBytesPerSec)
		if control.StopAfterCommit {
			// The just-written batch has already been durably checkpointed.
			cancel()
			return stats, nil
		}

		current := clamp(int(batchRows.Load()), cfg.MinBatchRows, cfg.MaxBatchRows)
		if control.TargetBatchRows > 0 {
			current = clamp(control.TargetBatchRows, cfg.MinBatchRows, cfg.MaxBatchRows)
		}
		if control.MaxBatchRows > 0 && current > control.MaxBatchRows {
			current = control.MaxBatchRows
		}
		backpressured := control.Level == "WARN" || control.Level == "CRITICAL"
		if cfg.Adaptive && control.TargetBatchRows <= 0 && !backpressured {
			totalMS := b.ReadMS + writeMS
			switch {
			case b.Rows >= b.RequestedRows && totalMS > 0 && totalMS < 1500 && current < cfg.MaxBatchRows:
				current *= 2
			case totalMS > 8000 && current > cfg.MinBatchRows:
				current /= 2
			}
		}
		batchRows.Store(int64(clamp(current, cfg.MinBatchRows, cfg.MaxBatchRows)))

		pause := control.Pause + targetRatePause(bytesWritten, targetBytesPerSec.Load(), time.Since(batchStarted))
		if pause > 0 {
			timer := time.NewTimer(pause)
			select {
			case <-ctx.Done():
				timer.Stop()
				return stats, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return stats, nil
}
