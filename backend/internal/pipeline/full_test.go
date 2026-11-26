package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestApplyBeforeCheckpointWithBoundedPrefetch(t *testing.T) {
	var mu sync.Mutex
	next := 0
	committed := []string{}
	writes := 0
	r := Runner{
		Read: func(_ context.Context, limit int) (*Batch, error) {
			mu.Lock()
			defer mu.Unlock()
			if next >= 3 {
				return nil, nil
			}
			next++
			return &Batch{Rows: limit, Bytes: int64(limit), Cursor: string(rune('0' + next)), ReadMS: 1}, nil
		},
		Write: func(_ context.Context, b *Batch) (int64, int64, int64, error) {
			writes++
			if writes == 2 {
				return 0, 0, 1, errors.New("sink failed")
			}
			return int64(b.Rows), b.Bytes, 1, nil
		},
		Commit: func(_ context.Context, b *Batch, _ Stats) (Control, error) {
			committed = append(committed, b.Cursor)
			return Control{Level: "NORMAL"}, nil
		},
	}
	_, err := r.Run(context.Background(), Config{InitialBatchRows: 2, BufferBatches: 2})
	if err == nil {
		t.Fatal("expected sink failure")
	}
	if len(committed) != 1 || committed[0] != "1" {
		t.Fatalf("checkpoint advanced past failed sink write: %#v", committed)
	}
}

func TestBackpressureShrinksBatch(t *testing.T) {
	limits := []int{}
	calls := 0
	r := Runner{
		Read: func(_ context.Context, limit int) (*Batch, error) {
			limits = append(limits, limit)
			calls++
			if calls > 5 {
				return nil, nil
			}
			return &Batch{Rows: limit, Bytes: int64(limit), Cursor: "x", ReadMS: 10}, nil
		},
		Write: func(_ context.Context, b *Batch) (int64, int64, int64, error) { return int64(b.Rows), b.Bytes, 10, nil },
		Commit: func(_ context.Context, _ *Batch, _ Stats) (Control, error) {
			return Control{Level: "CRITICAL", MaxBatchRows: 50}, nil
		},
	}
	_, err := r.Run(context.Background(), Config{InitialBatchRows: 200, MinBatchRows: 50, BufferBatches: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(limits) < 4 {
		t.Fatalf("limits=%v", limits)
	}
	// The bounded reader may have one queued and one in-flight read using the
	// previous batch size. Later reads must converge to the checkpoint control.
	if limits[len(limits)-1] > 50 {
		t.Fatalf("backpressure not applied: %v", limits)
	}
}

func TestTransformRunsBetweenReadAndWrite(t *testing.T) {
	read := false
	transformed := false
	written := false
	r := Runner{
		Read: func(_ context.Context, limit int) (*Batch, error) {
			if read {
				return nil, nil
			}
			read = true
			return &Batch{Rows: 1, Bytes: 1, Payload: "source"}, nil
		},
		Transform: func(_ context.Context, b *Batch) (*Batch, error) {
			if b.Payload != "source" {
				t.Fatalf("unexpected source payload: %#v", b.Payload)
			}
			out := *b
			out.Payload = "normalized"
			transformed = true
			return &out, nil
		},
		Write: func(_ context.Context, b *Batch) (int64, int64, int64, error) {
			if !transformed || b.Payload != "normalized" {
				t.Fatalf("writer saw untransformed payload: %#v", b.Payload)
			}
			written = true
			return 1, 1, 1, nil
		},
		Commit: func(_ context.Context, _ *Batch, _ Stats) (Control, error) {
			if !written {
				t.Fatal("checkpoint ran before writer")
			}
			return Control{Level: "NORMAL"}, nil
		},
	}
	stats, err := r.Run(context.Background(), Config{InitialBatchRows: 1, BufferBatches: 1})
	if err != nil {
		t.Fatal(err)
	}
	if stats.RowsWritten != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestControlPlaneTargetBatchRowsOverridesLocalDoubling(t *testing.T) {
	limits := []int{}
	calls := 0
	r := Runner{
		Read: func(_ context.Context, limit int) (*Batch, error) {
			limits = append(limits, limit)
			calls++
			if calls > 6 {
				return nil, nil
			}
			return &Batch{Rows: limit, Bytes: int64(limit), Cursor: "x", ReadMS: 10}, nil
		},
		Write: func(_ context.Context, b *Batch) (int64, int64, int64, error) { return int64(b.Rows), b.Bytes, 10, nil },
		Commit: func(_ context.Context, _ *Batch, _ Stats) (Control, error) {
			return Control{Level: "NORMAL", TargetBatchRows: 125}, nil
		},
	}
	_, err := r.Run(context.Background(), Config{InitialBatchRows: 500, MinBatchRows: 50, MaxBatchRows: 5000, BufferBatches: 1, Adaptive: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(limits) < 5 || limits[len(limits)-1] != 125 {
		t.Fatalf("control-plane target batch did not converge: %v", limits)
	}
}

func TestTargetRatePause(t *testing.T) {
	if got := targetRatePause(10<<20, 10<<20, 250*time.Millisecond); got < 740*time.Millisecond || got > 760*time.Millisecond {
		t.Fatalf("targetRatePause=%s want about 750ms", got)
	}
	if got := targetRatePause(10<<20, 10<<20, 2*time.Second); got != 0 {
		t.Fatalf("already-slow batch should not pause, got %s", got)
	}
}

func TestStopAfterCommitYieldsOnlyAtDurableBatchBoundary(t *testing.T) {
	reads, writes, commits := 0, 0, 0
	r := Runner{
		Read: func(_ context.Context, limit int) (*Batch, error) {
			reads++
			if reads > 3 {
				return nil, nil
			}
			return &Batch{Rows: 50, Bytes: 50, Cursor: fmt.Sprintf("%d", reads)}, nil
		},
		Write: func(_ context.Context, b *Batch) (int64, int64, int64, error) {
			writes++
			return int64(b.Rows), b.Bytes, 1, nil
		},
		Commit: func(_ context.Context, _ *Batch, _ Stats) (Control, error) {
			commits++
			return Control{StopAfterCommit: true}, nil
		},
	}
	stats, err := r.Run(context.Background(), Config{InitialBatchRows: 50, MinBatchRows: 50, BufferBatches: 2})
	if err != nil {
		t.Fatal(err)
	}
	if writes != 1 || commits != 1 || stats.RowsWritten <= 0 {
		t.Fatalf("writes=%d commits=%d stats=%+v", writes, commits, stats)
	}
}

func TestShortPagesContinueUntilSourceIsExhausted(t *testing.T) {
	pages := []int{2, 1, 2}
	var cursor, applied int
	r := Runner{
		Read: func(context.Context, int) (*Batch, error) {
			if cursor == len(pages) {
				return nil, nil
			}
			rows := pages[cursor]
			cursor++
			return &Batch{Rows: rows}, nil
		},
		Write: func(_ context.Context, b *Batch) (int64, int64, int64, error) {
			return int64(b.Rows), 0, 0, nil
		},
		Commit: func(_ context.Context, b *Batch, _ Stats) (Control, error) {
			applied += b.Rows
			return Control{}, nil
		},
	}
	stats, err := r.Run(context.Background(), Config{InitialBatchRows: 50})
	if err != nil || stats.RowsWritten != 5 || applied != 5 {
		t.Fatalf("wrote %d, checkpointed %d, err=%v", stats.RowsWritten, applied, err)
	}
}

func TestRunJoinsPrefetchOnWriteFailure(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	first := true
	fault := errors.New("write failed")
	r := Runner{
		Read: func(ctx context.Context, _ int) (*Batch, error) {
			if first {
				first = false
				return &Batch{Rows: 1}, nil
			}
			close(started)
			<-ctx.Done()
			close(stopped)
			return nil, ctx.Err()
		},
		Write: func(context.Context, *Batch) (int64, int64, int64, error) {
			<-started
			return 0, 0, 0, fault
		},
		Commit: func(context.Context, *Batch, Stats) (Control, error) {
			t.Fatal("checkpointed a failed write")
			return Control{}, nil
		},
	}
	_, err := r.Run(context.Background(), Config{})
	if !errors.Is(err, fault) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("source reader was still running on return")
	}
}

func TestCancellationAtEndOfReadIsNotSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := Runner{
		Read: func(context.Context, int) (*Batch, error) { cancel(); return nil, nil },
		Write: func(context.Context, *Batch) (int64, int64, int64, error) {
			t.Fatal("unexpected write")
			return 0, 0, 0, nil
		},
		Commit: func(context.Context, *Batch, Stats) (Control, error) {
			t.Fatal("unexpected commit")
			return Control{}, nil
		},
	}
	if _, err := r.Run(ctx, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
