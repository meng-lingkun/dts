package repository

import (
	"context"
	"qmigration/backend/internal/domain"
)

type ChunkTableSummary struct {
	Total          int
	Success        int
	Pending        int
	Running        int
	Failed         int
	RowsWritten    int64
	BytesWritten   int64
	ReadMS         int64
	WriteMS        int64
	LatencySamples int64
}

type TaskChunkSummary struct {
	Total          int
	Success        int
	Pending        int
	Running        int
	Failed         int
	RowsWritten    int64
	BytesWritten   int64
	ReadMS         int64
	WriteMS        int64
	LatencySamples int64
	Tables         map[string]ChunkTableSummary
}

// ChunkSummaryProvider lets hot control-plane paths aggregate at the repository
// boundary instead of materializing every chunk. PostgreSQL returns O(tables)
// rows even when a 40TB task has tens of thousands of chunks.
type ChunkSummaryProvider interface {
	SummarizeTaskChunks(context.Context, string) (TaskChunkSummary, error)
}

func SummarizeChunks(ctx context.Context, repo Repository, taskID string) (TaskChunkSummary, error) {
	if p, ok := repo.(ChunkSummaryProvider); ok {
		return p.SummarizeTaskChunks(ctx, taskID)
	}
	chunks, err := repo.ListChunks(ctx, taskID)
	if err != nil {
		return TaskChunkSummary{}, err
	}
	out := TaskChunkSummary{Tables: map[string]ChunkTableSummary{}}
	for _, c := range chunks {
		t := out.Tables[c.TableID]
		out.Total++
		t.Total++
		switch c.Status {
		case domain.ChunkSuccess:
			out.Success++
			t.Success++
			out.RowsWritten += c.RowsWritten
			t.RowsWritten += c.RowsWritten
			out.BytesWritten += c.BytesWritten
			t.BytesWritten += c.BytesWritten
		case domain.ChunkPending:
			out.Pending++
			t.Pending++
		case domain.ChunkRunning:
			out.Running++
			t.Running++
		case domain.ChunkFailed:
			out.Failed++
			t.Failed++
		}
		out.ReadMS += c.LastReadMS
		t.ReadMS += c.LastReadMS
		out.WriteMS += c.LastWriteMS
		t.WriteMS += c.LastWriteMS
		if c.LastReadMS > 0 {
			out.LatencySamples++
			t.LatencySamples++
		}
		out.Tables[c.TableID] = t
	}
	return out, nil
}
