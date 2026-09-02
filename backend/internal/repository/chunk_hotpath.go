package repository

import (
	"context"
	"qmigration/backend/internal/domain"
)

// TableRunnableCounts is the small control-plane view needed by running chunk
// rebalance. It deliberately excludes completed history so 10-40TB tasks do not
// materialize every chunk during lease renewals.
type TableRunnableCounts struct {
	Pending int
	Running int
}

func (c TableRunnableCounts) Runnable() int { return c.Pending + c.Running }

// ChunkHotPathProvider exposes bounded/index-backed chunk queries used by worker
// lease renewals and adaptive split control. PostgreSQL implements these with
// WHERE/GROUP BY/MAX queries; wrappers must delegate the capability so the
// production Secure/Spool stack does not silently fall back to ListChunks.
type ChunkHotPathProvider interface {
	MaxTaskChunkNo(context.Context, string) (int, error)
	CountTableRunnable(context.Context, string, string) (TableRunnableCounts, error)
	ListPendingTableChunks(context.Context, string, string) ([]domain.MigrationChunk, error)
	ListRunningTopologyChunks(context.Context, string, string) ([]domain.MigrationChunk, error)
	ListRunningFaultDomainChunks(context.Context, string, string, string) ([]domain.MigrationChunk, error)
}

func MaxTaskChunkNo(ctx context.Context, repo Repository, taskID string) (int, error) {
	if p, ok := repo.(ChunkHotPathProvider); ok {
		return p.MaxTaskChunkNo(ctx, taskID)
	}
	chunks, err := repo.ListChunks(ctx, taskID)
	if err != nil {
		return 0, err
	}
	maxNo := 0
	for i := range chunks {
		if chunks[i].ChunkNo > maxNo {
			maxNo = chunks[i].ChunkNo
		}
	}
	return maxNo, nil
}

func CountTableRunnable(ctx context.Context, repo Repository, taskID, tableID string) (TableRunnableCounts, error) {
	if p, ok := repo.(ChunkHotPathProvider); ok {
		return p.CountTableRunnable(ctx, taskID, tableID)
	}
	chunks, err := repo.ListChunks(ctx, taskID)
	if err != nil {
		return TableRunnableCounts{}, err
	}
	var out TableRunnableCounts
	for i := range chunks {
		if chunks[i].TableID != tableID {
			continue
		}
		switch chunks[i].Status {
		case domain.ChunkPending:
			out.Pending++
		case domain.ChunkRunning:
			out.Running++
		}
	}
	return out, nil
}

func ListPendingTableChunks(ctx context.Context, repo Repository, taskID, tableID string) ([]domain.MigrationChunk, error) {
	if p, ok := repo.(ChunkHotPathProvider); ok {
		return p.ListPendingTableChunks(ctx, taskID, tableID)
	}
	chunks, err := repo.ListChunks(ctx, taskID)
	if err != nil {
		return nil, err
	}
	out := make([]domain.MigrationChunk, 0)
	for i := range chunks {
		if chunks[i].TableID == tableID && chunks[i].Status == domain.ChunkPending {
			out = append(out, chunks[i])
		}
	}
	return out, nil
}

func ListRunningTopologyChunks(ctx context.Context, repo Repository, taskID, topologyID string) ([]domain.MigrationChunk, error) {
	if p, ok := repo.(ChunkHotPathProvider); ok {
		return p.ListRunningTopologyChunks(ctx, taskID, topologyID)
	}
	chunks, err := repo.ListChunks(ctx, taskID)
	if err != nil {
		return nil, err
	}
	out := make([]domain.MigrationChunk, 0)
	for i := range chunks {
		if chunks[i].Status == domain.ChunkRunning && chunks[i].TopologyID == topologyID {
			out = append(out, chunks[i])
		}
	}
	return out, nil
}

func ListRunningFaultDomainChunks(ctx context.Context, repo Repository, taskID, scope, value string) ([]domain.MigrationChunk, error) {
	if p, ok := repo.(ChunkHotPathProvider); ok {
		return p.ListRunningFaultDomainChunks(ctx, taskID, scope, value)
	}
	chunks, err := repo.ListChunks(ctx, taskID)
	if err != nil {
		return nil, err
	}
	out := make([]domain.MigrationChunk, 0)
	for i := range chunks {
		if chunks[i].Status == domain.ChunkRunning && chunks[i].FaultDomain[scope] == value {
			out = append(out, chunks[i])
		}
	}
	return out, nil
}
