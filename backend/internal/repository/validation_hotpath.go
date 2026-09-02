package repository

import (
	"context"
	"sort"

	"qmigration/backend/internal/domain"
)

// ValidationHotPathProvider keeps large validation tasks bounded. Implementations
// should use indexes/EXISTS/lateral latest-result queries instead of materializing
// every chunk/result in the control plane.
type ValidationChunkPageProvider interface {
	ListTableChunksPage(context.Context, string, string, int, string, int) ([]domain.MigrationChunk, error)
}

type ValidationHotPathProvider interface {
	ValidationChunkPageProvider
	HasValidationResults(context.Context, string) (bool, error)
	FirstInvalidSuccessfulChunk(context.Context, string) (string, domain.ValidationStatus, error)
	ListRepairableValidationChunkIDs(context.Context, string, int) ([]string, error)
}

func ListTableChunksPage(ctx context.Context, repo Repository, taskID, tableID string, afterChunkNo int, afterID string, limit int) ([]domain.MigrationChunk, error) {
	if p, ok := repo.(ValidationChunkPageProvider); ok {
		return p.ListTableChunksPage(ctx, taskID, tableID, afterChunkNo, afterID, limit)
	}
	chunks, err := repo.ListChunks(ctx, taskID)
	if err != nil {
		return nil, err
	}
	out := make([]domain.MigrationChunk, 0)
	for _, ch := range chunks {
		if ch.TableID != tableID {
			continue
		}
		if ch.ChunkNo < afterChunkNo || (ch.ChunkNo == afterChunkNo && ch.ID <= afterID) {
			continue
		}
		out = append(out, ch)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ChunkNo == out[j].ChunkNo {
			return out[i].ID < out[j].ID
		}
		return out[i].ChunkNo < out[j].ChunkNo
	})
	if limit <= 0 {
		limit = 512
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func HasValidationResults(ctx context.Context, repo Repository, taskID string) (bool, error) {
	if p, ok := repo.(ValidationHotPathProvider); ok {
		return p.HasValidationResults(ctx, taskID)
	}
	items, err := repo.ListValidationResults(ctx, taskID)
	return len(items) > 0, err
}

func FirstInvalidSuccessfulChunk(ctx context.Context, repo Repository, taskID string) (string, domain.ValidationStatus, error) {
	if p, ok := repo.(ValidationHotPathProvider); ok {
		return p.FirstInvalidSuccessfulChunk(ctx, taskID)
	}
	chunks, err := repo.ListChunks(ctx, taskID)
	if err != nil {
		return "", "", err
	}
	results, err := repo.ListValidationResults(ctx, taskID)
	if err != nil {
		return "", "", err
	}
	latest := map[string]domain.ValidationResult{}
	for _, v := range results {
		old, ok := latest[v.ChunkID]
		if !ok || v.FinishedAt.After(old.FinishedAt) {
			latest[v.ChunkID] = v
		}
	}
	for _, ch := range chunks {
		if ch.Status != domain.ChunkSuccess {
			continue
		}
		v, ok := latest[ch.ID]
		if !ok {
			return ch.ID, "", nil
		}
		if v.Status != domain.ValidationSuccess {
			return ch.ID, v.Status, nil
		}
	}
	return "", "", nil
}

func ListRepairableValidationChunkIDs(ctx context.Context, repo Repository, taskID string, limit int) ([]string, error) {
	if p, ok := repo.(ValidationHotPathProvider); ok {
		return p.ListRepairableValidationChunkIDs(ctx, taskID, limit)
	}
	items, err := repo.ListValidationResults(ctx, taskID)
	if err != nil {
		return nil, err
	}
	latest := map[string]domain.ValidationResult{}
	for _, v := range items {
		old, ok := latest[v.ChunkID]
		if !ok || v.FinishedAt.After(old.FinishedAt) {
			latest[v.ChunkID] = v
		}
	}
	out := []string{}
	for cid, v := range latest {
		if v.Status == domain.ValidationMismatch || v.Status == domain.ValidationError {
			out = append(out, cid)
		}
	}
	sort.Strings(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
