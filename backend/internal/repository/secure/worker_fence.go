package secure

import (
	"context"
	"dts/backend/internal/domain"
)

func (s *Store) UpdateOwnedChunk(ctx context.Context, worker string, c *domain.MigrationChunk) error {
	return s.inner.UpdateOwnedChunk(ctx, worker, c)
}
func (s *Store) UpdateOwnedEngineJob(ctx context.Context, worker string, j *domain.EngineJob) error {
	return s.inner.UpdateOwnedEngineJob(ctx, worker, j)
}
