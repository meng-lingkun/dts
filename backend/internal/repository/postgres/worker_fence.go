package postgres

import (
	"context"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
	"fmt"
	"strings"
)

func workerFenceSQL(ctx context.Context) string {
	if generation, ok := repository.LeaseGeneration(ctx); ok {
		return fmt.Sprintf(" AND lease_generation=%d AND lease_until>now()", generation)
	}
	return " AND (lease_generation=0 OR lease_until>now())"
}
func (s *Store) UpdateOwnedChunk(ctx context.Context, worker string, c *domain.MigrationChunk) error {
	if g, ok := repository.LeaseGeneration(ctx); ok && g != c.LeaseGeneration {
		return repository.ErrLeaseOwner
	}
	q := chunkMutationSQL(c)
	if c.Status != domain.ChunkRunning {
		q = strings.Replace(q, "SET ", "SET lease_generation=lease_generation+1,", 1)
	}
	q += fmt.Sprintf(" AND worker_id=%s AND status='RUNNING' AND lease_generation=%d", qs(worker), c.LeaseGeneration) + workerFenceSQL(ctx) + " RETURNING lease_generation"
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return err
	}
	if len(r.Rows) == 0 {
		return repository.ErrLeaseOwner
	}
	c.LeaseGeneration = iv(r.Rows[0], 0)
	return nil
}
func (s *Store) UpdateOwnedEngineJob(ctx context.Context, worker string, j *domain.EngineJob) error {
	if g, ok := repository.LeaseGeneration(ctx); ok && g != j.LeaseGeneration {
		return repository.ErrLeaseOwner
	}
	q := engineMutationSQL(j)
	if j.Status == domain.EngineJobRunning {
		q = strings.Replace(q, "status='RUNNING'", "status=CASE WHEN status='STOP_REQUESTED' THEN 'STOP_REQUESTED' ELSE 'RUNNING' END", 1)
	}
	if j.Status != domain.EngineJobRunning && j.Status != domain.EngineJobStopRequested {
		q = strings.Replace(q, "SET ", "SET lease_generation=lease_generation+1,", 1)
	}
	q += fmt.Sprintf(" AND worker_id=%s AND status IN ('RUNNING','STOP_REQUESTED') AND lease_generation=%d", qs(worker), j.LeaseGeneration) + workerFenceSQL(ctx) + " RETURNING lease_generation,status"
	r, err := s.db.QuerySQL(ctx, q)
	if err != nil {
		return err
	}
	if len(r.Rows) == 0 {
		return repository.ErrLeaseOwner
	}
	j.LeaseGeneration = iv(r.Rows[0], 0)
	j.Status = domain.EngineJobStatus(sv(r.Rows[0], 1))
	return nil
}
