package memory

import (
	"context"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

func (s *Store) UpdateOwnedChunk(ctx context.Context, worker string, next *domain.MigrationChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.chunks[next.ID]
	if !ok {
		return ErrNotFound
	}
	if old.Status != domain.ChunkRunning || old.WorkerID != worker || old.LeaseGeneration != next.LeaseGeneration || !repository.WorkerLeaseValid(ctx, old.LeaseGeneration, old.LeaseUntil) {
		return repository.ErrLeaseOwner
	}
	v := *next
	if v.Status != domain.ChunkRunning {
		v.LeaseGeneration++
	}
	s.chunks[v.ID] = cloneChunk(v)
	if err := s.persistLocked(); err != nil {
		s.chunks[v.ID] = old
		return err
	}
	*next = v
	return nil
}
func (s *Store) UpdateOwnedEngineJob(ctx context.Context, worker string, next *domain.EngineJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.engineJobs[next.ID]
	if !ok {
		return ErrNotFound
	}
	if (old.Status != domain.EngineJobRunning && old.Status != domain.EngineJobStopRequested) || old.WorkerID != worker || old.LeaseGeneration != next.LeaseGeneration || !repository.WorkerLeaseValid(ctx, old.LeaseGeneration, old.LeaseUntil) {
		return repository.ErrLeaseOwner
	}
	v := *next
	if old.Status == domain.EngineJobStopRequested && v.Status == domain.EngineJobRunning {
		v.Status = domain.EngineJobStopRequested
	}
	if v.Status != domain.EngineJobRunning && v.Status != domain.EngineJobStopRequested {
		v.LeaseGeneration++
	}
	s.engineJobs[v.ID] = v
	if err := s.persistLocked(); err != nil {
		s.engineJobs[v.ID] = old
		return err
	}
	*next = v
	return nil
}
