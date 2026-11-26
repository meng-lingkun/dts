package migration

import (
	"context"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

type managedLeaseGuardKey struct{}

func (s *Service) withManagedLeaseGuard(ctx context.Context, worker string, claimed *domain.EngineJob) context.Context {
	ctx = repository.WithManagedFence(ctx, repository.ManagedFence{JobID: claimed.ID, WorkerID: worker, Generation: claimed.LeaseGeneration})
	id, generation := claimed.ID, claimed.LeaseGeneration
	guard := func() error {
		current, err := s.repo.GetEngineJob(ctx, id)
		if err != nil {
			return err
		}
		if current.WorkerID != worker || current.LeaseGeneration != generation || (current.Status != domain.EngineJobRunning && current.Status != domain.EngineJobStopRequested) || !repository.WorkerLeaseValid(ctx, current.LeaseGeneration, current.LeaseUntil) {
			return repository.ErrLeaseOwner
		}
		return nil
	}
	return context.WithValue(ctx, managedLeaseGuardKey{}, guard)
}
func checkManagedLease(ctx context.Context) error {
	if guard, ok := ctx.Value(managedLeaseGuardKey{}).(func() error); ok {
		return guard()
	}
	return ctx.Err()
}
