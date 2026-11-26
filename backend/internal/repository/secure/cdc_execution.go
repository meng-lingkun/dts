package secure

import (
	"context"
	"errors"
	"time"

	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

var _ repository.CDCExecutionLeaser = (*Store)(nil)

func (s *Store) CDCExecutionEpoch(ctx context.Context, taskID, direction, owner string) (int64, error) {
	p, ok := s.inner.(repository.CDCExecutionLeaser)
	if !ok {
		return 0, errors.New("repository does not support CDC execution epochs")
	}
	return p.CDCExecutionEpoch(ctx, taskID, direction, owner)
}

func (s *Store) RenewCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string, ttl time.Duration) error {
	p, ok := s.inner.(repository.CDCExecutionLeaser)
	if !ok {
		return errors.New("repository does not support CDC execution leases")
	}
	return p.RenewCDCSpoolDrainLease(ctx, taskID, direction, owner, ttl)
}

func (s *Store) LatestCDCPosition(ctx context.Context, taskID, direction string) (*domain.CDCPosition, error) {
	return s.inner.LatestCDCPosition(ctx, taskID, direction)
}
