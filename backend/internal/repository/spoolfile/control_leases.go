package spoolfile

import (
	"context"
	"errors"
	"time"

	"dts/backend/internal/repository"
)

var _ repository.ControlOperationLeaser = (*Store)(nil)

func (s *Store) AcquireControlOperation(ctx context.Context, taskID, operation, owner string, ttl time.Duration) (bool, error) {
	p, ok := s.Repository.(repository.ControlOperationLeaser)
	if !ok {
		return false, errors.New("repository does not support control-operation leases")
	}
	return p.AcquireControlOperation(ctx, taskID, operation, owner, ttl)
}

func (s *Store) RenewControlOperation(ctx context.Context, taskID, operation, owner string, ttl time.Duration) error {
	p, ok := s.Repository.(repository.ControlOperationLeaser)
	if !ok {
		return errors.New("repository does not support control-operation leases")
	}
	return p.RenewControlOperation(ctx, taskID, operation, owner, ttl)
}

func (s *Store) ReleaseControlOperation(ctx context.Context, taskID, operation, owner string) error {
	p, ok := s.Repository.(repository.ControlOperationLeaser)
	if !ok {
		return errors.New("repository does not support control-operation leases")
	}
	return p.ReleaseControlOperation(ctx, taskID, operation, owner)
}
