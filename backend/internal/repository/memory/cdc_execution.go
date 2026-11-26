package memory

import (
	"context"
	"time"

	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

var _ repository.CDCExecutionLeaser = (*Store)(nil)

// Reuse the durable lease map with a disjoint key space. Control-plane leases
// remain keyed by task ID; CDC directions must not block worker shutdown.
func cdcLeaseKey(taskID, direction string) string { return "cdc\x00" + taskID + "\x00" + direction }

func (s *Store) AcquireCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := cdcLeaseKey(taskID, direction)
	old, exists := s.controlOperations[key]
	if old.LeaseUntil.After(time.Now()) {
		return false, nil
	}
	now := time.Now()
	s.controlOperations[key] = persistedControlOperation{TaskID: taskID, Operation: "cdc", Owner: owner, LeaseUntil: now.Add(ttl), UpdatedAt: now, Generation: old.Generation + 1}
	if err := s.persistLocked(); err != nil {
		if exists {
			s.controlOperations[key] = old
		} else {
			delete(s.controlOperations, key)
		}
		return false, err
	}
	return true, nil
}

func (s *Store) RenewCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string, ttl time.Duration) error {
	return s.RenewControlOperation(ctx, cdcLeaseKey(taskID, direction), "cdc", owner, ttl)
}

func (s *Store) ReleaseCDCSpoolDrainLease(ctx context.Context, taskID, direction, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := cdcLeaseKey(taskID, direction)
	old, exists := s.controlOperations[key]
	if !exists {
		return nil
	}
	if old.Owner != owner {
		return repository.ErrLeaseOwner
	}
	v := old
	v.Owner = ""
	v.LeaseUntil = time.Time{}
	v.UpdatedAt = time.Now()
	s.controlOperations[key] = v
	if err := s.persistLocked(); err != nil {
		s.controlOperations[key] = old
		return err
	}
	return nil
}

func (s *Store) CDCExecutionEpoch(_ context.Context, taskID, direction, owner string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v := s.controlOperations[cdcLeaseKey(taskID, direction)]
	if v.Owner != owner || !v.LeaseUntil.After(time.Now()) || v.Generation <= 0 {
		return 0, repository.ErrLeaseOwner
	}
	return v.Generation, nil
}

// Runs under the same mutex as the checkpoint/spool mutation and takeover.
func (s *Store) validateAdmissionLocked(ctx context.Context, taskID, direction string) error {
	if f, ok := repository.CDCFenceFrom(ctx); ok {
		if direction == "" {
			direction = "forward"
		}
		v := s.controlOperations[cdcLeaseKey(taskID, direction)]
		if f.TaskID != taskID || f.Direction != direction || f.Owner != v.Owner || f.Epoch != v.Generation || !v.LeaseUntil.After(time.Now()) {
			return repository.ErrLeaseOwner
		}
	}
	if f, ok := repository.ManagedFenceFrom(ctx); ok {
		v, exists := s.engineJobs[f.JobID]
		if !exists || v.TaskID != taskID || v.WorkerID != f.WorkerID || v.LeaseGeneration != f.Generation || !repository.WorkerLeaseValid(ctx, v.LeaseGeneration, v.LeaseUntil) || (v.Status != domain.EngineJobRunning && v.Status != domain.EngineJobStopRequested) {
			return repository.ErrLeaseOwner
		}
	}
	return ctx.Err()
}

func (s *Store) LatestCDCPosition(_ context.Context, taskID, direction string) (*domain.CDCPosition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := len(s.cdcPositions) - 1; i >= 0; i-- {
		p := s.cdcPositions[i]
		if p.TaskID == taskID && (p.Direction == direction || (direction == "forward" && p.Direction == "")) {
			return &p, nil
		}
	}
	return nil, nil
}
