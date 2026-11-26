package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

var ErrCDCExecutionBusy = errors.New("another server is executing CDC for this task and direction; retry without acknowledging the source")

type cdcExecutionKey struct{}
type cdcExecutionGuard func() error

func cdcDirection(direction string) (string, error) {
	direction = strings.ToLower(strings.TrimSpace(direction))
	if direction == "" {
		direction = "forward"
	}
	if direction != "forward" && direction != "reverse" {
		return "", fmt.Errorf("invalid CDC direction %q", direction)
	}
	return direction, nil
}

func cdcExecutionLeaseTTL() time.Duration {
	seconds := envInt64("DTS_CDC_SPOOL_DRAIN_LEASE_SECONDS", 300)
	if seconds < 30 {
		seconds = 30
	}
	if seconds > 3600 {
		seconds = 3600
	}
	return time.Duration(seconds) * time.Second
}

// One gate covers every target writer and validation barrier, including direct
// API calls and operator recovery. The unique owner token identifies an execution,
// not a server: an old request can neither renew nor release its successor.
// The durable epoch is also installed in each supported target transaction.
func (s *Service) lockCDCExecution(ctx context.Context, taskID, direction string) (context.Context, func(), error) {
	lock := s.cdcSpoolLock(taskID, direction)
	for !lock.TryLock() {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := ctx.Err(); err != nil {
		lock.Unlock()
		return nil, nil, err
	}
	leaser, ok := s.repo.(repository.CDCExecutionLeaser)
	if !ok {
		// Custom in-process repositories retain local serialization. All shipped
		// stores and production wrappers implement the durable lease interface.
		return ctx, lock.Unlock, nil
	}
	owner := s.instanceID + "/" + newID("cdc")
	ttl := cdcExecutionLeaseTTL()
	acquired, err := leaser.AcquireCDCSpoolDrainLease(ctx, taskID, direction, owner, ttl)
	if err != nil || !acquired {
		lock.Unlock()
		if err == nil {
			err = ErrCDCExecutionBusy
		}
		return nil, nil, err
	}
	opCtx, cancel := context.WithCancelCause(ctx)
	epoch, err := leaser.CDCExecutionEpoch(ctx, taskID, direction, owner)
	if err != nil {
		cancel(err)
		_ = leaser.ReleaseCDCSpoolDrainLease(context.Background(), taskID, direction, owner)
		lock.Unlock()
		return nil, nil, err
	}
	opCtx = repository.WithCDCFence(opCtx, repository.CDCFence{TaskID: taskID, Direction: direction, Owner: owner, Epoch: epoch})
	guard := func() error {
		if err := context.Cause(opCtx); err != nil {
			return err
		}
		renewCtx, renewCancel := context.WithTimeout(opCtx, ttl/3)
		defer renewCancel()
		if err := leaser.RenewCDCSpoolDrainLease(renewCtx, taskID, direction, owner, ttl); err != nil {
			err = fmt.Errorf("CDC execution lease lost: %w", err)
			cancel(err)
			return err
		}
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(ttl / 3)
		defer ticker.Stop()
		for {
			select {
			case <-opCtx.Done():
				return
			case <-ticker.C:
				if guard() != nil {
					return
				}
			}
		}
	}()
	operationCtx := context.WithValue(opCtx, cdcExecutionKey{}, cdcExecutionGuard(guard))
	release := func() {
		cancel(context.Canceled)
		<-done
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer releaseCancel()
		_ = leaser.ReleaseCDCSpoolDrainLease(releaseCtx, taskID, direction, owner)
		lock.Unlock()
	}
	return operationCtx, release, nil
}

func checkCDCExecution(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if guard, ok := ctx.Value(cdcExecutionKey{}).(cdcExecutionGuard); ok {
		if err := guard(); err != nil {
			return err
		}
	}
	return checkManagedLease(ctx)
}

func (s *Service) ApplyCDCEvents(ctx context.Context, taskID string, req domain.CDCApplyRequest) (*domain.CDCApplyResult, error) {
	direction, err := cdcDirection(req.Direction)
	if err != nil {
		return nil, err
	}
	opCtx, release, err := s.lockCDCExecution(ctx, taskID, direction)
	if err != nil {
		return nil, err
	}
	defer release()
	req.Direction = direction
	if len(req.Events) == 0 {
		return nil, errors.New("CDC event batch is empty")
	}
	if strings.TrimSpace(req.Events[len(req.Events)-1].PositionValue) == "" {
		return nil, errors.New("final CDC event must include a durable source position before any rows can be applied")
	}
	task, err := s.repo.GetMigration(opCtx, taskID)
	if err != nil {
		return nil, err
	}
	if !cdcApplyReady(task, direction) {
		return nil, fmt.Errorf("%s CDC apply is unsafe from %s", direction, task.Status)
	}
	// A direct bridge must not overtake transactions captured earlier by a
	// managed reader. The private apply used by the drain avoids gate re-entry.
	stats, err := s.repo.CDCSpoolStats(opCtx, taskID, direction)
	if err != nil {
		return nil, err
	}
	if stats.PendingTransactions > 0 {
		stats, err = s.drainCDCSpoolLocked(opCtx, taskID, direction, 1000)
		if err != nil {
			return nil, err
		}
		if stats.PendingTransactions > 0 {
			return nil, fmt.Errorf("%s CDC spool still has %d earlier transactions; retry without acknowledging the source", direction, stats.PendingTransactions)
		}
	}
	return s.applyCDCEvents(opCtx, taskID, req)
}

func (s *Service) ReplayCDCDeadLetter(ctx context.Context, taskID, id string) (*domain.CDCApplyResult, error) {
	return s.withCDCDeadLetter(ctx, taskID, id, func(opCtx context.Context) (*domain.CDCApplyResult, error) {
		return s.replayCDCDeadLetter(opCtx, taskID, id)
	})
}

func (s *Service) ResolveCDCCommitUncertain(ctx context.Context, taskID, id, decision string) (*domain.CDCApplyResult, error) {
	return s.withCDCDeadLetter(ctx, taskID, id, func(opCtx context.Context) (*domain.CDCApplyResult, error) {
		return s.resolveCDCCommitUncertain(opCtx, taskID, id, decision)
	})
}

func (s *Service) withCDCDeadLetter(ctx context.Context, taskID, id string, work func(context.Context) (*domain.CDCApplyResult, error)) (*domain.CDCApplyResult, error) {
	item, err := s.repo.GetCDCDeadLetter(ctx, id)
	if err != nil {
		return nil, err
	}
	if item.TaskID != taskID {
		return nil, errors.New("CDC dead letter does not belong to migration")
	}
	direction, err := cdcDirection(item.Direction)
	if err != nil {
		return nil, err
	}
	opCtx, release, err := s.lockCDCExecution(ctx, taskID, direction)
	if err != nil {
		return nil, err
	}
	defer release()
	// The private operation re-reads the DLQ under the lease.
	return work(opCtx)
}

func (s *Service) withCDCExecution(ctx context.Context, taskID, direction string, work func(context.Context) error) error {
	opCtx, release, err := s.lockCDCExecution(ctx, taskID, direction)
	if err != nil {
		return err
	}
	defer release()
	if err := checkCDCExecution(opCtx); err != nil {
		return err
	}
	return work(opCtx)
}

func (s *Service) RecordCDCProgress(ctx context.Context, id string, p *domain.CDCPosition) error {
	err := s.withCDCExecution(ctx, id, "forward", func(opCtx context.Context) error {
		return s.recordCDCProgress(opCtx, id, p, false)
	})
	if err == nil {
		s.maybeStartValidationAfterCatchup(ctx, id)
	}
	return err
}

func (s *Service) RecordRollbackCDCProgress(ctx context.Context, id string, p *domain.CDCPosition) error {
	return s.withCDCExecution(ctx, id, "reverse", func(opCtx context.Context) error {
		return s.recordRollbackCDCProgress(opCtx, id, p)
	})
}

func (s *Service) ReadyForCutover(ctx context.Context, id string, maxLagMS int64) error {
	return s.withCDCExecution(ctx, id, "forward", func(opCtx context.Context) error {
		return s.readyForCutover(opCtx, id, maxLagMS)
	})
}

func (s *Service) ReadyForRollback(ctx context.Context, id string, maxLagMS int64) error {
	return s.withCDCExecution(ctx, id, "reverse", func(opCtx context.Context) error {
		return s.readyForRollback(opCtx, id, maxLagMS)
	})
}

func (s *Service) requireFinalCDCState(ctx context.Context, taskID, direction string) error {
	if err := checkCDCExecution(ctx); err != nil {
		return err
	}
	stats, err := s.repo.CDCSpoolStats(ctx, taskID, direction)
	if err != nil {
		return err
	}
	if stats.PendingTransactions != 0 || stats.PendingEvents != 0 {
		return fmt.Errorf("final %s CDC spool changed before finalization; retry after draining", direction)
	}
	return s.requireResolvedCDCDeadLetters(ctx, taskID, direction)
}
