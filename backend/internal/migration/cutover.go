package migration

import (
	"context"
	"fmt"

	"dts/backend/internal/domain"
)

// Re-read the task only after acquiring the shared task lease. The private
// implementations must never act on a snapshot read before lease acquisition.
func (s *Service) exclusiveControlOperation(ctx context.Context, id, operation string, work func(context.Context, string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	acquired, err := s.acquireControlOperation(ctx, id, operation)
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("another control operation is already running for task %s", id)
	}
	s.runControlOperation(ctx, id, operation, func(opCtx context.Context) {
		err = work(opCtx, id)
	})
	return err
}

func (s *Service) Cutover(ctx context.Context, id string) error {
	return s.exclusiveControlOperation(ctx, id, "cutover", s.cutover)
}

func (s *Service) PrepareRollback(ctx context.Context, id string) error {
	return s.exclusiveControlOperation(ctx, id, "rollback-prepare", s.prepareRollback)
}

func (s *Service) Rollback(ctx context.Context, id string) error {
	return s.exclusiveControlOperation(ctx, id, "rollback", s.rollback)
}

func (s *Service) requireResolvedCDCDeadLetters(ctx context.Context, id, direction string) error {
	items, err := s.repo.ListCDCDeadLetters(ctx, id)
	if err != nil {
		return err
	}
	for _, item := range items {
		if (item.Direction == direction || (direction == "forward" && item.Direction == "")) && item.Status != domain.CDCDeadLetterResolved {
			return fmt.Errorf("%s CDC dead letter %s is %s; resolve it before cutover or rollback", direction, item.ID, item.Status)
		}
	}
	return nil
}
