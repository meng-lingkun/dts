package migration

import (
	"context"
	"dts/backend/internal/domain"
	"errors"
	"fmt"
)

func (s *Service) completeLifecycleCleanup(ctx context.Context, id, direction string) error {
	_, err := s.mutateMigration(ctx, id, func(m *domain.MigrationTask) error {
		o := lifecycleOperation(m, direction)
		if o == nil {
			return ErrFreezeRequired
		}
		if (direction == "forward" && m.Status != domain.StatusFinished) || (direction == "reverse" && m.Status != domain.StatusRolledBack) {
			return errors.New("checkpoint cleanup requires completed lifecycle")
		}
		o.CleanupStatus = "RUNNING"
		o.CleanupError = ""
		return nil
	})
	if err != nil {
		return err
	}
	workErr := s.cleanupCDCCheckpoint(ctx, id, direction)
	_, saveErr := s.mutateMigration(ctx, id, func(m *domain.MigrationTask) error {
		o := lifecycleOperation(m, direction)
		if o == nil {
			return ErrFreezeRequired
		}
		o.CleanupStatus = "DONE"
		o.CleanupError = ""
		if workErr != nil {
			o.CleanupStatus = "RETRY"
			o.CleanupError = fmt.Sprintf("%s checkpoint cleanup failed: %v", direction, workErr)
		}
		return nil
	})
	return errors.Join(workErr, saveErr)
}
