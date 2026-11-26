package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
)

func validateRowFilters(task *domain.MigrationTask) error {
	for i, mapping := range task.Tables {
		if normalizedSplitStrategy(mapping.SplitStrategy) != "CUSTOM_SQL" {
			if strings.TrimSpace(mapping.CustomWhere) != "" {
				return fmt.Errorf("table mapping %d: custom_where requires CUSTOM_SQL", i)
			}
			continue
		}
		if task.Mode != domain.ModeFull {
			return fmt.Errorf("table mapping %d: CUSTOM_SQL requires FULL mode because CDC does not filter row events", i)
		}
		if _, err := connector.CompileCustomWhere(mapping.CustomWhere, mapping.SourceTable, nil); err != nil {
			return fmt.Errorf("table mapping %d: %w", i, err)
		}
	}
	return nil
}

// Previously created mixed-mode jobs may still be active during an upgrade.
func hasCDCRowFilter(task *domain.MigrationTask) bool {
	for _, mapping := range task.Tables {
		if normalizedSplitStrategy(mapping.SplitStrategy) == "CUSTOM_SQL" || strings.TrimSpace(mapping.CustomWhere) != "" {
			return true
		}
	}
	return false
}

// A persisted task may predate the FULL-only restriction. Check the mapping
// before recording a CDC position or acting on one during cutover/rollback.
func (s *Service) unfilteredCDCTask(ctx context.Context, id string) (*domain.MigrationTask, error) {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return nil, err
	}
	if hasCDCRowFilter(task) {
		return nil, errors.New("CDC is unavailable for row-filtered migrations")
	}
	return task, nil
}
