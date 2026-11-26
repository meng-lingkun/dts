package migration

import (
	"context"
	"strings"
	"testing"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository/memory"
)

func TestFilteredMigrationScope(t *testing.T) {
	task := &domain.MigrationTask{Mode: domain.ModeFull, Tables: []domain.TableMapping{{SourceTable: "orders", SplitStrategy: "CUSTOM_SQL", CustomWhere: "orders.status = 'PAID'"}}}
	if err := validateRowFilters(task); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []domain.MigrationMode{domain.ModeFullAndIncremental, domain.ModeIncremental} {
		task.Mode, task.Status = mode, domain.StatusCDCCatchingUp
		if validateRowFilters(task) == nil || cdcApplyReady(task, "forward") || cdcCaptureReady(task, "forward") {
			t.Fatalf("filtered %s accepted", mode)
		}
	}
	task.Mode, task.Tables[0].SplitStrategy = domain.ModeFull, "AUTO"
	if validateRowFilters(task) == nil {
		t.Fatal("silently ignored filter")
	}
}

func TestLegacyFilteredTaskCannotAdvanceCDCOrCutover(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	s := NewService(repo, connector.NewRegistry())
	p := &domain.CDCPosition{PositionType: "BINLOG", PositionValue: "log:9"}
	for _, tc := range []struct {
		name   string
		status domain.MigrationStatus
		call   func(string) error
	}{
		{"started", domain.StatusCDCInitializing, func(id string) error { return s.MarkCDCStarted(ctx, id, p) }},
		{"ready", domain.StatusCDCCatchingUp, func(id string) error { return s.ReadyForCutover(ctx, id, 1) }},
		{"cutover", domain.StatusReadyCutover, func(id string) error { return s.Cutover(ctx, id) }},
		{"reverse-prepare", domain.StatusFinished, func(id string) error { return s.PrepareRollback(ctx, id) }},
		{"reverse-started", domain.StatusRollbackPreparing, func(id string) error { return s.MarkRollbackCDCStarted(ctx, id, p) }},
		{"reverse-progress", domain.StatusRollbackSyncing, func(id string) error { return s.RecordRollbackCDCProgress(ctx, id, p) }},
		{"reverse-ready", domain.StatusRollbackSyncing, func(id string) error { return s.ReadyForRollback(ctx, id, 1) }},
		{"rollback", domain.StatusRollbackReady, func(id string) error { return s.Rollback(ctx, id) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &domain.MigrationTask{ID: tc.name, Mode: domain.ModeFullAndIncremental, Status: tc.status, Tables: []domain.TableMapping{{SourceTable: "orders", SplitStrategy: "CUSTOM_SQL", CustomWhere: "id > 4"}}}
			if err := repo.CreateMigration(ctx, m); err != nil {
				t.Fatal(err)
			}
			if err := tc.call(m.ID); err == nil || !strings.Contains(err.Error(), "row-filtered") {
				t.Fatalf("got %v", err)
			}
			task, err := repo.GetMigration(ctx, m.ID)
			if err != nil || task.Status != tc.status {
				t.Fatalf("task=%v err=%v", task, err)
			}
		})
	}
}

func TestValidationRejectsInvalidPersistedUpperBound(t *testing.T) {
	for _, upper := range []string{"invalid", "null", "[]", "[null]", "[{}]", `[{"Null":true}]`, `[{"Raw":"MQ=="},{"Raw":"MQ=="}]`} {
		_, err := complexValidationReadRequest(domain.MigrationTable{}, domain.MigrationChunk{SplitType: "PRIMARY_KEY_KEYSET", EndCursorJSON: upper}, []string{"id"})
		if err == nil {
			t.Errorf("accepted invalid persisted upper bound %q", upper)
		}
	}
}
