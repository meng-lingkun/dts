package api

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/engine"
	"dts/backend/internal/repository/memory"
)

func TestConcurrentCDCAPIRequestReturnsRetryableConflictWithoutAck(t *testing.T) {
	t.Setenv("DTS_AUTH_REQUIRED", "")
	t.Setenv("DTS_API_TOKEN", "")
	t.Setenv("DTS_RBAC_TOKENS", "")
	t.Setenv("DTS_AUTH_SECRET", "test-cdc-execution-auth-secret")
	ctx := context.Background()
	store := memory.New()
	if err := store.CreateMigration(ctx, &domain.MigrationTask{ID: "task", Status: domain.StatusCDCCatchingUp, Mode: domain.ModeIncremental}); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.AcquireCDCSpoolDrainLease(ctx, "task", "forward", "another-server/execution", time.Minute); err != nil || !ok {
		t.Fatalf("seed active execution: %v %v", ok, err)
	}
	handler := New(store, connector.NewRegistry(), engine.NewRegistry()).Handler()
	response := requestJSON(t, handler, http.MethodPost, "/api/v1/migrations/task/cdc/events", domain.CDCApplyRequest{Events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "BINLOG", PositionValue: "binlog.1:100"}}}, "")
	if response.Code != http.StatusConflict || response.Header().Get("Retry-After") != "1" || !strings.Contains(response.Body.String(), "retry without acknowledging") {
		t.Fatalf("CDC contention is not retryable: status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	if checkpoint, err := store.LatestCDCPosition(ctx, "task", "forward"); err != nil || checkpoint != nil {
		t.Fatalf("rejected request advanced checkpoint: %+v %v", checkpoint, err)
	}
}
