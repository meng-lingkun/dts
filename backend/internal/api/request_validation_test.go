package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/engine"
	"dts/backend/internal/repository/memory"
)

func TestMalformedLifecycleJSONCannotCompleteOrStartWork(t *testing.T) {
	for _, action := range []string{"complete", "fail", "rollback/cdc-started"} {
		t.Run(action, func(t *testing.T) {
			t.Setenv("DTS_WORKER_TOKEN", "worker-test")
			t.Setenv("DTS_RBAC_TOKENS", "admin:admin-test")
			ctx := context.Background()
			store := memory.New()
			if err := store.CreateMigration(ctx, &domain.MigrationTask{ID: "task", Status: domain.StatusRollbackPreparing}); err != nil {
				t.Fatal(err)
			}
			if err := store.CreateEngineJob(ctx, &domain.EngineJob{ID: "job", TaskID: "task", WorkerID: "worker", Status: domain.EngineJobRunning}); err != nil {
				t.Fatal(err)
			}
			handler := New(store, connector.NewRegistry(), engine.NewRegistry()).Handler()
			path := "/api/v1/workers/worker/engine-jobs/job/" + action
			if action == "rollback/cdc-started" {
				path = "/api/v1/migrations/task/" + action
			}
			r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"error":`))
			r.Header.Set("Authorization", "Bearer admin-test")
			r.Header.Set("X-DTS-Worker-Token", "worker-test")
			r.Header.Set("X-DTS-Lease-Generation", "1")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, r)
			if response.Code != http.StatusBadRequest {
				t.Errorf("malformed mutation accepted: status=%d body=%s", response.Code, response.Body.String())
			}
			job, _ := store.GetEngineJob(ctx, "job")
			task, _ := store.GetMigration(ctx, "task")
			if job.Status != domain.EngineJobRunning || task.Status != domain.StatusRollbackPreparing {
				t.Fatalf("malformed body changed state: job=%s task=%s", job.Status, task.Status)
			}
		})
	}
}

func TestJSONRequestMustContainOneCompleteValue(t *testing.T) {
	for _, body := range []string{`{} {}`, `{} trailing`, `{"max_lag_ms":1} {"max_lag_ms":99999}`} {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		var value map[string]any
		if err := decode(r, &value); err == nil {
			t.Errorf("accepted trailing request data: %s", body)
		}
	}
}

func TestOperatorPrivilegedRoutesAreDeniedAtHTTPBoundary(t *testing.T) {
	t.Setenv("DTS_RBAC_TOKENS", "operator:operator-test")
	handler := New(memory.New(), connector.NewRegistry(), engine.NewRegistry()).Handler()
	for _, action := range []string{"ready-cutover", "cdc", "cdc/started", "cdc/events", "cdc/debezium", "cdc/canal"} {
		response := requestJSON(t, handler, http.MethodPost, "/api/v1/migrations/task/"+action, map[string]any{}, "operator-test")
		if response.Code != http.StatusForbidden {
			t.Errorf("operator reached %s handler: status=%d body=%s", action, response.Code, response.Body.String())
		}
	}
}

func TestAuthenticatedModeDoesNotExposeWorkerAPIWithoutWorkerToken(t *testing.T) {
	t.Setenv("DTS_AUTH_REQUIRED", "true")
	t.Setenv("DTS_RBAC_TOKENS", "admin:admin-test")
	t.Setenv("DTS_WORKER_TOKEN", "")
	store := memory.New()
	handler := New(store, connector.NewRegistry(), engine.NewRegistry()).Handler()
	response := requestJSON(t, handler, http.MethodPost, "/api/v1/workers/register", map[string]any{"id": "intruder", "hostname": "intruder"}, "")
	if response.Code != http.StatusUnauthorized {
		t.Errorf("worker endpoint bypassed user authentication when no machine token is configured: %d %s", response.Code, response.Body.String())
	}
	workers, _ := store.ListWorkers(context.Background())
	if len(workers) != 0 {
		t.Fatalf("unauthenticated worker registered: %+v", workers)
	}
}

func TestJSONRequestSizeAndOptionalEmptyBody(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	r.ContentLength = maxJSONRequestBytes + 1
	var result map[string]any
	err := decode(r, &result)
	if err == nil {
		t.Fatal("oversized Content-Length accepted")
	}
	response := httptest.NewRecorder()
	apiError(response, http.StatusBadRequest, err)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized request status=%d", response.Code)
	}
	if err := decodeOptional(httptest.NewRequest(http.MethodPost, "/", nil), &result); err != nil {
		t.Fatalf("legacy empty lifecycle request rejected: %v", err)
	}
}
