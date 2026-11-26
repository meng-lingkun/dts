package api

import (
	"dts/backend/internal/repository"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWorkerMutationRequiresClaimGeneration(t *testing.T) {
	for _, path := range []string{"chunks/c/lease", "chunks/c/complete", "chunks/c/fail", "engine-jobs/j/start", "engine-jobs/j/cdc/events", "engine-jobs/j/complete"} {
		for _, header := range []string{"", "0", "-1", "garbage", "7"} {
			called := false
			h := workerLeaseFence(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				g, ok := repository.LeaseGeneration(r.Context())
				if !ok || g != 7 {
					t.Errorf("missing generation in context")
				}
				w.WriteHeader(204)
			}))
			r := httptest.NewRequest("POST", "/api/v1/workers/w/"+path, nil)
			r.Header.Set("X-DTS-Lease-Generation", header)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if header == "7" {
				if w.Code != 204 || !called {
					t.Fatalf("valid generation rejected: %s %d", path, w.Code)
				}
			} else if w.Code != 428 || called {
				t.Fatalf("unfenced request reached handler: %s %s %d", path, header, w.Code)
			}
		}
	}
}
