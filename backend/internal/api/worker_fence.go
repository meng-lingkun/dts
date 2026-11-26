package api

import (
	"dts/backend/internal/repository"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

func workerLeaseFence(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if r.Method == http.MethodPost && len(parts) >= 8 && strings.HasPrefix(r.URL.Path, "/api/v1/workers/") && (parts[5] == "chunks" || parts[5] == "engine-jobs") {
			generation, err := strconv.ParseInt(r.Header.Get("X-DTS-Lease-Generation"), 10, 64)
			if err != nil || generation <= 0 {
				apiError(w, http.StatusPreconditionRequired, errors.New("positive X-DTS-Lease-Generation from the current claim is required; upgrade Worker before resuming"))
				return
			}
			r = r.WithContext(repository.WithLeaseGeneration(r.Context(), generation))
		}
		next.ServeHTTP(w, r)
	})
}
