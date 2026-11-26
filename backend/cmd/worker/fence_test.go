package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
)

func TestRequestsKeepTheirOwnClaimGeneration(t *testing.T) {
	// Same job URL, concurrent executions: the old execution must never borrow
	// the successor's generation from a process-wide map.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-DTS-Lease-Generation") != r.URL.Query().Get("expected") {
			t.Error("request borrowed another execution generation")
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	var wg sync.WaitGroup
	for _, g := range []int64{7, 8} {
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(g int64) {
				defer wg.Done()
				if _, err := doJSON("POST", server.URL+"/api/v1/workers/w/chunks/c/lease?expected="+strconv.FormatInt(g, 10), nil, nil, g); err != nil {
					t.Error(err)
				}
			}(g)
		}
	}
	wg.Wait()
}
