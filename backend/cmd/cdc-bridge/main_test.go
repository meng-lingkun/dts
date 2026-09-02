package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"qmigration/backend/internal/domain"
	"testing"
)

func TestPostBatch(t *testing.T) {
	var got domain.CDCApplyRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("missing auth header")
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(domain.CDCApplyResult{Applied: len(got.Events), PositionType: "BINLOG", PositionValue: "bin.1:10"})
	}))
	defer ts.Close()
	out, err := postBatch(ts.Client(), ts.URL, "secret", domain.CDCApplyRequest{Direction: "forward", Events: []domain.CDCEvent{{Operation: domain.CDCInsert, PositionValue: "bin.1:10"}}})
	if err != nil {
		t.Fatal(err)
	}
	if out.Applied != 1 || got.Direction != "forward" {
		t.Fatalf("unexpected result out=%+v request=%+v", out, got)
	}
}
