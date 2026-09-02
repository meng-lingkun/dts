package db2log

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"qmigration/backend/internal/domain"
	"testing"
)

func TestPureScaleReaderRequiresProofAndGroupsGlobalSequence(t *testing.T) {
	lineage := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	streams := []PureScaleStream{{ClusterID: "c", CaptureLineage: lineage, StreamID: "s1", MemberID: "m1", Epoch: 1, ResolvedGlobalSequence: 11, CurrentLRI: LRI{1, 1, 1}}, {ClusterID: "c", CaptureLineage: lineage, StreamID: "s2", MemberID: "m2", Epoch: 1, ResolvedGlobalSequence: 11, CurrentLRI: LRI{1, 1, 1}}}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/streams", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(PureScaleStreamsResponse{Streams: streams})
	})
	mux.HandleFunc("/v1/streams/", func(w http.ResponseWriter, r *http.Request) {
		id := "s1"
		if r.URL.Path == "/v1/streams/s2/records" {
			id = "s2"
		}
		st := streams[0]
		if id == "s2" {
			st = streams[1]
		}
		json.NewEncoder(w).Encode(PureScaleReadResponse{Stream: st, Fragments: []PureScaleFragment{{StreamID: id, Epoch: 1, GlobalSequence: 10, TID: id, NextLRI: LRI{1, 1, 2}, CompleteRowImages: true, Events: []domain.CDCEvent{{Operation: domain.CDCInsert, SourceSchema: "A", SourceTable: "T", After: []domain.CDCField{{Column: "ID", Value: "1"}}}}}}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := NewClient(srv.URL, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewPureScaleReader(context.Background(), c, nil, []Selection{{Schema: "A", Table: "T"}}, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 2 || tx.Checkpoint.PositionType != "DB2_PURESCALE_VECTOR" {
		t.Fatalf("tx=%+v", tx)
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
}
