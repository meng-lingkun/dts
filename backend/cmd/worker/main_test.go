package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"dts/backend/internal/domain"
)

func TestResumeFilteredNumericChunk(t *testing.T) {
	for _, split := range []string{"PARTITION_RANGE", "CUSTOM_SQL_RANGE", "PRIMARY_KEY_RANGE"} {
		t.Run(split, func(t *testing.T) {
			job := &domain.ChunkJob{}
			job.Chunk = domain.MigrationChunk{SplitType: split, Start: -3, End: 9, CursorJSON: `{"after_pk":0}`, PartitionName: "p0", CustomWhere: "active = 1"}
			r, err := chunkReadRequest(job)
			if err != nil || !r.HasAfter || r.AfterPK != 0 || r.Partition != "p0" || r.CustomWhere != "active = 1" {
				t.Fatalf("resumed read: %+v %v", r, err)
			}
			job.Chunk.CursorJSON = `{"after_pk":12}`
			if _, err := chunkReadRequest(job); err == nil {
				t.Fatal("out-of-range checkpoint accepted")
			}
		})
	}
}

func TestThrottleStopsWhenLeaseContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := throttleUnits(ctx, 5000, 1, time.Now()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("throttle cancellation: %v", err)
	}
}

func TestParseWorkerLabels(t *testing.T) {
	got := parseWorkerLabels(`zone=az-a,rack=r2`)
	if got["zone"] != "az-a" || got["rack"] != "r2" {
		t.Fatalf("labels=%v", got)
	}
	got = parseWorkerLabels(`{"region":"cn-hangzhou","network":"migration"}`)
	if got["region"] != "cn-hangzhou" || got["network"] != "migration" {
		t.Fatalf("json labels=%v", got)
	}
}
