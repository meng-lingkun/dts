package perfmodel

import "testing"

func TestModel(t *testing.T) {
	if got := EWMA(100, 200, 25); got != 125 {
		t.Fatalf("ewma=%d", got)
	}
	if got := RecommendChunkRows(10000, 30, 1000, 1_000_000); got != 300000 {
		t.Fatalf("recommend=%d", got)
	}
	if got := BoundHistoricalRows(900000, 100000, .25, 4); got != 400000 {
		t.Fatalf("bound=%d", got)
	}
	if got := PiecesForRows(1_000_000, 250_000, 16); got != 4 {
		t.Fatalf("pieces=%d", got)
	}
}

func TestPercentileAndTopologyHealth(t *testing.T) {
	s := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	if got := Percentile(s, 95); got != 100 {
		t.Fatalf("p95=%d", got)
	}
	if got := TopologyHealth(40, 100, 100000, 2, 5); got != "DEGRADED" {
		t.Fatalf("health=%s", got)
	}
	if got := TopologyHealth(40, 100, 100000, 5, 6); got != "CIRCUIT_OPEN" {
		t.Fatalf("health=%s", got)
	}
	if got := TopologyHealth(40, 100, 100000, 5, 2); got != "HEALTHY" {
		t.Fatalf("low sample health=%s", got)
	}
}

func TestRC35TopologySchedulingPolicyAndTailRisk(t *testing.T) {
	if got := NormalizeTopologyHealth(" half_open "); got != "HALF_OPEN" {
		t.Fatalf("normalize=%s", got)
	}
	if got := TopologyHealthRank("CIRCUIT_OPEN"); got != 3 {
		t.Fatalf("rank=%d", got)
	}
	if got := TopologySchedulingWeight("DEGRADED"); got != 50 {
		t.Fatalf("weight=%d", got)
	}
	if got := TopologyConcurrencyCap("DEGRADED", 0, 2); got != 2 {
		t.Fatalf("degraded cap=%d", got)
	}
	if got := TopologyConcurrencyCap("HALF_OPEN", 0, 4); got != 1 {
		t.Fatalf("half-open cap=%d", got)
	}
	if got := TopologyConcurrencyCap("CIRCUIT_OPEN", 8, 4); got != 0 {
		t.Fatalf("open cap=%d", got)
	}
	// 60s P99 vs a 30s target with 50% weight adds a 50% ETA penalty.
	if got := TailRiskETA(1000, 30000, 60000, 50); got != 1500 {
		t.Fatalf("tail eta=%d", got)
	}
	if got := TopologyHealthWithThreshold(40, 100, 50000, 5, 10, 45000); got != "CIRCUIT_OPEN" {
		t.Fatalf("threshold health=%s", got)
	}
}
