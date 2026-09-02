package perfmodel

import (
	"math"
	"sort"
	"strings"
)

func EWMA(previous, current int64, alphaPct int) int64 {
	if current <= 0 {
		return previous
	}
	if previous <= 0 {
		return current
	}
	if alphaPct < 1 {
		alphaPct = 1
	}
	if alphaPct > 100 {
		alphaPct = 100
	}
	return (previous*int64(100-alphaPct) + current*int64(alphaPct)) / 100
}

func RecommendChunkRows(rowsPerSec int64, targetSeconds int, minRows, maxRows int64) int64 {
	if rowsPerSec <= 0 || targetSeconds <= 0 {
		return 0
	}
	n := rowsPerSec * int64(targetSeconds)
	if minRows > 0 && n < minRows {
		n = minRows
	}
	if maxRows > 0 && n > maxRows {
		n = maxRows
	}
	return n
}

func BoundHistoricalRows(recommended, baseline int64, minFactor, maxFactor float64) int64 {
	if recommended <= 0 {
		return baseline
	}
	if baseline <= 0 {
		return recommended
	}
	lo := int64(math.Round(float64(baseline) * minFactor))
	hi := int64(math.Round(float64(baseline) * maxFactor))
	if lo < 1 {
		lo = 1
	}
	if hi < lo {
		hi = lo
	}
	if recommended < lo {
		return lo
	}
	if recommended > hi {
		return hi
	}
	return recommended
}

func PiecesForRows(estimatedRows, recommendedRows int64, maxPieces int) int {
	if estimatedRows <= 0 || recommendedRows <= 0 {
		return 1
	}
	p := int((estimatedRows + recommendedRows - 1) / recommendedRows)
	if p < 1 {
		p = 1
	}
	if maxPieces > 0 && p > maxPieces {
		p = maxPieces
	}
	return p
}

// AppendSample keeps only the newest max samples so topology latency profiles stay bounded.
func AppendSample(samples []int64, value int64, max int) []int64 {
	if value <= 0 {
		return samples
	}
	if max <= 0 {
		max = 64
	}
	out := append(append([]int64(nil), samples...), value)
	if len(out) > max {
		out = append([]int64(nil), out[len(out)-max:]...)
	}
	return out
}

func Percentile(samples []int64, pct int) int64 {
	if len(samples) == 0 {
		return 0
	}
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	v := append([]int64(nil), samples...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	idx := int(math.Ceil(float64(pct)*float64(len(v))/100.0)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(v) {
		idx = len(v) - 1
	}
	return v[idx]
}

// TopologyHealth requires repeated evidence before degrading a placement. It intentionally
// returns HEALTHY for low-sample profiles to avoid poisoning a new topology from one slow chunk.
func TopologyHealth(topologyBPS, tableBPS, p99MS int64, slowStreak, samples int) string {
	return TopologyHealthWithThreshold(topologyBPS, tableBPS, p99MS, slowStreak, samples, 90000)
}

func NormalizeTopologyHealth(health string) string {
	switch strings.ToUpper(strings.TrimSpace(health)) {
	case "DEGRADED":
		return "DEGRADED"
	case "CIRCUIT_OPEN":
		return "CIRCUIT_OPEN"
	case "HALF_OPEN":
		return "HALF_OPEN"
	default:
		return "HEALTHY"
	}
}

func TopologySchedulingWeight(health string) int {
	switch NormalizeTopologyHealth(health) {
	case "HEALTHY":
		return 100
	case "DEGRADED":
		return 50
	case "HALF_OPEN":
		return 10
	case "CIRCUIT_OPEN":
		return 0
	default:
		return 100
	}
}

func TopologyHealthRank(health string) int {
	switch NormalizeTopologyHealth(health) {
	case "HEALTHY":
		return 0
	case "DEGRADED":
		return 1
	case "HALF_OPEN":
		return 2
	case "CIRCUIT_OPEN":
		return 3
	default:
		return 0
	}
}

// TopologyConcurrencyCap returns 0 for an open circuit, 1 for a half-open probe,
// a bounded cap for degraded placements, and the configured healthy cap for a
// normal placement. A healthyCap <= 0 means unlimited.
func TopologyConcurrencyCap(health string, healthyCap, degradedCap int) int {
	switch NormalizeTopologyHealth(health) {
	case "CIRCUIT_OPEN":
		return 0
	case "HALF_OPEN":
		return 1
	case "DEGRADED":
		if degradedCap <= 0 {
			return 1
		}
		return degradedCap
	default:
		return healthyCap
	}
}

func TopologyHealthWithThreshold(topologyBPS, tableBPS, p99MS int64, slowStreak, samples int, slowP99MS int64) string {
	if samples < 3 {
		return "HEALTHY"
	}
	if slowP99MS <= 0 {
		slowP99MS = 90000
	}
	badThroughput := tableBPS > 0 && topologyBPS*100 < tableBPS*55
	badTail := p99MS >= slowP99MS
	if badThroughput || badTail {
		if slowStreak >= 5 {
			return "CIRCUIT_OPEN"
		}
		if slowStreak >= 2 {
			return "DEGRADED"
		}
	}
	return "HEALTHY"
}

// TailRiskETA inflates a base ETA when topology tail latency is above the
// target chunk duration. weightPct controls how much of the tail excess is
// reflected in the task ETA and is bounded to [0,100].
func TailRiskETA(baseETASeconds, targetChunkMS, percentileMS int64, weightPct int) int64 {
	if baseETASeconds <= 0 {
		return 0
	}
	if targetChunkMS <= 0 || percentileMS <= targetChunkMS {
		return baseETASeconds
	}
	if weightPct < 0 {
		weightPct = 0
	}
	if weightPct > 100 {
		weightPct = 100
	}
	excessPct := (percentileMS - targetChunkMS) * 100 / targetChunkMS
	penaltyPct := excessPct * int64(weightPct) / 100
	return baseETASeconds * (100 + penaltyPct) / 100
}
