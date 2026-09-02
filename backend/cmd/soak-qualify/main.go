package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"qmigration/backend/internal/perfmodel"
)

type scenario struct {
	Name                 string  `json:"name"`
	VirtualTB            float64 `json:"virtual_tb"`
	Workers              int     `json:"workers"`
	Samples              int     `json:"samples"`
	ExpectedRowsPerSec   int64   `json:"expected_rows_per_sec"`
	LearnedRowsPerSec    int64   `json:"learned_rows_per_sec"`
	RecommendedChunkRows int64   `json:"recommended_chunk_rows"`
	EstimatedHours       float64 `json:"estimated_hours"`
	Qualified            bool    `json:"qualified"`
}

type report struct {
	Version   string     `json:"version"`
	Kind      string     `json:"kind"`
	Synthetic bool       `json:"synthetic"`
	Qualified bool       `json:"qualified"`
	Scenarios []scenario `json:"scenarios"`
}

func run(name string, tb float64, workers, samples int, baseRPS int64) scenario {
	var learned int64
	for i := 0; i < samples; i++ {
		// Deterministic +/-12% topology/load modulation exercises convergence without randomness.
		wave := float64((i%17)-8) / 8.0
		current := int64(float64(baseRPS) * (1.0 + wave*0.12))
		learned = perfmodel.EWMA(learned, current, 25)
	}
	rec := perfmodel.RecommendChunkRows(learned, 30, 1000, 10_000_000)
	bytes := tb * 1024 * 1024 * 1024 * 1024
	// Synthetic row width is 1024 bytes. Aggregate throughput scales by workers but caps at 2 GiB/s
	// to keep the virtual model conservative and comparable across runs.
	bps := float64(learned * 1024 * int64(workers))
	capBPS := float64(2 * 1024 * 1024 * 1024)
	if bps > capBPS {
		bps = capBPS
	}
	hours := bytes / bps / 3600
	delta := math.Abs(float64(learned-baseRPS)) / float64(baseRPS)
	latency := make([]int64, 0, 64)
	for i := 0; i < 64; i++ {
		latency = perfmodel.AppendSample(latency, int64(22000+(i%11)*1100), 64)
	}
	p99 := perfmodel.Percentile(latency, 99)
	return scenario{Name: name, VirtualTB: tb, Workers: workers, Samples: samples, ExpectedRowsPerSec: baseRPS, LearnedRowsPerSec: learned, RecommendedChunkRows: rec, EstimatedHours: hours, Qualified: delta < 0.08 && rec > 0 && hours > 0 && p99 < 90000}
}

func main() {
	samples := flag.Int("samples", 50000, "virtual profile samples per scenario")
	flag.Parse()
	if *samples < 100 {
		*samples = 100
	}
	r := report{Version: "0.15.0-rc49", Kind: "qmigration-synthetic-soak", Synthetic: true}
	r.Scenarios = []scenario{
		run("10TB-balanced", 10, 8, *samples, 70000),
		run("40TB-distributed", 40, 20, *samples, 55000),
	}
	r.Qualified = true
	for _, s := range r.Scenarios {
		if !s.Qualified {
			r.Qualified = false
		}
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
	if !r.Qualified {
		os.Exit(2)
	}
}
