package migration

import (
	"encoding/json"
	"errors"
	"fmt"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
)

const (
	maxKeysetPartitions = 128
	// A table's initial range plan and its metadata are materialized together.
	// Limit that allocation even if a source reports an enormous row estimate.
	maxInitialRangeChunks = 16_384
)

// PlanIntegerRange splits an inclusive signed integer PK range by key span.
func PlanIntegerRange(minPK, maxPK, span int64) ([]domain.MigrationChunk, error) {
	if span <= 0 {
		return nil, errors.New("span must be greater than zero")
	}
	if minPK > maxPK {
		return nil, errors.New("minPK must be <= maxPK")
	}
	// The distance fits uint64 even when the inclusive cardinality is 2^64.
	distance := uint64(maxPK) - uint64(minPK)
	extraChunks := distance / uint64(span)
	if extraChunks >= maxInitialRangeChunks {
		return nil, fmt.Errorf("span %d would require more than %d initial range chunks", span, maxInitialRangeChunks)
	}
	return integerRangeChunks(minPK, maxPK, uint64(span-1), int(extraChunks)+1), nil
}

func integerRangeChunks(minPK, maxPK int64, spanMinusOne uint64, capacity int) []domain.MigrationChunk {
	chunks := make([]domain.MigrationChunk, 0, capacity)
	for start := minPK; ; {
		end := maxPK
		if uint64(maxPK)-uint64(start) > spanMinusOne {
			end = int64(uint64(start) + spanMinusOne)
		}
		chunks = append(chunks, domain.MigrationChunk{ChunkNo: len(chunks) + 1, SplitType: "PRIMARY_KEY_RANGE", Start: start, End: end, Status: domain.ChunkPending})
		if end == maxPK {
			break
		}
		start = end + 1
	}
	return chunks
}

type integerRange struct{ start, end int64 }

// Split an existing inclusive range into nonempty parts whose widths differ
// by at most one. Unlike a fixed-span plan this yields exactly the requested
// number of parts whenever there are enough keys.
func balancedIntegerRanges(low, high int64, pieces int) []integerRange {
	if pieces <= 1 || low == high {
		return []integerRange{{low, high}}
	}
	distance := uint64(high) - uint64(low)
	pieces = int(min(uint64(pieces-1), distance)) + 1
	// Distribute distance+1 keys without forming that potentially 65-bit sum.
	base, extra := distance/uint64(pieces), distance%uint64(pieces)+1
	ranges := make([]integerRange, pieces)
	start := low
	for i := range ranges {
		width := base
		if uint64(i) < extra {
			width++
		}
		end := int64(uint64(start) + width - 1)
		ranges[i] = integerRange{start, end}
		if end < high {
			start = end + 1
		}
	}
	return ranges
}

// PlanIntegerRangeByRows estimates a PK span that targets approximately targetRows
// per chunk while remaining robust to sparse integer primary keys.
func PlanIntegerRangeByRows(minPK, maxPK, estimatedRows, targetRows int64) ([]domain.MigrationChunk, error) {
	if targetRows <= 0 {
		return nil, errors.New("targetRows must be greater than zero")
	}
	if minPK > maxPK {
		return nil, errors.New("minPK must be <= maxPK")
	}
	distance := uint64(maxPK) - uint64(minPK)
	// Without statistics the key domain can be very sparse. Use the same
	// bound on speculative work as keyset planning.
	extraChunks := min(distance/uint64(targetRows), uint64(maxKeysetPartitions-1))
	if estimatedRows > 0 {
		extraChunks = min(uint64((estimatedRows-1)/targetRows), uint64(maxInitialRangeChunks-1))
	}
	count := min(extraChunks, distance) + 1
	// ceil((distance+1)/count)-1 equals distance/count without overflow.
	return integerRangeChunks(minPK, maxPK, distance/count, int(count)), nil
}

// PlanBoundedKeyset converts ordered split points into gap-free [lower, upper)
// chunks. Runtime CursorJSON remains independent from immutable chunk bounds.
func PlanBoundedKeyset(boundaries [][]connector.Value) ([]domain.MigrationChunk, error) {
	chunks := make([]domain.MigrationChunk, 0, len(boundaries)+1)
	var lower []connector.Value
	for i := 0; i <= len(boundaries); i++ {
		chunk := domain.MigrationChunk{ChunkNo: i + 1, SplitType: "PRIMARY_KEY_KEYSET", Status: domain.ChunkPending}
		if len(lower) > 0 {
			b, err := json.Marshal(lower)
			if err != nil {
				return nil, err
			}
			chunk.StartCursorJSON = string(b)
		}
		if i < len(boundaries) {
			upper := boundaries[i]
			if len(upper) == 0 {
				return nil, errors.New("keyset boundary cannot be empty")
			}
			b, err := json.Marshal(upper)
			if err != nil {
				return nil, err
			}
			chunk.EndCursorJSON = string(b)
			lower = upper
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

// DesiredKeysetPartitions caps boundary planning so a large table has enough
// queued work to smooth skew without creating thousands of planning partitions.
func DesiredKeysetPartitions(estimatedRows, targetRows int64, parallelism int) int {
	if estimatedRows <= 0 {
		return 1
	}
	if targetRows <= 0 {
		targetRows = 100000
	}
	if parallelism <= 0 {
		parallelism = 1
	}
	byRows := (estimatedRows-1)/targetRows + 1
	capByWorkers := min(parallelism, maxKeysetPartitions/4) * 4
	return int(min(byRows, int64(capByWorkers)))
}
