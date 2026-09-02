package migration

import (
	"encoding/json"
	"errors"
	"math"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

// PlanIntegerRange splits an inclusive signed integer PK range by key span.
func PlanIntegerRange(minPK, maxPK, span int64) ([]domain.MigrationChunk, error) {
	if span <= 0 {
		return nil, errors.New("span must be greater than zero")
	}
	if minPK > maxPK {
		return nil, errors.New("minPK must be <= maxPK")
	}
	chunks := make([]domain.MigrationChunk, 0)
	start := minPK
	chunkNo := 1
	for start <= maxPK {
		end := start + span - 1
		if end < start || end > maxPK {
			end = maxPK
		}
		chunks = append(chunks, domain.MigrationChunk{ChunkNo: chunkNo, SplitType: "PRIMARY_KEY_RANGE", Start: start, End: end, Status: domain.ChunkPending})
		if end == maxPK {
			break
		}
		start = end + 1
		chunkNo++
	}
	return chunks, nil
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
	if estimatedRows <= 0 {
		estimatedRows = maxPK - minPK + 1
	}
	chunkCount := int64(math.Ceil(float64(estimatedRows) / float64(targetRows)))
	if chunkCount < 1 {
		chunkCount = 1
	}
	rangeSize := maxPK - minPK + 1
	if rangeSize <= 0 {
		return nil, errors.New("primary key range overflow")
	}
	span := rangeSize / chunkCount
	if rangeSize%chunkCount != 0 {
		span++
	}
	if span < 1 {
		span = 1
	}
	return PlanIntegerRange(minPK, maxPK, span)
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
	byRows := int((estimatedRows + targetRows - 1) / targetRows)
	if byRows < 1 {
		byRows = 1
	}
	capByWorkers := parallelism * 4
	if capByWorkers < 1 {
		capByWorkers = 1
	}
	if capByWorkers > 128 {
		capByWorkers = 128
	}
	if byRows > capByWorkers {
		return capByWorkers
	}
	return byRows
}
