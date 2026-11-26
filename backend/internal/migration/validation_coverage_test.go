package migration

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"dts/backend/internal/domain"
)

func TestHistoricalPlanCoverageBeforeValidationAndCutover(t *testing.T) {
	const a = `[{"Raw":"YQ=="}]`
	const b = `[{"Raw":"Yg=="}]`
	for _, tc := range []struct {
		name, split, strategy string
		ranges                [][2]int64
		edges                 [][2]string
		valid                 bool
	}{
		{name: "out of order BIGINT halves", split: "PK_RANGE_ADAPTIVE", ranges: [][2]int64{{0, math.MaxInt64}, {math.MinInt64, -1}}, valid: true},
		{name: "missing original suffix", split: "PK_RANGE_ADAPTIVE", ranges: [][2]int64{{math.MinInt64, 0}}},
		{name: "one missing key", split: "PK_RANGE_REBALANCED", ranges: [][2]int64{{math.MinInt64, -1}, {1, math.MaxInt64}}},
		{name: "overlap", split: "PRIMARY_KEY_RANGE", ranges: [][2]int64{{math.MinInt64, 0}, {0, math.MaxInt64}}},
		{name: "unrestricted keyset", split: "PRIMARY_KEY_KEYSET", edges: [][2]string{{a, b}, {b, ""}, {"", a}}, valid: true},
		{name: "missing keyset suffix", split: "PRIMARY_KEY_KEYSET", edges: [][2]string{{"", a}}},
		{name: "detached keyset cycle", split: "PRIMARY_KEY_KEYSET", edges: [][2]string{{"", ""}, {a, b}, {b, a}}},
		{name: "legitimate partition ranges", split: "PARTITION_RANGE", strategy: "PARTITION", ranges: [][2]int64{{math.MinInt64, math.MaxInt64}, {math.MinInt64, math.MaxInt64}}, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			count := max(len(tc.ranges), len(tc.edges))
			s, r := validationRunFixture(t, tc.split, count, count)
			tbl, err := r.GetMigrationTable(ctx, "table")
			if err != nil {
				t.Fatal(err)
			}
			tbl.MinPK, tbl.MaxPK, tbl.SplitStrategy = math.MinInt64, math.MaxInt64, tc.strategy
			if err := r.UpdateMigrationTable(ctx, tbl); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < count; i++ {
				ch, err := r.GetChunk(ctx, fmt.Sprintf("chunk-%d", i))
				if err != nil {
					t.Fatal(err)
				}
				if len(tc.ranges) > 0 {
					ch.Start, ch.End = tc.ranges[i][0], tc.ranges[i][1]
				} else {
					ch.StartCursorJSON, ch.EndCursorJSON = tc.edges[i][0], tc.edges[i][1]
				}
				if err := r.UpdateChunk(ctx, ch); err != nil {
					t.Fatal(err)
				}
				result := domain.ValidationResult{ID: "old-" + ch.ID, ChunkID: ch.ID, TaskID: ch.TaskID, TableID: ch.TableID, Status: domain.ValidationSuccess}
				if err := r.CreateValidationResult(ctx, &result); err != nil {
					t.Fatal(err)
				}
			}
			task, err := r.GetMigration(ctx, "task")
			if err != nil {
				t.Fatal(err)
			}
			err = s.requireSuccessfulValidation(ctx, task)
			if tc.valid && err != nil || !tc.valid && !errors.Is(err, errIncompleteRangeCoverage) {
				t.Fatalf("old evidence gate: %v", err)
			}
			if err := s.validateTask(ctx, "task"); err != nil {
				t.Fatal(err)
			}
			if tc.valid {
				expectValidationState(t, r, domain.StatusFinished, count)
			} else {
				expectValidationState(t, r, domain.StatusFailed, 0)
			}
		})
	}
}
