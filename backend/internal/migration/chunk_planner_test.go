package migration

import (
	"context"
	"math"
	"sort"
	"testing"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository/memory"
)

func TestPlanIntegerRange(t *testing.T) {
	chunks, err := PlanIntegerRange(1, 25, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	want := [][2]int64{{1, 10}, {11, 20}, {21, 25}}
	for i, c := range chunks {
		if c.Start != want[i][0] || c.End != want[i][1] {
			t.Fatalf("chunk %d got %d-%d", i, c.Start, c.End)
		}
	}
}
func TestPlanIntegerRangeByRowsSparse(t *testing.T) {
	chunks, err := PlanIntegerRangeByRows(1, 1000, 100, 25)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(chunks))
	}
	if chunks[0].Start != 1 || chunks[0].End != 250 || chunks[3].End != 1000 {
		t.Fatalf("unexpected chunks: %+v", chunks)
	}
}
func TestPlanIntegerRangeRejectsInvalidInput(t *testing.T) {
	if _, err := PlanIntegerRange(10, 1, 10); err == nil {
		t.Fatal("expected range error")
	}
	if _, err := PlanIntegerRange(1, 10, 0); err == nil {
		t.Fatal("expected span error")
	}
	if _, err := PlanIntegerRangeByRows(1, 10, 10, 0); err == nil {
		t.Fatal("expected targetRows error")
	}
}

func TestPlanIntegerRangeFullDomain(t *testing.T) {
	chunks, err := PlanIntegerRange(math.MinInt64, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	want := [][2]int64{{math.MinInt64, -2}, {-1, math.MaxInt64 - 2}, {math.MaxInt64 - 1, math.MaxInt64}}
	if len(chunks) != len(want) {
		t.Fatalf("got %d chunks", len(chunks))
	}
	for i, c := range chunks {
		if c.Start != want[i][0] || c.End != want[i][1] {
			t.Errorf("chunk %d = [%d, %d], want %v", i, c.Start, c.End, want[i])
		}
	}
	if _, err := PlanIntegerRange(math.MinInt64, math.MaxInt64, 1); err == nil {
		t.Fatal("accepted a fixed-span plan that cannot be materialized")
	}
}

func TestPlanIntegerRangeByRowsExtremeCoverage(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		low, high, rows, target int64
		count                   int
	}{
		{"single full domain", math.MinInt64, math.MaxInt64, 1, 1, 1},
		{"two halves", math.MinInt64, math.MaxInt64, math.MaxInt64, math.MaxInt64 - 1, 2},
		{"past floating point precision", -4, 4, 1<<53 + 1, 1 << 53, 2},
		{"absent estimate", math.MinInt64, math.MaxInt64, 0, 1, maxKeysetPartitions},
		{"large estimate", math.MinInt64, math.MaxInt64, math.MaxInt64, 1, maxInitialRangeChunks},
		{"more rows than keys", math.MaxInt64 - 2, math.MaxInt64, 100, 1, 3},
		{"single minimum key", math.MinInt64, math.MinInt64, 0, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chunks, err := PlanIntegerRangeByRows(tc.low, tc.high, tc.rows, tc.target)
			if err != nil || len(chunks) != tc.count {
				t.Fatalf("count=%d, want %d, err=%v", len(chunks), tc.count, err)
			}
			for i, c := range chunks {
				if c.Start > c.End || c.ChunkNo != i+1 || i > 0 && c.Start != chunks[i-1].End+1 {
					t.Fatalf("invalid chunk %d [%d,%d]", c.ChunkNo, c.Start, c.End)
				}
			}
			if chunks[0].Start != tc.low || chunks[len(chunks)-1].End != tc.high {
				t.Fatal("incomplete coverage")
			}
			if tc.name == "two halves" && chunks[0].End != -1 {
				t.Fatalf("first half ends at %d", chunks[0].End)
			}
		})
	}
}

func TestPlanBoundedKeyset(t *testing.T) {
	bounds := [][]connector.Value{
		{{Raw: []byte("m")}},
		{{Raw: []byte("t")}},
	}
	chunks, err := PlanBoundedKeyset(bounds)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	if chunks[0].StartCursorJSON != "" || chunks[0].EndCursorJSON == "" {
		t.Fatalf("unexpected first chunk bounds: %+v", chunks[0])
	}
	if chunks[1].StartCursorJSON != chunks[0].EndCursorJSON || chunks[1].EndCursorJSON == "" {
		t.Fatalf("middle chunk is not contiguous: %+v", chunks[1])
	}
	if chunks[2].StartCursorJSON != chunks[1].EndCursorJSON || chunks[2].EndCursorJSON != "" {
		t.Fatalf("unexpected last chunk bounds: %+v", chunks[2])
	}
}

func TestExtremeNumericYieldAndAdaptiveRanges(t *testing.T) {
	t.Setenv("DTS_RUNNING_CHUNK_REBALANCE_MAX_PIECES", "2")
	t.Setenv("DTS_ADAPTIVE_CHUNK_TARGET_SECONDS", "1")
	t.Setenv("DTS_ADAPTIVE_CHUNK_HARD_MAX_SPLITS", "1")
	ctx := context.Background()
	repo := memory.New()
	svc := NewService(repo, connector.NewRegistry())
	task := domain.MigrationTask{ID: "task", Mode: domain.ModeFull, Status: domain.StatusFullMigrating, Parallelism: 2}
	table := domain.MigrationTable{ID: "table", TaskID: task.ID, Engine: "native", PrimaryKey: "id", MinPK: math.MinInt64, MaxPK: math.MaxInt64, TotalChunks: 2}
	running := domain.MigrationChunk{ID: "running", TableID: table.ID, TaskID: task.ID, PrimaryKey: "id", SplitType: "PK_RANGE", Start: math.MinInt64, End: math.MaxInt64, Status: domain.ChunkRunning}
	children, err := svc.buildYieldRemainders(ctx, &task, &table, &running, `{"after_pk":-9223372036854775808}`, 1)
	if err != nil || running.End != math.MinInt64 || len(children) != 2 {
		t.Fatalf("yielded end=%d, child count=%d, err=%v", running.End, len(children), err)
	}
	check := func(a, b domain.MigrationChunk) {
		t.Helper()
		if a.Start != math.MinInt64+1 || a.End != 0 || b.Start != 1 || b.End != math.MaxInt64 {
			t.Fatalf("got [%d,%d] and [%d,%d]", a.Start, a.End, b.Start, b.End)
		}
	}
	check(children[0], children[1])
	if err := repo.CreateMigration(ctx, &task); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateMigrationTable(ctx, &table); err != nil {
		t.Fatal(err)
	}
	done := domain.MigrationChunk{ID: "done", ChunkNo: 1, TaskID: task.ID, TableID: table.ID, PrimaryKey: "id", SplitType: "PK_RANGE", Start: math.MinInt64, End: math.MinInt64, Status: domain.ChunkSuccess}
	pending := domain.MigrationChunk{ID: "pending", ChunkNo: 2, TaskID: task.ID, TableID: table.ID, PrimaryKey: "id", SplitType: "PK_RANGE", Start: math.MinInt64 + 1, End: math.MaxInt64, Status: domain.ChunkPending}
	if err := repo.CreateChunks(ctx, []domain.MigrationChunk{done, pending}); err != nil {
		t.Fatal(err)
	}
	if err := svc.adaptPendingChunks(ctx, &done, domain.ChunkResult{DurationMS: 3000}); err != nil {
		t.Fatal(err)
	}
	chunks, err := repo.ListChunks(ctx, task.ID)
	if err != nil || len(chunks) != 3 {
		t.Fatalf("adaptive chunk count=%d err=%v", len(chunks), err)
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Start < chunks[j].Start })
	check(chunks[1], chunks[2])
}

func TestEstimateNumericRowsAcrossEntireDomain(t *testing.T) {
	table := &domain.MigrationTable{MinPK: math.MinInt64, MaxPK: math.MaxInt64, EstimatedRows: math.MaxInt64}
	for _, tc := range []struct{ low, high, want int64 }{
		{math.MinInt64, math.MaxInt64, math.MaxInt64},
		{math.MinInt64, -1, 1 << 62},
		{0, math.MaxInt64, 1 << 62},
		{math.MaxInt64, math.MaxInt64, 1},
	} {
		if got := estimateNumericChunkRows(table, &domain.MigrationChunk{Start: tc.low, End: tc.high}); got != tc.want {
			t.Errorf("estimated rows for [%d,%d] = %d, want %d", tc.low, tc.high, got, tc.want)
		}
	}
}

func TestDesiredKeysetPartitions(t *testing.T) {
	if got := DesiredKeysetPartitions(10, 100, 8); got != 1 {
		t.Fatalf("small table partitions=%d", got)
	}
	if got := DesiredKeysetPartitions(1000000, 1000, 4); got != 16 {
		t.Fatalf("worker cap partitions=%d", got)
	}
	if got := DesiredKeysetPartitions(1000000000, 1, 100); got != 128 {
		t.Fatalf("hard cap partitions=%d", got)
	}
	if got := DesiredKeysetPartitions(math.MaxInt64, math.MaxInt64-1, math.MaxInt); got != 2 {
		t.Fatalf("extreme estimate partitions=%d", got)
	}
}
