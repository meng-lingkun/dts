package migration

import (
	"qmigration/backend/internal/connector"
	"testing"
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
}
