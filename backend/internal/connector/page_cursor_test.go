package connector

import (
	"testing"

	"dts/backend/internal/domain"
)

func TestPageCursorOwnsCompositeKeysAndRejectsBadPages(t *testing.T) {
	req := ReadBatchRequest{UseKeyset: true, PrimaryKeys: []string{"id", "sub"}, Columns: []domain.ColumnInfo{{Name: "payload"}}, Limit: 8}
	page := RowBatch{Rows: [][]Value{{{}}}, LastKey: []Value{{Raw: []byte("a")}, {Raw: []byte("b")}}}
	if err := req.AdvancePage(&page); err != nil {
		t.Fatal(err)
	}
	page.LastKey[0].Raw[0] = 'c'
	page.Rows = append(page.Rows, nil)
	if err := req.AdvancePage(&page); err == nil || string(req.Cursor[0].Raw) != "a" {
		t.Fatalf("invalid page advanced the cursor: %+v, %v", req.Cursor, err)
	}
}

func TestNumericCursorStaysWithinFilteredRange(t *testing.T) {
	req := ReadBatchRequest{StartPK: -2, EndPK: 4, Partition: "p0", CustomWhere: "active = 1", Columns: []domain.ColumnInfo{{Name: "id"}}, Limit: 1}
	page := RowBatch{Rows: [][]Value{{{}}}, LastPK: 0}
	if err := req.AdvancePage(&page); err != nil || !req.HasAfter || req.AfterPK != 0 {
		t.Fatal(req, err)
	}
	for _, last := range []int64{-3, 0, 5} {
		page.LastPK = last
		if err := req.AdvancePage(&page); err == nil || req.AfterPK != 0 {
			t.Fatalf("invalid cursor %d: %+v %v", last, req, err)
		}
	}
}
