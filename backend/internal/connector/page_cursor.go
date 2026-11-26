package connector

import (
	"bytes"
	"errors"
	"fmt"
)

// AdvancePage validates a page before any of its rows are written or counted.
// An empty page ends the scan. Nonempty pages must provide the next cursor.
func (req *ReadBatchRequest) AdvancePage(batch *RowBatch) error {
	if batch == nil || len(batch.Rows) > req.Limit {
		return errors.New("connector returned an invalid batch size")
	}
	if len(batch.Rows) == 0 {
		return nil
	}
	for _, row := range batch.Rows {
		if len(row) != len(req.Columns) {
			return fmt.Errorf("connector returned %d values for %d columns", len(row), len(req.Columns))
		}
	}
	if !req.UseKeyset {
		if batch.LastPK < req.StartPK || batch.LastPK > req.EndPK {
			return errors.New("range connector returned a cursor outside its bounds")
		}
		if req.HasAfter && batch.LastPK <= req.AfterPK {
			return errors.New("range connector did not advance its cursor")
		}
		req.AfterPK, req.HasAfter = batch.LastPK, true
		return nil
	}
	width := len(req.PrimaryKeys)
	if width == 0 && req.PrimaryKey != "" {
		width = 1
	}
	if width == 0 || len(batch.LastKey) != width {
		return errors.New("keyset connector returned an invalid cursor width")
	}
	same := len(req.Cursor) == width
	next := make([]Value, width)
	for i, key := range batch.LastKey {
		if key.Null {
			return errors.New("keyset connector returned a null cursor field")
		}
		same = same && bytes.Equal(req.Cursor[i].Raw, key.Raw)
		next[i].Raw = bytes.Clone(key.Raw)
	}
	if same {
		return errors.New("keyset connector returned an unchanged cursor")
	}
	req.Cursor = next
	return nil
}
