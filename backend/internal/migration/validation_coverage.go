package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

var errIncompleteRangeCoverage = errors.New("incomplete migration range coverage")

func plainTableRanges(t domain.MigrationTable) bool {
	switch normalizedSplitStrategy(t.SplitStrategy) {
	case "AUTO", "PRIMARY_KEY_RANGE", "UNIQUE_KEY_RANGE":
		// Partition names and hash bucket hints on AUTO mappings are ignored
		// by the planner. The chunk split type identifies the physical plan.
		return t.CustomWhere == ""
	}
	return false
}

func (s *Service) checkTableRangeCoverage(ctx context.Context, t domain.MigrationTable, splitType string) error {
	if !plainTableRanges(t) {
		return nil
	}
	if numericRangeSplit(splitType) {
		ok, err := s.repo.NumericRangeCoverage(ctx, t)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w for table %s: numeric chunks must cover [%d,%d] exactly once", errIncompleteRangeCoverage, t.ID, t.MinPK, t.MaxPK)
		}
		return nil
	}
	if splitType != "PRIMARY_KEY_KEYSET" {
		return nil
	}
	// Compare canonical tuple encodings only for equality. The source database
	// owns their ordering and collation. Keep just the endpoints while paging.
	width := len(t.PrimaryKeys)
	if width == 0 && t.PrimaryKey != "" {
		width = 1
	}
	encode := func(raw string) (string, error) {
		if raw == "" {
			return "", nil
		}
		values, err := connector.DecodeKeysetBound(raw, width)
		if err != nil {
			return "", err
		}
		if len(values) == 0 {
			return "", errors.New("empty stored keyset endpoint")
		}
		b, err := json.Marshal(values)
		return string(b), err
	}
	edges := make(map[string]string)
	afterNo, afterID := -1, ""
	limit := validationPageSize()
	for {
		page, err := repository.ListTableChunksPage(ctx, s.repo, t.TaskID, t.ID, afterNo, afterID, limit)
		if err != nil {
			return err
		}
		for _, piece := range page {
			lower, le := encode(piece.StartCursorJSON)
			upper, ue := encode(piece.EndCursorJSON)
			if piece.SplitType != "PRIMARY_KEY_KEYSET" || le != nil || ue != nil {
				return fmt.Errorf("%w for table %s: invalid keyset bounds for chunk %s", errIncompleteRangeCoverage, t.ID, piece.ID)
			}
			if _, dup := edges[lower]; dup {
				return fmt.Errorf("%w for table %s: duplicate keyset lower bound", errIncompleteRangeCoverage, t.ID)
			}
			edges[lower] = upper
		}
		if len(page) < limit {
			break
		}
		last := page[len(page)-1]
		afterNo, afterID = last.ChunkNo, last.ID
	}
	// The chain must reach the unbounded upper end and consume every edge.
	// This also rejects a detached cycle even when all its endpoints match.
	for at := ""; ; {
		next, ok := edges[at]
		if !ok {
			break
		}
		delete(edges, at)
		if next == "" {
			if len(edges) == 0 {
				return nil
			}
			break
		}
		at = next
	}
	return fmt.Errorf("%w for table %s: keyset ranges do not form one open-ended chain", errIncompleteRangeCoverage, t.ID)
}
