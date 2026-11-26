package repository

import (
	"errors"
	"maps"
	"reflect"

	"dts/backend/internal/domain"
)

// PendingChunkSplittable excludes resumed chunks whose checkpoints already
// describe work within the original interval.
func PendingChunkSplittable(c domain.MigrationChunk) bool {
	return c.Status == domain.ChunkPending && c.WorkerID == "" && c.LeaseUntil.IsZero() &&
		c.CursorJSON == "" && c.RowsRead == 0 && c.RowsWritten == 0 && c.BytesRead == 0 && c.BytesWritten == 0
}

// CheckPendingSplit must run with the chunk locked until all pieces are
// committed. Claim, progress, topology and boundary changes invalidate a plan.
func CheckPendingSplit(expected, current domain.MigrationChunk, pieces []domain.MigrationChunk) error {
	if !PendingChunkSplittable(current) || !reflect.DeepEqual(expected, current) {
		return ErrConflict
	}
	if len(pieces) < 2 || pieces[0].ID != expected.ID || pieces[0].ChunkNo != expected.ChunkNo {
		return errors.New("invalid pending split head")
	}
	keyset := expected.SplitType == "PRIMARY_KEY_KEYSET"
	switch expected.SplitType {
	case "PRIMARY_KEY_KEYSET", "PRIMARY_KEY_RANGE", "PK_RANGE", "PK_RANGE_ADAPTIVE", "PK_RANGE_REBALANCED":
	default:
		return errors.New("unsupported pending split strategy")
	}
	first, last := pieces[0], pieces[len(pieces)-1]
	if (keyset && (first.StartCursorJSON != current.StartCursorJSON || last.EndCursorJSON != current.EndCursorJSON)) || (!keyset && (first.Start != current.Start || last.End != current.End)) {
		return errors.New("pending split does not cover original bounds")
	}
	ids := map[string]bool{}
	numbers := map[int]bool{}
	for i, ch := range pieces {
		if ch.ID == "" || ch.TableID != current.TableID || ch.TaskID != current.TaskID || ch.PrimaryKey != current.PrimaryKey || !PendingChunkSplittable(ch) || ids[ch.ID] || numbers[ch.ChunkNo] {
			return errors.New("invalid pending split piece")
		}
		if ch.CustomWhere != current.CustomWhere || ch.PartitionName != current.PartitionName ||
			ch.HashBucket != current.HashBucket || ch.HashBuckets != current.HashBuckets ||
			ch.TopologyID != current.TopologyID || ch.TopologyKind != current.TopologyKind ||
			!maps.Equal(ch.FaultDomain, current.FaultDomain) || !maps.Equal(ch.PlacementHint, current.PlacementHint) {
			return errors.New("pending split piece changes its source or placement")
		}
		ids[ch.ID], numbers[ch.ChunkNo] = true, true
		if keyset {
			if ch.SplitType != current.SplitType || (i > 0 && (ch.StartCursorJSON == "" || ch.StartCursorJSON != pieces[i-1].EndCursorJSON)) {
				return errors.New("noncontiguous keyset split")
			}
		} else if ch.SplitType != "PK_RANGE_ADAPTIVE" || ch.Start > ch.End || (i > 0 && (pieces[i-1].End >= ch.Start || pieces[i-1].End+1 != ch.Start)) {
			return errors.New("noncontiguous numeric split")
		}
	}
	return nil
}
