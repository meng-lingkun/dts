package orderedmerge

import (
	"errors"
	"fmt"
	"sort"
)

// Fragment is one committed source transaction fragment from one member/primary.
// Order must be globally comparable across all participating streams.
type Fragment[T any] struct {
	Stream string
	Order  uint64
	Value  T
}

type Stream[T any] struct {
	ID        string
	Resolved  uint64
	Fragments []Fragment[T]
}

type Group[T any] struct {
	Order     uint64
	Fragments []Fragment[T]
}

// Merge releases only groups whose order is <= the minimum explicit resolved
// watermark of every stream. Empty/idle streams therefore need a watermark;
// the function never guesses that an idle stream cannot later produce an older
// transaction.
func Merge[T any](streams []Stream[T]) ([]Group[T], error) {
	if len(streams) == 0 {
		return nil, nil
	}
	seen := map[string]bool{}
	minResolved := ^uint64(0)
	all := make([]Fragment[T], 0)
	for _, s := range streams {
		if s.ID == "" {
			return nil, errors.New("ordered merge stream id is empty")
		}
		if seen[s.ID] {
			return nil, fmt.Errorf("duplicate ordered merge stream %q", s.ID)
		}
		seen[s.ID] = true
		if s.Resolved == 0 {
			return nil, fmt.Errorf("ordered merge stream %q has no resolved watermark", s.ID)
		}
		if s.Resolved < minResolved {
			minResolved = s.Resolved
		}
		last := uint64(0)
		for _, f := range s.Fragments {
			if f.Stream == "" {
				f.Stream = s.ID
			}
			if f.Stream != s.ID {
				return nil, fmt.Errorf("fragment stream %q does not match container %q", f.Stream, s.ID)
			}
			if f.Order == 0 {
				return nil, fmt.Errorf("stream %q has zero global order", s.ID)
			}
			if f.Order < last {
				return nil, fmt.Errorf("stream %q fragment order regressed from %d to %d", s.ID, last, f.Order)
			}
			last = f.Order
			if f.Order <= minResolved {
				all = append(all, f)
			}
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].Order != all[j].Order {
			return all[i].Order < all[j].Order
		}
		return all[i].Stream < all[j].Stream
	})
	out := make([]Group[T], 0)
	for _, f := range all {
		if len(out) == 0 || out[len(out)-1].Order != f.Order {
			out = append(out, Group[T]{Order: f.Order})
		}
		out[len(out)-1].Fragments = append(out[len(out)-1].Fragments, f)
	}
	return out, nil
}
