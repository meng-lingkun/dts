package db2log

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// LRI is the normalized durable Db2 Log Record Identifier used by QMigration.
// IBM's native db2ReadLog provider serializes the opaque LRI components as
// unsigned hexadecimal values so the Worker never depends on C struct packing.
type LRI struct {
	Type  uint64 `json:"type"`
	Part1 uint64 `json:"part1"`
	Part2 uint64 `json:"part2"`
}

func (l LRI) IsZero() bool { return l.Type == 0 && l.Part1 == 0 && l.Part2 == 0 }
func (l LRI) String() string {
	return fmt.Sprintf("%x:%016x:%016x", l.Type, l.Part1, l.Part2)
}

func ParseLRI(s string) (LRI, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return LRI{}, errors.New("empty DB2 LRI")
	}
	p := strings.Split(s, ":")
	if len(p) != 3 {
		return LRI{}, fmt.Errorf("invalid DB2 LRI %q", s)
	}
	vals := [3]uint64{}
	for i := range p {
		v, err := strconv.ParseUint(strings.TrimSpace(p[i]), 16, 64)
		if err != nil {
			return LRI{}, fmt.Errorf("invalid DB2 LRI %q: %w", s, err)
		}
		vals[i] = v
	}
	return LRI{Type: vals[0], Part1: vals[1], Part2: vals[2]}, nil
}

func CompareLRI(a, b LRI) int {
	if a.Type < b.Type {
		return -1
	}
	if a.Type > b.Type {
		return 1
	}
	if a.Part1 < b.Part1 {
		return -1
	}
	if a.Part1 > b.Part1 {
		return 1
	}
	if a.Part2 < b.Part2 {
		return -1
	}
	if a.Part2 > b.Part2 {
		return 1
	}
	return 0
}

func NormalizeTID(s string) (string, error) {
	s = strings.ToLower(strings.TrimSpace(strings.ReplaceAll(s, "-", "")))
	if len(s) != 12 {
		return "", fmt.Errorf("DB2 TID must be 6 bytes, got %q", s)
	}
	if _, err := hex.DecodeString(s); err != nil {
		return "", fmt.Errorf("invalid DB2 TID %q: %w", s, err)
	}
	return s, nil
}
