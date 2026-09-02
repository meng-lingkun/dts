package ticdc

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Position is QMigration's durable TiCDC receive cursor. TSO preserves the
// upstream TiDB commit/resolved timestamp. Offset is retained as the
// single-partition compatibility cursor for partition 0, while Offsets carries
// the next offset for every Kafka partition when a TiCDC topic is sharded.
//
// Canonical encodings:
//
//	tso=429918007904436226;kafka=19
//	tso=429918007904436226;kafka=0:19,1:42,2:7
//
// The per-partition form is required for multi-partition restart correctness:
// advancing one partition can never cause unread records from another
// partition to be skipped after Worker failover.
type Position struct {
	TSO     uint64
	Offset  int64
	Offsets map[int32]int64
}

func (p Position) normalizedOffsets() map[int32]int64 {
	if len(p.Offsets) > 0 {
		out := make(map[int32]int64, len(p.Offsets))
		for partition, offset := range p.Offsets {
			out[partition] = offset
		}
		if _, ok := out[0]; !ok && p.Offset >= 0 {
			out[0] = p.Offset
		}
		return out
	}
	return map[int32]int64{0: p.Offset}
}

func (p Position) PartitionOffset(partition int32) int64 {
	if len(p.Offsets) > 0 {
		if v, ok := p.Offsets[partition]; ok {
			return v
		}
	}
	if partition == 0 {
		return p.Offset
	}
	return 0
}

func (p Position) HasDurableKafkaOffset() bool {
	for _, offset := range p.normalizedOffsets() {
		if offset > 0 {
			return true
		}
	}
	return false
}

func (p Position) Partitions() []int32 {
	offsets := p.normalizedOffsets()
	parts := make([]int32, 0, len(offsets))
	for partition := range offsets {
		parts = append(parts, partition)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	return parts
}

func (p Position) String() string {
	offsets := p.normalizedOffsets()
	if len(offsets) == 1 {
		if v, ok := offsets[0]; ok {
			return fmt.Sprintf("tso=%d;kafka=%d", p.TSO, v)
		}
	}
	parts := make([]int32, 0, len(offsets))
	for partition := range offsets {
		parts = append(parts, partition)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	items := make([]string, 0, len(parts))
	for _, partition := range parts {
		items = append(items, fmt.Sprintf("%d:%d", partition, offsets[partition]))
	}
	return fmt.Sprintf("tso=%d;kafka=%s", p.TSO, strings.Join(items, ","))
}

func ParsePosition(raw string) (Position, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Position{}, errors.New("empty TiCDC position")
	}
	// Compatibility for an initial position captured before the Kafka adapter
	// exists: a bare TSO starts consumption at partition 0 offset 0. Multi-
	// partition readers expand missing partition offsets to zero at runtime.
	if !strings.Contains(raw, "=") {
		tso, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return Position{}, fmt.Errorf("invalid TiCDC TSO %q: %w", raw, err)
		}
		return Position{TSO: tso, Offset: 0, Offsets: map[int32]int64{0: 0}}, nil
	}
	var out Position
	seenTSO, seenOffset := false, false
	for _, item := range strings.Split(raw, ";") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			return Position{}, fmt.Errorf("invalid TiCDC position item %q", item)
		}
		switch strings.ToLower(strings.TrimSpace(parts[0])) {
		case "tso":
			v, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				return Position{}, fmt.Errorf("invalid TiCDC TSO: %w", err)
			}
			out.TSO, seenTSO = v, true
		case "kafka":
			rawKafka := strings.TrimSpace(parts[1])
			if rawKafka == "" {
				return Position{}, errors.New("TiCDC Kafka position is empty")
			}
			if !strings.Contains(rawKafka, ":") {
				v, err := strconv.ParseInt(rawKafka, 10, 64)
				if err != nil || v < 0 {
					return Position{}, fmt.Errorf("invalid TiCDC Kafka offset %q", rawKafka)
				}
				out.Offset = v
				out.Offsets = map[int32]int64{0: v}
				seenOffset = true
				continue
			}
			offsets := map[int32]int64{}
			for _, pair := range strings.Split(rawKafka, ",") {
				kv := strings.SplitN(strings.TrimSpace(pair), ":", 2)
				if len(kv) != 2 {
					return Position{}, fmt.Errorf("invalid TiCDC Kafka partition cursor %q", pair)
				}
				partition64, err := strconv.ParseInt(strings.TrimSpace(kv[0]), 10, 32)
				if err != nil || partition64 < 0 {
					return Position{}, fmt.Errorf("invalid TiCDC Kafka partition %q", kv[0])
				}
				offset, err := strconv.ParseInt(strings.TrimSpace(kv[1]), 10, 64)
				if err != nil || offset < 0 {
					return Position{}, fmt.Errorf("invalid TiCDC Kafka offset %q", kv[1])
				}
				partition := int32(partition64)
				if _, exists := offsets[partition]; exists {
					return Position{}, fmt.Errorf("duplicate TiCDC Kafka partition %d", partition)
				}
				offsets[partition] = offset
			}
			if len(offsets) == 0 {
				return Position{}, errors.New("TiCDC Kafka partition cursor set is empty")
			}
			out.Offsets = offsets
			out.Offset = offsets[0]
			seenOffset = true
		default:
			return Position{}, fmt.Errorf("unknown TiCDC position key %q", parts[0])
		}
	}
	if !seenTSO || !seenOffset {
		return Position{}, errors.New("TiCDC position requires tso and kafka components")
	}
	return out, nil
}
