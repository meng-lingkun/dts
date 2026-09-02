package mysqlbinlog

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// GTID represents one MySQL transaction identifier (SID:GNO).
type GTID struct {
	SID string `json:"sid"`
	GNO uint64 `json:"gno"`
}

func normalizeSID(v string) (string, [16]byte, error) {
	var out [16]byte
	raw := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(v), "-", ""))
	if len(raw) != 32 {
		return "", out, fmt.Errorf("invalid MySQL GTID SID %q", v)
	}
	b, err := hex.DecodeString(raw)
	if err != nil || len(b) != 16 {
		return "", out, fmt.Errorf("invalid MySQL GTID SID %q", v)
	}
	copy(out[:], b)
	normalized := fmt.Sprintf("%x-%x-%x-%x-%x", out[0:4], out[4:6], out[6:8], out[8:10], out[10:16])
	return normalized, out, nil
}

func ParseGTIDEvent(e *Event) (*GTID, error) {
	if e == nil || (e.Header.Type != GTIDEvent && e.Header.Type != AnonymousGTIDEvent) {
		return nil, errors.New("not a GTID_EVENT")
	}
	if len(e.Payload) < 25 { // flags(1) + SID(16) + GNO(8)
		return nil, fmt.Errorf("GTID_EVENT payload too short: %d", len(e.Payload))
	}
	sidBytes := e.Payload[1:17]
	sid := fmt.Sprintf("%x-%x-%x-%x-%x", sidBytes[0:4], sidBytes[4:6], sidBytes[6:8], sidBytes[8:10], sidBytes[10:16])
	return &GTID{SID: sid, GNO: binary.LittleEndian.Uint64(e.Payload[17:25])}, nil
}

func (g GTID) String() string {
	if g.SID == "" || g.GNO == 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", strings.ToLower(g.SID), g.GNO)
}

type GTIDInterval struct {
	Start uint64 // inclusive
	End   uint64 // inclusive
}

// GTIDSet is a canonical MySQL UUID interval set. It deliberately models MySQL
// GTIDs, not MariaDB domain-server-sequence GTIDs.
type GTIDSet struct {
	sets map[string][]GTIDInterval
}

func NewGTIDSet() *GTIDSet { return &GTIDSet{sets: map[string][]GTIDInterval{}} }

func ParseGTIDSet(raw string) (*GTIDSet, error) {
	s := NewGTIDSet()
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return s, nil
	}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.Split(part, ":")
		if len(fields) < 2 {
			return nil, fmt.Errorf("invalid MySQL GTID set component %q", part)
		}
		sid, _, err := normalizeSID(fields[0])
		if err != nil {
			return nil, err
		}
		for _, token := range fields[1:] {
			token = strings.TrimSpace(token)
			if token == "" {
				continue
			}
			start, end := uint64(0), uint64(0)
			if i := strings.IndexByte(token, '-'); i >= 0 {
				start, err = strconv.ParseUint(token[:i], 10, 64)
				if err == nil {
					end, err = strconv.ParseUint(token[i+1:], 10, 64)
				}
			} else {
				start, err = strconv.ParseUint(token, 10, 64)
				end = start
			}
			if err != nil || start == 0 || end < start {
				return nil, fmt.Errorf("invalid MySQL GTID interval %q", token)
			}
			s.sets[sid] = append(s.sets[sid], GTIDInterval{Start: start, End: end})
		}
	}
	s.normalize()
	return s, nil
}

func (s *GTIDSet) Clone() *GTIDSet {
	out := NewGTIDSet()
	if s == nil {
		return out
	}
	for sid, intervals := range s.sets {
		out.sets[sid] = append([]GTIDInterval(nil), intervals...)
	}
	return out
}

func (s *GTIDSet) Add(gtid string) error {
	parts := strings.Split(strings.TrimSpace(gtid), ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid MySQL GTID %q", gtid)
	}
	sid, _, err := normalizeSID(parts[0])
	if err != nil {
		return err
	}
	gno, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || gno == 0 {
		return fmt.Errorf("invalid MySQL GTID GNO %q", parts[1])
	}
	s.sets[sid] = append(s.sets[sid], GTIDInterval{Start: gno, End: gno})
	s.normalizeSID(sid)
	return nil
}

func (s *GTIDSet) normalize() {
	for sid := range s.sets {
		s.normalizeSID(sid)
	}
}

func (s *GTIDSet) normalizeSID(sid string) {
	intervals := s.sets[sid]
	sort.Slice(intervals, func(i, j int) bool {
		if intervals[i].Start == intervals[j].Start {
			return intervals[i].End < intervals[j].End
		}
		return intervals[i].Start < intervals[j].Start
	})
	merged := make([]GTIDInterval, 0, len(intervals))
	for _, cur := range intervals {
		if len(merged) == 0 || cur.Start > merged[len(merged)-1].End+1 {
			merged = append(merged, cur)
			continue
		}
		if cur.End > merged[len(merged)-1].End {
			merged[len(merged)-1].End = cur.End
		}
	}
	s.sets[sid] = merged
}

func (s *GTIDSet) String() string {
	if s == nil || len(s.sets) == 0 {
		return ""
	}
	sids := make([]string, 0, len(s.sets))
	for sid := range s.sets {
		sids = append(sids, sid)
	}
	sort.Strings(sids)
	parts := make([]string, 0, len(sids))
	for _, sid := range sids {
		b := strings.Builder{}
		b.WriteString(sid)
		for _, in := range s.sets[sid] {
			b.WriteByte(':')
			b.WriteString(strconv.FormatUint(in.Start, 10))
			if in.End != in.Start {
				b.WriteByte('-')
				b.WriteString(strconv.FormatUint(in.End, 10))
			}
		}
		parts = append(parts, b.String())
	}
	return strings.Join(parts, ",")
}

// EncodeSIDBlock returns the binary SID-block expected by COM_BINLOG_DUMP_GTID:
// n_sids, then SID + n_intervals + [start,end-exclusive] intervals.
func (s *GTIDSet) EncodeSIDBlock() ([]byte, error) {
	if s == nil {
		s = NewGTIDSet()
	}
	s.normalize()
	sids := make([]string, 0, len(s.sets))
	for sid := range s.sets {
		sids = append(sids, sid)
	}
	sort.Strings(sids)
	size := 8
	for _, sid := range sids {
		size += 16 + 8 + len(s.sets[sid])*16
	}
	out := make([]byte, size)
	binary.LittleEndian.PutUint64(out[:8], uint64(len(sids)))
	off := 8
	for _, sid := range sids {
		_, sidBytes, err := normalizeSID(sid)
		if err != nil {
			return nil, err
		}
		copy(out[off:off+16], sidBytes[:])
		off += 16
		intervals := s.sets[sid]
		binary.LittleEndian.PutUint64(out[off:off+8], uint64(len(intervals)))
		off += 8
		for _, in := range intervals {
			binary.LittleEndian.PutUint64(out[off:off+8], in.Start)
			binary.LittleEndian.PutUint64(out[off+8:off+16], in.End+1)
			off += 16
		}
	}
	return out, nil
}
