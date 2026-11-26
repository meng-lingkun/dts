package mysqlbinlog

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"dts/backend/internal/domain"
)

// The source dictionary returns the quoted labels in declaration order.
// A binlog row stores either a one-based ENUM index or a SET bitmap.
func parseMemberLabels(definition, kind string) ([]string, error) {
	name, rest, ok := strings.Cut(definition, "(")
	if !ok || !strings.EqualFold(name, kind) || !utf8.ValidString(rest) {
		return nil, errors.New("missing or invalid character member definition")
	}
	var labels []string
	for strings.HasPrefix(rest, "'") {
		rest = rest[1:]
		var label strings.Builder
		for {
			pos := strings.IndexAny(rest, "'\\")
			if pos < 0 || pos+1 >= len(rest) {
				return nil, errors.New("unterminated member definition")
			}
			label.WriteString(rest[:pos])
			next := rest[pos+1]
			if rest[pos] == '\'' {
				if next != '\'' {
					rest = rest[pos+1:]
					break
				}
				label.WriteByte('\'')
			} else {
				switch next {
				case '\\':
					label.WriteByte('\\')
				case '0':
					label.WriteByte(0)
				case 'n':
					label.WriteByte('\n')
				case 'r':
					label.WriteByte('\r')
				default:
					return nil, errors.New("invalid member escape")
				}
			}
			rest = rest[pos+2:]
		}
		labels = append(labels, label.String())
		if rest == ")" {
			return labels, nil
		}
		if !strings.HasPrefix(rest, ",") {
			break
		}
		rest = rest[1:]
	}
	return nil, errors.New("invalid member list")
}

type memberCache map[string][]string

func takeMemberBytes(raw []byte) (value, rest []byte, err error) {
	size, n, err := readLenEnc(raw)
	if err != nil || size > uint64(len(raw)-n) {
		return nil, nil, errors.New("truncated member metadata")
	}
	return raw[n : n+int(size)], raw[n+int(size):], nil
}

// Verify the live dictionary against the labels recorded with this row event.
// A later ALTER TABLE may have reassigned ordinals since the binlog was written.
func eventMemberLabels(tm *TableMap, types []uint16, columns []domain.ColumnInfo) (memberCache, error) {
	if len(columns) != len(tm.ColumnTypes) {
		return nil, errors.New("live column count does not match binlog table map")
	}
	const setField, enumField = 5, 6
	blocks := map[byte][]byte{}
	for raw := tm.optionalMetadata; len(raw) > 0; {
		tag := raw[0]
		value, rest, err := takeMemberBytes(raw[1:])
		if err != nil {
			return nil, err
		}
		raw = rest
		if tag == setField || tag == enumField {
			if _, exists := blocks[tag]; exists {
				return nil, errors.New("duplicate table-map member metadata")
			}
			blocks[tag] = value
		}
	}
	members := make(memberCache)
	for i, tp := range tm.ColumnTypes {
		if tp != TypeString {
			continue
		}
		real, _ := realStringType(types[i])
		if real != TypeEnum && real != TypeSet {
			continue
		}
		kind, tag := "enum", byte(enumField)
		if real == TypeSet {
			kind, tag = "set", setField
		}
		col := columns[i]
		raw := blocks[tag]
		if len(raw) == 0 {
			return nil, fmt.Errorf("%s column %s has no labels in its source TABLE_MAP; binlog_row_metadata=FULL must be enabled before recording these changes", kind, col.Name)
		}
		count, n, err := readLenEnc(raw)
		if err != nil || count == 0 || count > 65535 || count > uint64(len(raw)-n) {
			return nil, errors.New("invalid table-map member count")
		}
		raw = raw[n:]
		labels := make([]string, 0, count)
		for j := uint64(0); j < count; j++ {
			label, rest, err := takeMemberBytes(raw)
			if err != nil {
				return nil, err
			}
			labels = append(labels, string(label))
			raw = rest
		}
		blocks[tag] = raw
		live, err := parseMemberLabels(col.ColumnType, kind)
		if err != nil || !slices.Equal(live, labels) {
			return nil, fmt.Errorf("recorded %s labels for %s differ from the UTF-8 source column definition", kind, col.Name)
		}
		members[col.Name] = labels
	}
	if len(blocks[setField])+len(blocks[enumField]) != 0 {
		return nil, errors.New("unexpected trailing table-map member definitions")
	}
	return members, nil
}

func (members memberCache) value(kind, column string, value uint64) (string, error) {
	labels, ok := members[column]
	if !ok {
		return "", errors.New("missing recorded member labels")
	}
	if kind == "enum" {
		if value == 0 {
			return "", errors.New("ENUM error member at index zero cannot be replayed as a label")
		}
		if value > uint64(len(labels)) {
			return "", fmt.Errorf("ENUM index %d exceeds source member count %d", value, len(labels))
		}
		return labels[value-1], nil
	}
	if len(labels) > 64 || value>>uint(len(labels)) != 0 {
		return "", errors.New("SET bitmap does not match source member count")
	}
	var joined strings.Builder
	for i, label := range labels {
		if value&(uint64(1)<<uint(i)) == 0 {
			continue
		}
		if joined.Len() > 0 {
			joined.WriteByte(',')
		}
		joined.WriteString(label)
	}
	return joined.String(), nil
}
