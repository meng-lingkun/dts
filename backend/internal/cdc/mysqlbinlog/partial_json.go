package mysqlbinlog

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type JSONDiffOperation byte

const (
	JSONDiffReplace JSONDiffOperation = iota
	JSONDiffInsert
	JSONDiffRemove
)

type JSONDiff struct {
	Operation JSONDiffOperation `json:"operation"`
	Path      string            `json:"path"`
	Value     string            `json:"value,omitempty"`
}

// parseJSONDiffAt decodes one Json_diff and returns the number of bytes consumed.
// MySQL serializes a Json_diff_vector as a 4-byte little-endian payload length
// followed by one or more Json_diff values. Each individual diff is encoded as:
// operation, length-encoded path, path, and (except REMOVE) length-encoded
// binary-JSON value.
func parseJSONDiffAt(data []byte) (*JSONDiff, int, error) {
	if len(data) < 2 {
		return nil, 0, errors.New("MySQL JSON diff payload too short")
	}
	op := JSONDiffOperation(data[0])
	if op != JSONDiffReplace && op != JSONDiffInsert && op != JSONDiffRemove {
		return nil, 0, fmt.Errorf("unsupported MySQL JSON diff operation %d", op)
	}
	pos := 1
	pathLen, n, err := readLenEnc(data[pos:])
	if err != nil {
		return nil, 0, fmt.Errorf("decode JSON diff path length: %w", err)
	}
	pos += n
	if pathLen > uint64(len(data)-pos) {
		return nil, 0, errors.New("MySQL JSON diff path exceeds payload")
	}
	path := string(data[pos : pos+int(pathLen)])
	pos += int(pathLen)
	out := &JSONDiff{Operation: op, Path: path}
	if op == JSONDiffRemove {
		return out, pos, nil
	}
	valueLen, n, err := readLenEnc(data[pos:])
	if err != nil {
		return nil, 0, fmt.Errorf("decode JSON diff value length: %w", err)
	}
	pos += n
	if valueLen > uint64(len(data)-pos) {
		return nil, 0, errors.New("MySQL JSON diff value exceeds payload")
	}
	text, err := DecodeBinaryJSON(data[pos : pos+int(valueLen)])
	if err != nil {
		return nil, 0, fmt.Errorf("decode JSON diff value: %w", err)
	}
	pos += int(valueLen)
	out.Value = text
	return out, pos, nil
}

// ParseJSONDiff decodes exactly one diff. It is kept for focused tests and
// diagnostics; binlog partial JSON fields should use ParseJSONDiffVector.
func ParseJSONDiff(data []byte) (*JSONDiff, error) {
	diff, n, err := parseJSONDiffAt(data)
	if err != nil {
		return nil, err
	}
	if n != len(data) {
		return nil, errors.New("unexpected trailing bytes after JSON diff")
	}
	return diff, nil
}

// ParseJSONDiffVector decodes the exact format emitted by
// Json_diff_vector::write_binary: uint32 little-endian byte length followed by
// all serialized diffs. Multiple JSON_SET/JSON_REPLACE operations on one
// column can therefore be reconstructed in their original order.
func ParseJSONDiffVector(data []byte) ([]JSONDiff, error) {
	if len(data) < 4 {
		return nil, errors.New("MySQL JSON diff vector payload too short")
	}
	length := int(binary.LittleEndian.Uint32(data[:4]))
	if length < 0 || length != len(data)-4 {
		return nil, fmt.Errorf("invalid MySQL JSON diff vector length %d, payload=%d", length, len(data)-4)
	}
	body := data[4:]
	out := make([]JSONDiff, 0, 1)
	for len(body) > 0 {
		diff, n, err := parseJSONDiffAt(body)
		if err != nil {
			return nil, fmt.Errorf("decode JSON diff #%d: %w", len(out)+1, err)
		}
		if n <= 0 || n > len(body) {
			return nil, errors.New("invalid MySQL JSON diff byte count")
		}
		out = append(out, *diff)
		body = body[n:]
	}
	if len(out) == 0 {
		return nil, errors.New("empty MySQL JSON diff vector")
	}
	return out, nil
}

type jsonPathToken struct {
	key   *string
	index *int
}

func parseJSONPath(path string) ([]jsonPathToken, error) {
	if path == "" || path[0] != '$' {
		return nil, fmt.Errorf("unsupported MySQL JSON path %q", path)
	}
	if path == "$" {
		return nil, nil
	}
	tokens := []jsonPathToken{}
	for i := 1; i < len(path); {
		switch path[i] {
		case '.':
			i++
			if i >= len(path) {
				return nil, fmt.Errorf("invalid JSON member path %q", path)
			}
			var key string
			if path[i] == '"' {
				start := i
				i++
				escaped := false
				for i < len(path) {
					if path[i] == '"' && !escaped {
						i++
						break
					}
					if path[i] == '\\' && !escaped {
						escaped = true
					} else {
						escaped = false
					}
					i++
				}
				if i > len(path) || path[i-1] != '"' {
					return nil, fmt.Errorf("unterminated quoted JSON member in %q", path)
				}
				quoted := path[start:i]
				if err := json.Unmarshal([]byte(quoted), &key); err != nil {
					return nil, fmt.Errorf("decode quoted JSON member %s: %w", quoted, err)
				}
			} else {
				start := i
				for i < len(path) && path[i] != '.' && path[i] != '[' {
					i++
				}
				if start == i {
					return nil, fmt.Errorf("empty JSON member in %q", path)
				}
				key = path[start:i]
				if strings.ContainsAny(key, "* ") {
					return nil, fmt.Errorf("unsupported JSON path member %q", key)
				}
			}
			tokens = append(tokens, jsonPathToken{key: &key})
		case '[':
			i++
			if i >= len(path) {
				return nil, fmt.Errorf("unterminated JSON array path %q", path)
			}
			if path[i] == '"' {
				start := i
				i++
				escaped := false
				for i < len(path) {
					if path[i] == '"' && !escaped {
						i++
						break
					}
					if path[i] == '\\' && !escaped {
						escaped = true
					} else {
						escaped = false
					}
					i++
				}
				if i >= len(path) || path[i] != ']' {
					return nil, fmt.Errorf("invalid quoted JSON bracket path %q", path)
				}
				quoted := path[start:i]
				var key string
				if err := json.Unmarshal([]byte(quoted), &key); err != nil {
					return nil, err
				}
				i++
				tokens = append(tokens, jsonPathToken{key: &key})
				continue
			}
			start := i
			for i < len(path) && path[i] >= '0' && path[i] <= '9' {
				i++
			}
			if start == i || i >= len(path) || path[i] != ']' {
				return nil, fmt.Errorf("unsupported JSON array path %q", path)
			}
			idx, err := strconv.Atoi(path[start:i])
			if err != nil {
				return nil, err
			}
			i++
			tokens = append(tokens, jsonPathToken{index: &idx})
		default:
			return nil, fmt.Errorf("unsupported JSON path syntax %q", path)
		}
	}
	return tokens, nil
}

func decodeStandardJSON(text string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	return v, nil
}

func applyJSONDiffValue(node any, tokens []jsonPathToken, op JSONDiffOperation, value any) (any, error) {
	if len(tokens) == 0 {
		switch op {
		case JSONDiffReplace:
			return value, nil
		case JSONDiffRemove:
			return nil, errors.New("removing the JSON document root is not supported")
		case JSONDiffInsert:
			return nil, errors.New("inserting at the JSON document root is not supported")
		}
	}
	t := tokens[0]
	if t.key != nil {
		obj, ok := node.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("JSON path expects object member %q", *t.key)
		}
		if len(tokens) == 1 {
			_, exists := obj[*t.key]
			switch op {
			case JSONDiffReplace:
				if !exists {
					return nil, fmt.Errorf("JSON REPLACE path %q does not exist", *t.key)
				}
				obj[*t.key] = value
			case JSONDiffInsert:
				if exists {
					return nil, fmt.Errorf("JSON INSERT path %q already exists", *t.key)
				}
				obj[*t.key] = value
			case JSONDiffRemove:
				if !exists {
					return nil, fmt.Errorf("JSON REMOVE path %q does not exist", *t.key)
				}
				delete(obj, *t.key)
			}
			return obj, nil
		}
		child, exists := obj[*t.key]
		if !exists {
			return nil, fmt.Errorf("JSON path member %q does not exist", *t.key)
		}
		updated, err := applyJSONDiffValue(child, tokens[1:], op, value)
		if err != nil {
			return nil, err
		}
		obj[*t.key] = updated
		return obj, nil
	}
	if t.index != nil {
		arr, ok := node.([]any)
		if !ok {
			return nil, fmt.Errorf("JSON path expects array index %d", *t.index)
		}
		idx := *t.index
		if len(tokens) == 1 {
			switch op {
			case JSONDiffReplace:
				if idx < 0 || idx >= len(arr) {
					return nil, fmt.Errorf("JSON REPLACE array index %d out of range", idx)
				}
				arr[idx] = value
			case JSONDiffInsert:
				if idx < 0 || idx > len(arr) {
					return nil, fmt.Errorf("JSON INSERT array index %d out of range", idx)
				}
				arr = append(arr, nil)
				copy(arr[idx+1:], arr[idx:])
				arr[idx] = value
			case JSONDiffRemove:
				if idx < 0 || idx >= len(arr) {
					return nil, fmt.Errorf("JSON REMOVE array index %d out of range", idx)
				}
				arr = append(arr[:idx], arr[idx+1:]...)
			}
			return arr, nil
		}
		if idx < 0 || idx >= len(arr) {
			return nil, fmt.Errorf("JSON path array index %d out of range", idx)
		}
		updated, err := applyJSONDiffValue(arr[idx], tokens[1:], op, value)
		if err != nil {
			return nil, err
		}
		arr[idx] = updated
		return arr, nil
	}
	return nil, errors.New("invalid JSON path token")
}

func ApplyJSONDiff(before string, diff *JSONDiff) (string, error) {
	if diff == nil {
		return "", errors.New("nil MySQL JSON diff")
	}
	doc, err := decodeStandardJSON(before)
	if err != nil {
		return "", fmt.Errorf("decode before JSON: %w", err)
	}
	tokens, err := parseJSONPath(diff.Path)
	if err != nil {
		return "", err
	}
	var value any
	if diff.Operation != JSONDiffRemove {
		value, err = decodeStandardJSON(diff.Value)
		if err != nil {
			return "", fmt.Errorf("decode JSON diff value: %w", err)
		}
	}
	doc, err = applyJSONDiffValue(doc, tokens, diff.Operation, value)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		return "", err
	}
	return strings.TrimSpace(buf.String()), nil
}

// ApplyJSONDiffVector applies all diffs in sequence, matching MySQL replica
// semantics. If any diff is rejected, the caller must fail the source
// transaction and must not advance the CDC checkpoint.
func ApplyJSONDiffVector(before string, diffs []JSONDiff) (string, error) {
	current := before
	for i := range diffs {
		next, err := ApplyJSONDiff(current, &diffs[i])
		if err != nil {
			return "", fmt.Errorf("apply JSON diff #%d (%s): %w", i+1, diffs[i].Path, err)
		}
		current = next
	}
	return current, nil
}
