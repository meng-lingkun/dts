package validation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"math/big"
	"strings"
	"unicode/utf8"
)

type Checksum struct {
	Rows int64
	Hash string
}

// UnorderedAccumulator incrementally combines canonical row digests modulo
// 2^256. It allows validation callers to stream a large list of immutable
// source descriptors without retaining every ReadBatchRequest in memory.
// The resulting checksum is byte-for-byte compatible with
// UnorderedCanonicalRequests.
type UnorderedAccumulator struct {
	rows int64
	sum  big.Int
}

func NewUnorderedAccumulator() *UnorderedAccumulator {
	return &UnorderedAccumulator{}
}

func (a *UnorderedAccumulator) AddRequest(ctx context.Context, c connector.DataConnector, template connector.ReadBatchRequest, readCols, canonicalCols []domain.ColumnInfo, batchRows int) error {
	if a == nil {
		return errors.New("nil unordered checksum accumulator")
	}
	var partial, digest big.Int
	h := sha256.New()
	rows, err := scanRows(ctx, c, template, readCols, canonicalCols, batchRows, func(row []connector.Value) {
		h.Reset()
		hashRow(h, canonicalCols, row)
		digest.SetBytes(h.Sum(nil))
		partial.Add(&partial, &digest)
	})
	if err != nil {
		return err
	}
	a.sum.Add(&a.sum, &partial)
	a.sum.Mod(&a.sum, new(big.Int).Lsh(big.NewInt(1), 256))
	a.rows += rows
	return nil
}

func (a *UnorderedAccumulator) Checksum() Checksum {
	if a == nil {
		return Checksum{Hash: fmt.Sprintf("%064x", 0)}
	}
	return Checksum{Rows: a.rows, Hash: fmt.Sprintf("%064x", &a.sum)}
}

// Range computes a deterministic checksum using the read column metadata as
// the canonical representation. RangeCanonical can be used for heterogeneous
// migrations where target column types/names differ from the source.
func Range(ctx context.Context, c connector.DataConnector, schema, table, pk string, cols []domain.ColumnInfo, start, end int64, batchRows int) (Checksum, error) {
	return RangeCanonical(ctx, c, schema, table, pk, cols, cols, start, end, batchRows)
}

func RangeCanonical(ctx context.Context, c connector.DataConnector, schema, table, pk string, readCols, canonicalCols []domain.ColumnInfo, start, end int64, batchRows int) (Checksum, error) {
	return orderedChecksum(ctx, c, connector.ReadBatchRequest{Schema: schema, Table: table, PrimaryKey: pk, StartPK: start, EndPK: end}, readCols, canonicalCols, batchRows)
}

func orderedChecksum(ctx context.Context, c connector.DataConnector, req connector.ReadBatchRequest, readCols, canonicalCols []domain.ColumnInfo, batchRows int) (Checksum, error) {
	h := fnv.New64a()
	rows, err := scanRows(ctx, c, req, readCols, canonicalCols, batchRows, func(row []connector.Value) { hashRow(h, canonicalCols, row) })
	if err != nil {
		return Checksum{}, err
	}
	return Checksum{Rows: rows, Hash: fmt.Sprintf("%016x", h.Sum64())}, nil
}

func hashRow(h hash.Hash, cols []domain.ColumnInfo, row []connector.Value) {
	for i, v := range row {
		if v.Null {
			_, _ = h.Write([]byte{0xff})
			continue
		}
		_, _ = h.Write([]byte{0})
		raw := canonicalValue(cols[i], v.Raw)
		var size [8]byte
		binary.LittleEndian.PutUint64(size[:], uint64(len(raw)))
		_, _ = h.Write(size[:])
		_, _ = h.Write(raw)
	}
	_, _ = h.Write([]byte{0xfe})
}

func scanRows(ctx context.Context, c connector.DataConnector, req connector.ReadBatchRequest, readCols, canonicalCols []domain.ColumnInfo, batchRows int, visit func([]connector.Value)) (int64, error) {
	if batchRows <= 0 {
		batchRows = 1000
	}
	if len(readCols) == 0 || len(canonicalCols) != len(readCols) {
		return 0, fmt.Errorf("invalid checksum projection: %d canonical columns for %d read columns", len(canonicalCols), len(readCols))
	}
	req.Columns, req.Limit = readCols, batchRows
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		b, err := c.ReadBatch(ctx, req)
		if err != nil {
			return 0, err
		}
		if err := req.AdvancePage(b); err != nil {
			return 0, err
		}
		if len(b.Rows) == 0 {
			return total, nil
		}
		for _, row := range b.Rows {
			visit(row)
		}
		total += int64(len(b.Rows))
		// A connector may cap the requested limit. Only an empty page
		// proves that the requested slice has been exhausted.
	}
}

func canonicalValue(col domain.ColumnInfo, raw []byte) []byte {
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	ct := strings.ToLower(strings.TrimSpace(col.ColumnType))
	if dt == "boolean" || dt == "bool" || ct == "tinyint(1)" || ct == "bit(1)" {
		switch strings.ToLower(strings.TrimSpace(string(raw))) {
		case "1", "t", "true", "yes", "y":
			return []byte("1")
		case "0", "f", "false", "no", "n":
			return []byte("0")
		}
	}
	if isNumericType(dt) {
		if r, ok := new(big.Rat).SetString(strings.TrimSpace(string(raw))); ok {
			return []byte(r.RatString())
		}
	}
	if dt == "json" || dt == "jsonb" {
		if utf8.Valid(raw) && json.Valid(raw) {
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.UseNumber()
			v, err := readJSONValue(dec)
			if err == nil {
				if b, err := json.Marshal(v); err == nil {
					return b
				}
			}
		}
	}
	if dt == "uuid" {
		return []byte(strings.ToLower(strings.TrimSpace(string(raw))))
	}
	// Binary values are already decoded by the connector and must not be
	// trimmed/converted as text.
	return raw
}

// The caller has already checked the whole JSON document. Preserve duplicate-key
// objects as raw bytes rather than silently discarding all but the last value.
func readJSONValue(dec *json.Decoder) (any, error) {
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch value := token.(type) {
	case json.Number:
		return exactJSONNumber(string(value)), nil
	case json.Delim:
		if value == '[' {
			items := make([]any, 0)
			for dec.More() {
				item, err := readJSONValue(dec)
				if err != nil {
					return nil, err
				}
				items = append(items, item)
			}
			_, err = dec.Token()
			return items, err
		}
		obj := make(map[string]any)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, err
			}
			name := key.(string)
			if _, found := obj[name]; found {
				return nil, errors.New("duplicate JSON key")
			}
			item, err := readJSONValue(dec)
			if err != nil {
				return nil, err
			}
			obj[name] = item
		}
		_, err = dec.Token()
		return obj, err
	default:
		return value, nil
	}
}

// Reformat the decimal coefficient and adjust the base-10 exponent. The
// exponent stays compressed even for numbers far beyond IEEE-754 range.
func exactJSONNumber(s string) json.Number {
	negative := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	exponentText := ""
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		exponentText = s[i+1:]
		s = s[:i]
	}
	point := strings.IndexByte(s, '.')
	if point < 0 {
		point = len(s)
	}
	all := strings.ReplaceAll(s, ".", "")
	digits := strings.TrimLeft(all, "0")
	if digits == "" {
		return "0"
	}
	shift := int64(point - (len(all) - len(digits)) - 1)
	digits = strings.TrimRight(digits, "0")
	sign := ""
	if negative {
		sign = "-"
	}
	// Use the same fixed/scientific thresholds as encoding/json.
	negExponent := strings.HasPrefix(exponentText, "-")
	magnitude := strings.TrimLeft(strings.TrimLeft(exponentText, "+-"), "0")
	var e string
	if len(magnitude) <= 19 {
		var exponent big.Int
		if magnitude != "" {
			exponent.SetString(magnitude, 10)
		}
		if negExponent {
			exponent.Neg(&exponent)
		}
		exponent.Add(&exponent, big.NewInt(shift))
		p := exponent.Int64()
		if exponent.IsInt64() && p >= -6 && p < 21 {
			cut := int(p) + 1
			switch {
			case cut <= 0:
				return json.Number(sign + "0." + strings.Repeat("0", -cut) + digits)
			case cut >= len(digits):
				return json.Number(sign + digits + strings.Repeat("0", cut-len(digits)))
			default:
				return json.Number(sign + digits[:cut] + "." + digits[cut:])
			}
		}
		e = exponent.String()
	} else {
		// More than 19 digits always exceeds |shift|. Avoid a quadratic
		// decimal-to-big.Int conversion on unbounded JSON exponents.
		if negExponent {
			shift = -shift
		}
		scale := []byte(magnitude)
		for i := len(scale) - 1; i >= 0 && shift != 0; i-- {
			digit := int64(scale[i]-'0') + shift%10
			shift /= 10
			if digit < 0 {
				digit += 10
				shift--
			} else if digit >= 10 {
				digit -= 10
				shift++
			}
			scale[i] = '0' + byte(digit)
		}
		e = strings.TrimLeft(string(scale), "0")
		if shift > 0 {
			e = "1" + string(scale)
		}
		if negExponent {
			e = "-" + e
		}
	}
	if len(digits) > 1 {
		digits = digits[:1] + "." + digits[1:]
	}
	if !strings.HasPrefix(e, "-") {
		e = "+" + e
	}
	return json.Number(sign + digits + "e" + e)
}

func isNumericType(dt string) bool {
	switch dt {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "int2", "int4", "int8", "decimal", "numeric", "float", "double", "double precision", "real", "float4", "float8":
		return true
	default:
		return false
	}
}
// KeysetCanonical computes a deterministic checksum over an entire table using
// primary-key keyset pagination. It is used by generic Native chunks whose keys
// are textual or composite and therefore cannot be represented as int64 ranges.
func KeysetCanonical(ctx context.Context, c connector.DataConnector, schema, table string, primaryKeys []string, readCols, canonicalCols []domain.ColumnInfo, batchRows int) (Checksum, error) {
	return KeysetCanonicalBounded(ctx, c, schema, table, primaryKeys, readCols, canonicalCols, nil, nil, batchRows)
}

// KeysetCanonicalBounded validates one immutable [lower, upper) tuple range.
// Runtime pagination still advances with a durable > cursor inside that range.
func KeysetCanonicalBounded(ctx context.Context, c connector.DataConnector, schema, table string, primaryKeys []string, readCols, canonicalCols []domain.ColumnInfo, lowerBound, upperBound []connector.Value, batchRows int) (Checksum, error) {
	if len(primaryKeys) == 0 {
		return Checksum{}, errors.New("keyset validation requires primary keys")
	}
	return orderedChecksum(ctx, c, connector.ReadBatchRequest{Schema: schema, Table: table, PrimaryKey: primaryKeys[0], PrimaryKeys: primaryKeys, LowerBound: lowerBound, UpperBound: upperBound, UseKeyset: true}, readCols, canonicalCols, batchRows)
}

// UnorderedCanonicalRequests computes a multiplicity-sensitive, order-independent
// checksum over one or more immutable source slices. It is intended for HASH,
// PARTITION and CUSTOM_SQL plans where source ordering/physical placement may
// differ from the target. Each row is canonicalized and SHA-256 hashed; row
// digests are added modulo 2^256 so the final checksum is independent of scan
// order while still preserving duplicate multiplicity.
func UnorderedCanonicalRequests(ctx context.Context, c connector.DataConnector, requests []connector.ReadBatchRequest, readCols, canonicalCols []domain.ColumnInfo, batchRows int) (Checksum, error) {
	acc := NewUnorderedAccumulator()
	for _, template := range requests {
		if err := acc.AddRequest(ctx, c, template, readCols, canonicalCols, batchRows); err != nil {
			return Checksum{}, err
		}
	}
	return acc.Checksum(), nil
}
