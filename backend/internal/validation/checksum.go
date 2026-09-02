package validation

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/big"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"strings"
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
	sum  *big.Int
	mod  *big.Int
}

func NewUnorderedAccumulator() *UnorderedAccumulator {
	return &UnorderedAccumulator{
		sum: new(big.Int),
		mod: new(big.Int).Lsh(big.NewInt(1), 256),
	}
}

func (a *UnorderedAccumulator) AddRequest(ctx context.Context, c connector.DataConnector, template connector.ReadBatchRequest, readCols, canonicalCols []domain.ColumnInfo, batchRows int) error {
	if a == nil {
		return errors.New("nil unordered checksum accumulator")
	}
	if batchRows <= 0 {
		batchRows = 1000
	}
	if len(canonicalCols) != len(readCols) {
		return fmt.Errorf("canonical column count %d != read column count %d", len(canonicalCols), len(readCols))
	}
	req := template
	req.Columns = readCols
	req.Limit = batchRows
	req.Cursor = append([]connector.Value(nil), template.Cursor...)
	for {
		b, err := c.ReadBatch(ctx, req)
		if err != nil {
			return err
		}
		if len(b.Rows) == 0 {
			break
		}
		for _, row := range b.Rows {
			h := sha256.New()
			for i, v := range row {
				if v.Null {
					_, _ = h.Write([]byte{0xff})
					continue
				}
				_, _ = h.Write([]byte{0x00})
				raw := canonicalValue(canonicalCols[i], v.Raw)
				var n [8]byte
				binary.LittleEndian.PutUint64(n[:], uint64(len(raw)))
				_, _ = h.Write(n[:])
				_, _ = h.Write(raw)
			}
			_, _ = h.Write([]byte{0xfe})
			digest := new(big.Int).SetBytes(h.Sum(nil))
			a.sum.Add(a.sum, digest)
			a.sum.Mod(a.sum, a.mod)
			a.rows++
		}
		if req.UseKeyset {
			if len(b.LastKey) == 0 {
				return errors.New("keyset connector returned rows without last-key cursor")
			}
			req.Cursor = append([]connector.Value(nil), b.LastKey...)
		} else {
			req.AfterPK = b.LastPK
			req.HasAfter = true
		}
		if len(b.Rows) < batchRows {
			break
		}
	}
	return nil
}

func (a *UnorderedAccumulator) Checksum() Checksum {
	if a == nil || a.sum == nil {
		return Checksum{Hash: fmt.Sprintf("%064x", 0)}
	}
	return Checksum{Rows: a.rows, Hash: fmt.Sprintf("%064x", a.sum)}
}

// Range computes a deterministic checksum using the read column metadata as
// the canonical representation. RangeCanonical can be used for heterogeneous
// migrations where target column types/names differ from the source.
func Range(ctx context.Context, c connector.DataConnector, schema, table, pk string, cols []domain.ColumnInfo, start, end int64, batchRows int) (Checksum, error) {
	return RangeCanonical(ctx, c, schema, table, pk, cols, cols, start, end, batchRows)
}

func RangeCanonical(ctx context.Context, c connector.DataConnector, schema, table, pk string, readCols, canonicalCols []domain.ColumnInfo, start, end int64, batchRows int) (Checksum, error) {
	if batchRows <= 0 {
		batchRows = 1000
	}
	if len(canonicalCols) != len(readCols) {
		return Checksum{}, fmt.Errorf("canonical column count %d != read column count %d", len(canonicalCols), len(readCols))
	}
	h := fnv.New64a()
	var rows int64
	var after int64
	hasAfter := false
	for {
		b, err := c.ReadBatch(ctx, connector.ReadBatchRequest{Schema: schema, Table: table, PrimaryKey: pk, Columns: readCols, StartPK: start, EndPK: end, AfterPK: after, HasAfter: hasAfter, Limit: batchRows})
		if err != nil {
			return Checksum{}, err
		}
		if len(b.Rows) == 0 {
			break
		}
		for _, row := range b.Rows {
			for i, v := range row {
				if v.Null {
					_, _ = h.Write([]byte{0xff})
					continue
				}
				_, _ = h.Write([]byte{0x00})
				raw := canonicalValue(canonicalCols[i], v.Raw)
				var n [8]byte
				binary.LittleEndian.PutUint64(n[:], uint64(len(raw)))
				_, _ = h.Write(n[:])
				_, _ = h.Write(raw)
			}
			_, _ = h.Write([]byte{0xfe})
			rows++
		}
		after = b.LastPK
		hasAfter = true
		if len(b.Rows) < batchRows {
			break
		}
	}
	return Checksum{Rows: rows, Hash: fmt.Sprintf("%016x", h.Sum64())}, nil
}

func canonicalValue(col domain.ColumnInfo, raw []byte) []byte {
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	ct := strings.ToLower(strings.TrimSpace(col.ColumnType))
	s := strings.TrimSpace(string(raw))

	if dt == "boolean" || dt == "bool" || ct == "tinyint(1)" || ct == "bit(1)" {
		switch strings.ToLower(s) {
		case "1", "t", "true", "yes", "y":
			return []byte("1")
		case "0", "f", "false", "no", "n":
			return []byte("0")
		}
	}
	if isNumericType(dt) {
		if r, ok := new(big.Rat).SetString(s); ok {
			return []byte(r.RatString())
		}
	}
	if dt == "json" || dt == "jsonb" {
		var v any
		if json.Unmarshal(raw, &v) == nil {
			if b, err := json.Marshal(v); err == nil {
				return b
			}
		}
	}
	if dt == "uuid" {
		return []byte(strings.ToLower(s))
	}
	// Binary values are already decoded by the connector and must not be
	// trimmed/converted as text.
	if isBinaryType(dt) {
		return raw
	}
	return raw
}

func isNumericType(dt string) bool {
	switch dt {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "int2", "int4", "int8", "decimal", "numeric", "float", "double", "double precision", "real", "float4", "float8":
		return true
	default:
		return false
	}
}
func isBinaryType(dt string) bool {
	switch dt {
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bytea":
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
	if batchRows <= 0 {
		batchRows = 1000
	}
	if len(primaryKeys) == 0 {
		return Checksum{}, fmt.Errorf("keyset validation requires primary keys")
	}
	if len(canonicalCols) != len(readCols) {
		return Checksum{}, fmt.Errorf("canonical column count %d != read column count %d", len(canonicalCols), len(readCols))
	}
	h := fnv.New64a()
	var rows int64
	var cursor []connector.Value
	for {
		b, err := c.ReadBatch(ctx, connector.ReadBatchRequest{Schema: schema, Table: table, PrimaryKey: primaryKeys[0], PrimaryKeys: primaryKeys, Columns: readCols, Cursor: cursor, LowerBound: lowerBound, UpperBound: upperBound, UseKeyset: true, Limit: batchRows})
		if err != nil {
			return Checksum{}, err
		}
		if len(b.Rows) == 0 {
			break
		}
		for _, row := range b.Rows {
			for i, v := range row {
				if v.Null {
					_, _ = h.Write([]byte{0xff})
					continue
				}
				_, _ = h.Write([]byte{0x00})
				raw := canonicalValue(canonicalCols[i], v.Raw)
				var n [8]byte
				binary.LittleEndian.PutUint64(n[:], uint64(len(raw)))
				_, _ = h.Write(n[:])
				_, _ = h.Write(raw)
			}
			_, _ = h.Write([]byte{0xfe})
			rows++
		}
		if len(b.LastKey) == 0 {
			return Checksum{}, errors.New("keyset connector returned rows without last-key cursor")
		}
		cursor = b.LastKey
		if len(b.Rows) < batchRows {
			break
		}
	}
	return Checksum{Rows: rows, Hash: fmt.Sprintf("%016x", h.Sum64())}, nil
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
