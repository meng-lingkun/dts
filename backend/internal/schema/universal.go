package schema

import (
	"qmigration/backend/internal/domain"
	"regexp"
	"strconv"
	"strings"
)

var typeArgs = regexp.MustCompile(`\((\d+)(?:\s*,\s*(\d+))?\)`)

func NormalizeType(col domain.ColumnInfo) domain.UniversalColumn {
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	ct := strings.ToLower(strings.TrimSpace(col.ColumnType))
	u := domain.UniversalColumn{Name: col.Name, Type: domain.UniversalUnknown, SourceType: col.ColumnType, Nullable: col.Nullable, PrimaryKey: col.PrimaryKey}
	if u.SourceType == "" {
		u.SourceType = col.DataType
	}
	if m := typeArgs.FindStringSubmatch(ct); len(m) > 1 {
		u.Length, _ = strconv.ParseInt(m[1], 10, 64)
		u.Precision = int(u.Length)
		if len(m) > 2 && m[2] != "" {
			u.Scale, _ = strconv.Atoi(m[2])
		}
	}
	switch dt {
	case "char", "character", "varchar", "character varying", "nvarchar", "nvarchar2", "nchar", "bpchar", "citext":
		u.Type = domain.UniversalString
	case "tinyint", "smallint", "mediumint", "int", "integer", "int2", "int4", "serial", "smallserial":
		u.Type = domain.UniversalInteger
	case "bigint", "int8", "bigserial", "number":
		if strings.HasPrefix(ct, "number(") && u.Scale > 0 {
			u.Type = domain.UniversalDecimal
		} else {
			u.Type = domain.UniversalBigInt
		}
	case "decimal", "numeric", "dec":
		u.Type = domain.UniversalDecimal
	case "float", "real", "float4", "binary_float":
		u.Type = domain.UniversalFloat
	case "double", "double precision", "float8", "binary_double":
		u.Type = domain.UniversalDouble
	case "date":
		u.Type = domain.UniversalDate
	case "time", "time without time zone", "time with time zone":
		u.Type = domain.UniversalTime
	case "datetime", "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz":
		u.Type = domain.UniversalTimestamp
	case "bool", "boolean", "bit":
		u.Type = domain.UniversalBoolean
	case "binary", "varbinary", "bytea", "blob", "tinyblob", "mediumblob", "longblob", "raw", "long raw":
		u.Type = domain.UniversalBinary
	case "text", "tinytext", "mediumtext", "longtext", "clob", "nclob", "xml", "xmltype":
		u.Type = domain.UniversalText
	case "json", "jsonb":
		u.Type = domain.UniversalJSON
	case "uuid", "uniqueidentifier":
		u.Type = domain.UniversalUUID
	default:
		switch {
		case strings.Contains(dt, "char") || strings.Contains(dt, "string"):
			u.Type = domain.UniversalString
		case strings.Contains(dt, "int"):
			u.Type = domain.UniversalBigInt
		case strings.Contains(dt, "decimal") || strings.Contains(dt, "numeric") || strings.Contains(dt, "number"):
			u.Type = domain.UniversalDecimal
		case strings.Contains(dt, "timestamp") || strings.Contains(dt, "datetime"):
			u.Type = domain.UniversalTimestamp
		case strings.Contains(dt, "binary") || strings.Contains(dt, "blob"):
			u.Type = domain.UniversalBinary
		case strings.Contains(dt, "text") || strings.Contains(dt, "clob"):
			u.Type = domain.UniversalText
		}
	}
	return u
}

func FromMetadata(database string, meta *domain.TableMetadata) domain.UniversalTable {
	if meta == nil {
		return domain.UniversalTable{}
	}
	out := domain.UniversalTable{Database: database, Schema: meta.Schema, Name: meta.Name, Indexes: append([]domain.IndexInfo(nil), meta.Indexes...), Constraints: append([]domain.ForeignKeyInfo(nil), meta.ForeignKeys...)}
	out.Columns = make([]domain.UniversalColumn, 0, len(meta.Columns))
	for _, c := range meta.Columns {
		out.Columns = append(out.Columns, NormalizeType(c))
	}
	return out
}
