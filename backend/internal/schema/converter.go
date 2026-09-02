package schema

import (
	"fmt"
	"qmigration/backend/internal/domain"
	"strings"
)

// ConvertColumns converts source catalog column metadata into conservative target
// catalog metadata through the Universal Schema Model. The result intentionally
// avoids copying vendor-specific defaults/extra attributes across database
// families unless their semantics are known to be portable.
func ConvertColumns(columns []domain.ColumnInfo, target domain.DataSourceType) ([]domain.ColumnInfo, []string) {
	out := make([]domain.ColumnInfo, 0, len(columns))
	warnings := []string{}
	for _, source := range columns {
		u := NormalizeType(source)
		c := source
		c.DataType = targetType(u, target)
		c.ColumnType = c.DataType
		// Generated expressions and AUTO_INCREMENT/IDENTITY are schema-object
		// semantics, not scalar type semantics. Do not guess them during
		// heterogeneous table creation.
		if strings.TrimSpace(c.Extra) != "" {
			warnings = append(warnings, fmt.Sprintf("column %s vendor-specific attribute %q was not copied", c.Name, c.Extra))
		}
		c.Extra = ""
		if u.Type == domain.UniversalUnknown {
			warnings = append(warnings, fmt.Sprintf("column %s source type %q mapped to a conservative text type", c.Name, u.SourceType))
		}
		out = append(out, c)
	}
	return out, warnings
}

func targetType(c domain.UniversalColumn, target domain.DataSourceType) string {
	if target.IsPostgreSQLFamily() || target == domain.DataSourceOpenGauss || target == domain.DataSourceKingbase {
		switch c.Type {
		case domain.UniversalString:
			if c.Length > 0 && c.Length <= 10485760 {
				return fmt.Sprintf("varchar(%d)", c.Length)
			}
			return "text"
		case domain.UniversalInteger:
			return "integer"
		case domain.UniversalBigInt:
			return "bigint"
		case domain.UniversalDecimal:
			if c.Precision > 0 && c.Precision <= 1000 {
				if c.Scale > 0 {
					return fmt.Sprintf("numeric(%d,%d)", c.Precision, c.Scale)
				}
				return fmt.Sprintf("numeric(%d)", c.Precision)
			}
			return "numeric"
		case domain.UniversalFloat:
			return "real"
		case domain.UniversalDouble:
			return "double precision"
		case domain.UniversalDate:
			return "date"
		case domain.UniversalTime:
			return "time"
		case domain.UniversalTimestamp:
			return "timestamp"
		case domain.UniversalBoolean:
			return "boolean"
		case domain.UniversalBinary:
			return "bytea"
		case domain.UniversalJSON:
			return "jsonb"
		case domain.UniversalUUID:
			return "uuid"
		case domain.UniversalText, domain.UniversalUnknown:
			return "text"
		default:
			return "text"
		}
	}
	if target == domain.DataSourceOracle {
		switch c.Type {
		case domain.UniversalString:
			if c.Length > 0 && c.Length <= 4000 {
				return fmt.Sprintf("varchar2(%d)", c.Length)
			}
			return "clob"
		case domain.UniversalInteger:
			return "number(10)"
		case domain.UniversalBigInt:
			return "number(19)"
		case domain.UniversalDecimal:
			p, scale := c.Precision, c.Scale
			if p <= 0 || p > 38 {
				p = 38
			}
			if scale < 0 {
				scale = 0
			}
			if scale > p {
				scale = p
			}
			return fmt.Sprintf("number(%d,%d)", p, scale)
		case domain.UniversalFloat:
			return "binary_float"
		case domain.UniversalDouble:
			return "binary_double"
		case domain.UniversalDate:
			return "date"
		case domain.UniversalTime, domain.UniversalTimestamp:
			return "timestamp(6)"
		case domain.UniversalBoolean:
			return "number(1)"
		case domain.UniversalBinary:
			return "blob"
		case domain.UniversalJSON, domain.UniversalText, domain.UniversalUnknown:
			return "clob"
		case domain.UniversalUUID:
			return "varchar2(36)"
		default:
			return "clob"
		}
	}
	if target == domain.DataSourceSQLServer {
		switch c.Type {
		case domain.UniversalString:
			if c.Length > 0 && c.Length <= 4000 {
				return fmt.Sprintf("nvarchar(%d)", c.Length)
			}
			return "nvarchar(max)"
		case domain.UniversalInteger:
			return "int"
		case domain.UniversalBigInt:
			return "bigint"
		case domain.UniversalDecimal:
			p, scale := c.Precision, c.Scale
			if p <= 0 || p > 38 {
				p = 38
			}
			if scale < 0 {
				scale = 0
			}
			if scale > p {
				scale = p
			}
			return fmt.Sprintf("decimal(%d,%d)", p, scale)
		case domain.UniversalFloat:
			return "real"
		case domain.UniversalDouble:
			return "float(53)"
		case domain.UniversalDate:
			return "date"
		case domain.UniversalTime:
			return "time(7)"
		case domain.UniversalTimestamp:
			return "datetime2(7)"
		case domain.UniversalBoolean:
			return "bit"
		case domain.UniversalBinary:
			return "varbinary(max)"
		case domain.UniversalJSON, domain.UniversalText, domain.UniversalUnknown:
			return "nvarchar(max)"
		case domain.UniversalUUID:
			return "uniqueidentifier"
		default:
			return "nvarchar(max)"
		}
	}
	if target == domain.DataSourceGBase8s {
		switch c.Type {
		case domain.UniversalString:
			if c.Length > 0 && c.Length <= 32739 {
				return fmt.Sprintf("varchar(%d)", c.Length)
			}
			return "lvarchar(32739)"
		case domain.UniversalInteger:
			return "integer"
		case domain.UniversalBigInt:
			return "bigint"
		case domain.UniversalDecimal:
			p, scale := c.Precision, c.Scale
			if p <= 0 || p > 32 {
				p = 32
			}
			if scale < 0 {
				scale = 0
			}
			if scale > p {
				scale = p
			}
			return fmt.Sprintf("decimal(%d,%d)", p, scale)
		case domain.UniversalFloat:
			return "smallfloat"
		case domain.UniversalDouble:
			return "float"
		case domain.UniversalDate:
			return "date"
		case domain.UniversalTime:
			return "datetime hour to fraction(5)"
		case domain.UniversalTimestamp:
			return "datetime year to fraction(5)"
		case domain.UniversalBoolean:
			return "boolean"
		case domain.UniversalBinary:
			return "blob"
		case domain.UniversalUUID:
			return "varchar(36)"
		case domain.UniversalJSON, domain.UniversalText, domain.UniversalUnknown:
			return "clob"
		default:
			return "clob"
		}
	}
	if target == domain.DataSourceGBase {
		switch c.Type {
		case domain.UniversalString:
			if c.Length > 0 && c.Length <= 8191 {
				return fmt.Sprintf("varchar(%d)", c.Length)
			}
			return "longtext"
		case domain.UniversalInteger:
			return "int"
		case domain.UniversalBigInt:
			return "bigint"
		case domain.UniversalDecimal:
			p, scale := c.Precision, c.Scale
			if p <= 0 || p > 65 {
				p = 65
			}
			if scale < 0 {
				scale = 0
			}
			if scale > 30 {
				scale = 30
			}
			if scale > p {
				scale = p
			}
			return fmt.Sprintf("decimal(%d,%d)", p, scale)
		case domain.UniversalFloat:
			return "float"
		case domain.UniversalDouble:
			return "double"
		case domain.UniversalDate:
			return "date"
		case domain.UniversalTime:
			return "time"
		case domain.UniversalTimestamp:
			return "datetime"
		case domain.UniversalBoolean:
			return "tinyint"
		case domain.UniversalBinary:
			return "longblob"
		case domain.UniversalUUID:
			return "varchar(36)"
		case domain.UniversalJSON, domain.UniversalText, domain.UniversalUnknown:
			return "longtext"
		default:
			return "longtext"
		}
	}
	if target.IsMySQLFamily() || target == domain.DataSourceOceanBase {
		switch c.Type {
		case domain.UniversalString:
			if c.Length > 0 && c.Length <= 16383 {
				return fmt.Sprintf("varchar(%d)", c.Length)
			}
			return "longtext"
		case domain.UniversalInteger:
			return "int"
		case domain.UniversalBigInt:
			return "bigint"
		case domain.UniversalDecimal:
			p, s := c.Precision, c.Scale
			if p <= 0 || p > 65 {
				p = 65
			}
			if s < 0 {
				s = 0
			}
			if s > 30 {
				s = 30
			}
			if s > p {
				s = p
			}
			return fmt.Sprintf("decimal(%d,%d)", p, s)
		case domain.UniversalFloat:
			return "float"
		case domain.UniversalDouble:
			return "double"
		case domain.UniversalDate:
			return "date"
		case domain.UniversalTime:
			return "time"
		case domain.UniversalTimestamp:
			return "datetime(6)"
		case domain.UniversalBoolean:
			return "tinyint(1)"
		case domain.UniversalBinary:
			return "longblob"
		case domain.UniversalJSON:
			return "json"
		case domain.UniversalUUID:
			return "char(36)"
		case domain.UniversalText, domain.UniversalUnknown:
			return "longtext"
		default:
			return "longtext"
		}
	}
	// Native renderers for additional database families are enabled as their connector SPI matures. Unknown targets remain conservative.
	return c.SourceType
}
