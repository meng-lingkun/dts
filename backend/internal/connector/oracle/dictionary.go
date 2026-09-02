package oracleconnector

import (
	"fmt"
	"strings"
)

// Oracle Data Dictionary plans are kept in the native connector so metadata
// semantics do not depend on JDBC/DataX/SeaTunnel. They become executable once
// the TTC authenticator/query executor is enabled against a real Oracle server.
const oracleListSchemasSQL = `SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME`
const oracleListTablesSQL = `SELECT OWNER, TABLE_NAME, NVL(NUM_ROWS,0), NVL(BLOCKS,0) FROM ALL_TABLES WHERE OWNER = :1 ORDER BY TABLE_NAME`
const oracleColumnsSQL = `SELECT OWNER,TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE,COLUMN_ID FROM ALL_TAB_COLUMNS WHERE OWNER=:1 AND TABLE_NAME=:2 ORDER BY COLUMN_ID`
const oraclePrimaryKeysSQL = `SELECT acc.COLUMN_NAME, acc.POSITION FROM ALL_CONSTRAINTS ac JOIN ALL_CONS_COLUMNS acc ON ac.OWNER=acc.OWNER AND ac.CONSTRAINT_NAME=acc.CONSTRAINT_NAME WHERE ac.CONSTRAINT_TYPE='P' AND ac.OWNER=:1 AND ac.TABLE_NAME=:2 ORDER BY acc.POSITION`
const oracleIndexesSQL = `SELECT ai.INDEX_NAME, ai.UNIQUENESS, aic.COLUMN_NAME, aic.COLUMN_POSITION FROM ALL_INDEXES ai JOIN ALL_IND_COLUMNS aic ON ai.OWNER=aic.INDEX_OWNER AND ai.INDEX_NAME=aic.INDEX_NAME WHERE ai.TABLE_OWNER=:1 AND ai.TABLE_NAME=:2 ORDER BY ai.INDEX_NAME,aic.COLUMN_POSITION`

func normalizeOracleType(dataType string, precision, scale *int64) string {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	switch t {
	case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG":
		return "STRING"
	case "RAW", "LONG RAW", "BLOB":
		return "BINARY"
	case "DATE":
		return "TIMESTAMP"
	case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
		return "TIMESTAMP"
	case "BINARY_FLOAT":
		return "FLOAT"
	case "BINARY_DOUBLE", "FLOAT":
		return "DOUBLE"
	case "JSON":
		return "JSON"
	case "BOOLEAN":
		return "BOOLEAN"
	case "NUMBER", "DECIMAL", "NUMERIC":
		if scale != nil && *scale == 0 && precision != nil {
			if *precision <= 9 {
				return "INTEGER"
			}
			if *precision <= 18 {
				return "BIGINT"
			}
		}
		return "DECIMAL"
	default:
		return "UNKNOWN"
	}
}

func quoteOracleIdentifier(v string) (string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", fmt.Errorf("empty Oracle identifier")
	}
	if strings.ContainsRune(v, 0) {
		return "", fmt.Errorf("invalid Oracle identifier")
	}
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`, nil
}
