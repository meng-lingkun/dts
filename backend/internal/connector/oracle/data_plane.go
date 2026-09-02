package oracleconnector

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func oracleString(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

func oracleIdent(v string) string {
	return `"` + strings.ReplaceAll(strings.TrimSpace(v), `"`, `""`) + `"`
}

func oracleQualified(schema, table string) string {
	if strings.TrimSpace(schema) == "" {
		return oracleIdent(table)
	}
	return oracleIdent(schema) + "." + oracleIdent(table)
}

func oracleAnyRaw(v any) (connector.Value, error) {
	switch x := v.(type) {
	case nil:
		return connector.Value{Null: true}, nil
	case string:
		return connector.Value{Raw: []byte(x)}, nil
	case []byte:
		return connector.Value{Raw: append([]byte(nil), x...)}, nil
	case fmt.Stringer:
		return connector.Value{Raw: []byte(x.String())}, nil
	default:
		return connector.Value{Raw: []byte(fmt.Sprint(x))}, nil
	}
}

func oracleRowValues(row []any) ([]connector.Value, error) {
	out := make([]connector.Value, len(row))
	for i := range row {
		v, err := oracleAnyRaw(row[i])
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func oracleCellString(row []any, i int) string {
	if i < 0 || i >= len(row) || row[i] == nil {
		return ""
	}
	switch x := row[i].(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}

func oracleCellInt64(row []any, i int) int64 {
	n, _ := strconv.ParseInt(strings.TrimSpace(oracleCellString(row, i)), 10, 64)
	return n
}

func (c *Connector) ListSchemas(ctx context.Context) ([]domain.SchemaInfo, error) {
	r, err := c.querySQL(ctx, `SELECT USERNAME FROM ALL_USERS WHERE ORACLE_MAINTAINED='N' ORDER BY USERNAME`, 256, 8192)
	if err != nil {
		// ORACLE_MAINTAINED is unavailable on old releases; fall back safely.
		r, err = c.querySQL(ctx, oracleListSchemasSQL, 256, 8192)
	}
	if err != nil {
		return nil, err
	}
	out := make([]domain.SchemaInfo, 0, len(r.Rows))
	for _, row := range r.Rows {
		if name := oracleCellString(row, 0); name != "" {
			out = append(out, domain.SchemaInfo{Name: name})
		}
	}
	return out, nil
}

func (c *Connector) ListTables(ctx context.Context, schema string) ([]domain.TableInfo, error) {
	owner := strings.ToUpper(strings.TrimSpace(schema))
	q := `SELECT OWNER,TABLE_NAME,NVL(NUM_ROWS,0),NVL(BLOCKS,0)*8192 FROM ALL_TABLES WHERE OWNER=` + oracleString(owner) + ` ORDER BY TABLE_NAME`
	r, err := c.querySQL(ctx, q, 256, 65536)
	if err != nil {
		return nil, err
	}
	out := make([]domain.TableInfo, 0, len(r.Rows))
	for _, row := range r.Rows {
		if len(row) < 4 {
			continue
		}
		out = append(out, domain.TableInfo{Schema: oracleCellString(row, 0), Name: oracleCellString(row, 1), Rows: oracleCellInt64(row, 2), DataLength: oracleCellInt64(row, 3)})
	}
	return out, nil
}

func oracleColumnTypeName(dataType string, dataLen, charLen, precision, scale string) string {
	dt := strings.ToUpper(strings.TrimSpace(dataType))
	switch dt {
	case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW":
		n := strings.TrimSpace(charLen)
		if n == "" || n == "0" {
			n = strings.TrimSpace(dataLen)
		}
		if n != "" && n != "0" {
			return strings.ToLower(dt) + "(" + n + ")"
		}
	case "NUMBER", "DECIMAL", "NUMERIC":
		p := strings.TrimSpace(precision)
		s := strings.TrimSpace(scale)
		if p != "" {
			if s != "" {
				return "number(" + p + "," + s + ")"
			}
			return "number(" + p + ")"
		}
	case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
		if s := strings.TrimSpace(scale); s != "" && s != "0" {
			return strings.ToLower(dt) + "(" + s + ")"
		}
	}
	return strings.ToLower(dt)
}

func oracleNumericPK(col domain.ColumnInfo) bool {
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	if dt != "number" && dt != "decimal" && dt != "numeric" && dt != "integer" && dt != "int" && dt != "bigint" {
		return false
	}
	ct := strings.ToLower(col.ColumnType)
	if strings.Contains(ct, ",") {
		parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(ct, "number("), ")"), ",")
		if len(parts) == 2 && strings.TrimSpace(parts[1]) != "0" {
			return false
		}
	}
	return true
}

func (c *Connector) GetTableMetadata(ctx context.Context, schema, table string) (*domain.TableMetadata, error) {
	owner := strings.ToUpper(strings.TrimSpace(schema))
	name := strings.ToUpper(strings.TrimSpace(table))
	m := &domain.TableMetadata{Schema: schema, Name: table}
	statQ := `SELECT NVL(NUM_ROWS,0),NVL(BLOCKS,0)*8192 FROM ALL_TABLES WHERE OWNER=` + oracleString(owner) + ` AND TABLE_NAME=` + oracleString(name)
	if r, err := c.querySQL(ctx, statQ, 8, 8); err == nil && len(r.Rows) > 0 {
		m.EstimatedRows = oracleCellInt64(r.Rows[0], 0)
		m.DataLength = oracleCellInt64(r.Rows[0], 1)
	}
	colQ := `SELECT COLUMN_NAME,DATA_TYPE,DATA_LENGTH,CHAR_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE,COLUMN_ID FROM ALL_TAB_COLUMNS WHERE OWNER=` + oracleString(owner) + ` AND TABLE_NAME=` + oracleString(name) + ` ORDER BY COLUMN_ID`
	rows, err := c.querySQL(ctx, colQ, 256, 8192)
	if err != nil {
		return nil, err
	}
	for _, row := range rows.Rows {
		if len(row) < 8 {
			continue
		}
		dt := strings.ToLower(oracleCellString(row, 1))
		col := domain.ColumnInfo{
			Name: oracleCellString(row, 0), DataType: dt,
			ColumnType: oracleColumnTypeName(oracleCellString(row, 1), oracleCellString(row, 2), oracleCellString(row, 3), oracleCellString(row, 4), oracleCellString(row, 5)),
			Nullable:   strings.EqualFold(oracleCellString(row, 6), "Y"), Ordinal: int(oracleCellInt64(row, 7)),
		}
		m.Columns = append(m.Columns, col)
	}
	if len(m.Columns) == 0 {
		return m, nil
	}
	pkQ := `SELECT ACC.COLUMN_NAME,ACC.POSITION FROM ALL_CONSTRAINTS AC JOIN ALL_CONS_COLUMNS ACC ON AC.OWNER=ACC.OWNER AND AC.CONSTRAINT_NAME=ACC.CONSTRAINT_NAME WHERE AC.CONSTRAINT_TYPE='P' AND AC.OWNER=` + oracleString(owner) + ` AND AC.TABLE_NAME=` + oracleString(name) + ` ORDER BY ACC.POSITION`
	if pks, e := c.querySQL(ctx, pkQ, 64, 512); e == nil {
		for _, row := range pks.Rows {
			if v := oracleCellString(row, 0); v != "" {
				m.PrimaryKeys = append(m.PrimaryKeys, v)
			}
		}
	}
	pkSet := map[string]bool{}
	for _, k := range m.PrimaryKeys {
		pkSet[strings.ToUpper(k)] = true
	}
	for i := range m.Columns {
		m.Columns[i].PrimaryKey = pkSet[strings.ToUpper(m.Columns[i].Name)]
	}
	idxQ := `SELECT AI.INDEX_NAME,AI.UNIQUENESS,CASE WHEN AC.CONSTRAINT_TYPE='P' THEN 'Y' ELSE 'N' END,AIC.COLUMN_NAME,AIC.COLUMN_POSITION FROM ALL_INDEXES AI JOIN ALL_IND_COLUMNS AIC ON AI.OWNER=AIC.INDEX_OWNER AND AI.INDEX_NAME=AIC.INDEX_NAME LEFT JOIN ALL_CONSTRAINTS AC ON AC.OWNER=AI.OWNER AND AC.INDEX_NAME=AI.INDEX_NAME AND AC.TABLE_NAME=AI.TABLE_NAME WHERE AI.TABLE_OWNER=` + oracleString(owner) + ` AND AI.TABLE_NAME=` + oracleString(name) + ` ORDER BY AI.INDEX_NAME,AIC.COLUMN_POSITION`
	if rr, e := c.querySQL(ctx, idxQ, 256, 16384); e == nil {
		byName := map[string]*domain.IndexInfo{}
		order := []string{}
		for _, row := range rr.Rows {
			if len(row) < 5 {
				continue
			}
			n := oracleCellString(row, 0)
			idx := byName[n]
			if idx == nil {
				idx = &domain.IndexInfo{Name: n, Unique: strings.EqualFold(oracleCellString(row, 1), "UNIQUE"), Primary: strings.EqualFold(oracleCellString(row, 2), "Y")}
				byName[n] = idx
				order = append(order, n)
			}
			idx.Columns = append(idx.Columns, oracleCellString(row, 3))
		}
		for _, n := range order {
			m.Indexes = append(m.Indexes, *byName[n])
		}
	}
	fkQ := `SELECT AC.CONSTRAINT_NAME,ACC.COLUMN_NAME,RAC.OWNER,RAC.TABLE_NAME,RACC.COLUMN_NAME,ACC.POSITION FROM ALL_CONSTRAINTS AC JOIN ALL_CONS_COLUMNS ACC ON AC.OWNER=ACC.OWNER AND AC.CONSTRAINT_NAME=ACC.CONSTRAINT_NAME JOIN ALL_CONSTRAINTS RAC ON RAC.OWNER=AC.R_OWNER AND RAC.CONSTRAINT_NAME=AC.R_CONSTRAINT_NAME JOIN ALL_CONS_COLUMNS RACC ON RACC.OWNER=RAC.OWNER AND RACC.CONSTRAINT_NAME=RAC.CONSTRAINT_NAME AND RACC.POSITION=ACC.POSITION WHERE AC.CONSTRAINT_TYPE='R' AND AC.OWNER=` + oracleString(owner) + ` AND AC.TABLE_NAME=` + oracleString(name) + ` ORDER BY AC.CONSTRAINT_NAME,ACC.POSITION`
	if rr, e := c.querySQL(ctx, fkQ, 128, 8192); e == nil {
		byName := map[string]*domain.ForeignKeyInfo{}
		order := []string{}
		for _, row := range rr.Rows {
			if len(row) < 5 {
				continue
			}
			n := oracleCellString(row, 0)
			fk := byName[n]
			if fk == nil {
				fk = &domain.ForeignKeyInfo{Name: n, ReferencedSchema: oracleCellString(row, 2), ReferencedTable: oracleCellString(row, 3)}
				byName[n] = fk
				order = append(order, n)
			}
			fk.Columns = append(fk.Columns, oracleCellString(row, 1))
			fk.ReferencedColumns = append(fk.ReferencedColumns, oracleCellString(row, 4))
		}
		for _, n := range order {
			m.ForeignKeys = append(m.ForeignKeys, *byName[n])
		}
	}
	if len(m.PrimaryKeys) == 1 {
		for _, col := range m.Columns {
			if strings.EqualFold(col.Name, m.PrimaryKeys[0]) {
				m.PrimaryKey = col.Name
				m.PrimaryKeyType = col.ColumnType
				m.PrimaryKeyNumeric = oracleNumericPK(col)
				break
			}
		}
		if m.PrimaryKeyNumeric {
			q := `SELECT MIN(` + oracleIdent(m.PrimaryKey) + `),MAX(` + oracleIdent(m.PrimaryKey) + `) FROM ` + oracleQualified(schema, table)
			if rr, e := c.querySQL(ctx, q, 4, 4); e == nil && len(rr.Rows) > 0 && len(rr.Rows[0]) >= 2 && rr.Rows[0][0] != nil && rr.Rows[0][1] != nil {
				m.HasRows = true
				m.MinPK, _ = strconv.ParseInt(strings.TrimSpace(oracleCellString(rr.Rows[0], 0)), 10, 64)
				m.MaxPK, _ = strconv.ParseInt(strings.TrimSpace(oracleCellString(rr.Rows[0], 1)), 10, 64)
			}
		}
	}
	if !m.HasRows && m.EstimatedRows > 0 {
		m.HasRows = true
	}
	return m, nil
}

func oracleValueLiteral(v connector.Value, col domain.ColumnInfo) (string, error) {
	if v.Null {
		return "NULL", nil
	}
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	raw := string(v.Raw)
	switch {
	case dt == "number" || dt == "numeric" || dt == "decimal" || dt == "integer" || dt == "int" || dt == "bigint" || dt == "smallint" || dt == "float" || dt == "binary_float" || dt == "binary_double" || dt == "double":
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return "", errors.New("empty Oracle numeric literal")
		}
		if !oracleNumberLiteralSafe.MatchString(raw) {
			return "", fmt.Errorf("invalid Oracle numeric literal %q", raw)
		}
		return raw, nil
	case strings.Contains(dt, "blob") || dt == "raw" || dt == "long raw" || strings.Contains(dt, "binary") || strings.Contains(dt, "bytea"):
		return "HEXTORAW('" + hex.EncodeToString(v.Raw) + "')", nil
	case dt == "date":
		return "TO_DATE(" + oracleString(raw) + ",'YYYY-MM-DD HH24:MI:SS')", nil
	case strings.Contains(dt, "timestamp") || strings.Contains(dt, "datetime"):
		return "TO_TIMESTAMP(" + oracleString(strings.TrimSuffix(raw, "Z")) + ",'YYYY-MM-DD HH24:MI:SS.FF')", nil
	case dt == "boolean" || dt == "bool":
		if raw == "1" || strings.EqualFold(raw, "true") || strings.EqualFold(raw, "t") {
			return "1", nil
		}
		if raw == "0" || strings.EqualFold(raw, "false") || strings.EqualFold(raw, "f") {
			return "0", nil
		}
		return "", fmt.Errorf("invalid Oracle boolean literal %q", raw)
	default:
		return oracleString(raw), nil
	}
}

func oracleKeyColumns(keys []string, columns []domain.ColumnInfo) ([]domain.ColumnInfo, error) {
	byName := map[string]domain.ColumnInfo{}
	for _, col := range columns {
		byName[strings.ToUpper(col.Name)] = col
	}
	out := make([]domain.ColumnInfo, len(keys))
	for i, key := range keys {
		col, ok := byName[strings.ToUpper(key)]
		if !ok {
			return nil, fmt.Errorf("migration key column %s is not present in selected columns", key)
		}
		out[i] = col
	}
	return out, nil
}

func oracleLexCompare(keys []string, cols []domain.ColumnInfo, vals []connector.Value, op string) (string, error) {
	if len(keys) == 0 || len(keys) != len(cols) || len(keys) != len(vals) {
		return "", errors.New("Oracle lexicographic comparison key/value shape mismatch")
	}
	strict := op
	inclusive := false
	switch op {
	case ">=":
		strict, inclusive = ">", true
	case "<=":
		strict, inclusive = "<", true
	case ">", "<":
	default:
		return "", fmt.Errorf("unsupported Oracle lexicographic operator %q", op)
	}
	parts := make([]string, 0, len(keys)+1)
	for i := range keys {
		and := make([]string, 0, i+1)
		for j := 0; j < i; j++ {
			lit, err := oracleValueLiteral(vals[j], cols[j])
			if err != nil {
				return "", fmt.Errorf("Oracle key %s: %w", keys[j], err)
			}
			and = append(and, oracleIdent(keys[j])+"="+lit)
		}
		lit, err := oracleValueLiteral(vals[i], cols[i])
		if err != nil {
			return "", fmt.Errorf("Oracle key %s: %w", keys[i], err)
		}
		and = append(and, oracleIdent(keys[i])+strict+lit)
		parts = append(parts, "("+strings.Join(and, " AND ")+")")
	}
	if inclusive {
		eq := make([]string, len(keys))
		for i := range keys {
			lit, err := oracleValueLiteral(vals[i], cols[i])
			if err != nil {
				return "", fmt.Errorf("Oracle key %s: %w", keys[i], err)
			}
			eq[i] = oracleIdent(keys[i]) + "=" + lit
		}
		parts = append(parts, "("+strings.Join(eq, " AND ")+")")
	}
	return "(" + strings.Join(parts, " OR ") + ")", nil
}

func (c *Connector) PlanKeysetBoundaries(ctx context.Context, req connector.KeysetBoundaryRequest) ([][]connector.Value, error) {
	if req.Partitions <= 1 {
		return nil, nil
	}
	if len(req.Keys) == 0 {
		return nil, errors.New("keyset boundary planning requires migration key columns")
	}
	keyCols, err := oracleKeyColumns(req.Keys, req.Columns)
	if err != nil {
		return nil, err
	}
	conditions := []string{}
	if len(req.LowerBound) > 0 {
		pred, err := oracleLexCompare(req.Keys, keyCols, req.LowerBound, ">=")
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, pred)
	}
	if len(req.UpperBound) > 0 {
		pred, err := oracleLexCompare(req.Keys, keyCols, req.UpperBound, "<")
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, pred)
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	names := make([]string, len(req.Keys))
	for i, k := range req.Keys {
		names[i] = oracleIdent(k)
	}
	keyList := strings.Join(names, ",")
	q := `WITH QM_RANKED AS (SELECT ` + keyList + `,NTILE(` + strconv.Itoa(req.Partitions) + `) OVER (ORDER BY ` + keyList + `) QM_BUCKET FROM ` + oracleQualified(req.Schema, req.Table) + where + `), QM_BOUNDS AS (SELECT ` + keyList + `,QM_BUCKET,ROW_NUMBER() OVER (PARTITION BY QM_BUCKET ORDER BY ` + keyList + `) QM_RN FROM QM_RANKED) SELECT ` + keyList + ` FROM QM_BOUNDS WHERE QM_BUCKET>1 AND QM_RN=1 ORDER BY QM_BUCKET`
	rr, err := c.querySQL(ctx, q, 256, req.Partitions+16)
	if err != nil {
		return nil, err
	}
	out := make([][]connector.Value, 0, len(rr.Rows))
	for _, row := range rr.Rows {
		vals, err := oracleRowValues(row)
		if err != nil {
			return nil, err
		}
		if len(vals) != len(req.Keys) {
			return nil, fmt.Errorf("Oracle boundary returned %d columns for %d keys", len(vals), len(req.Keys))
		}
		for i := range vals {
			if vals[i].Null {
				return nil, fmt.Errorf("Oracle migration key %s returned NULL boundary", req.Keys[i])
			}
		}
		out = append(out, vals)
	}
	return out, nil
}

func (c *Connector) ListTablePartitions(ctx context.Context, schema, table string) ([]string, error) {
	q := `SELECT PARTITION_NAME FROM ALL_TAB_PARTITIONS WHERE TABLE_OWNER=` + oracleString(strings.ToUpper(schema)) + ` AND TABLE_NAME=` + oracleString(strings.ToUpper(table)) + ` ORDER BY PARTITION_POSITION`
	rr, err := c.querySQL(ctx, q, 256, 65536)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rr.Rows))
	for _, row := range rr.Rows {
		if p := oracleCellString(row, 0); p != "" {
			out = append(out, p)
		}
	}
	return out, nil
}

// oracleValidationFrom renders a table reference for normal or exact-SCN
// validation reads. Oracle's table_reference grammar places the flashback query
// clause after query_table_expression, so a partition extension (when present)
// precedes AS OF SCN. The SCN has already been parsed as an unsigned integer and
// is therefore safe to render as a numeric literal.
func (c *Connector) oracleValidationFrom(schema, table, partition string) string {
	from := oracleQualified(schema, table)
	if p := strings.TrimSpace(partition); p != "" {
		from += " PARTITION (" + oracleIdent(p) + ")"
	}
	if c.validationSCN != "" {
		from += " AS OF SCN " + c.validationSCN
	}
	return from
}

func (c *Connector) ReadBatch(ctx context.Context, req connector.ReadBatchRequest) (*connector.RowBatch, error) {
	if req.Limit <= 0 {
		req.Limit = 500
	}
	if len(req.Columns) == 0 {
		return nil, errors.New("no selected columns")
	}
	selected := make([]string, len(req.Columns))
	idx := map[string]int{}
	for i, col := range req.Columns {
		selected[i] = oracleIdent(col.Name)
		idx[strings.ToUpper(col.Name)] = i
	}
	conditions := []string{}
	orderKeys := []string{}
	if req.UseKeyset {
		keys := append([]string(nil), req.PrimaryKeys...)
		if len(keys) == 0 && req.PrimaryKey != "" {
			keys = []string{req.PrimaryKey}
		}
		if len(keys) == 0 {
			return nil, errors.New("keyset read requires migration key columns")
		}
		keyCols, err := oracleKeyColumns(keys, req.Columns)
		if err != nil {
			return nil, err
		}
		if len(req.Cursor) > 0 {
			pred, err := oracleLexCompare(keys, keyCols, req.Cursor, ">")
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, pred)
		} else if len(req.LowerBound) > 0 {
			pred, err := oracleLexCompare(keys, keyCols, req.LowerBound, ">=")
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, pred)
		}
		if len(req.UpperBound) > 0 {
			pred, err := oracleLexCompare(keys, keyCols, req.UpperBound, "<")
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, pred)
		}
		for _, k := range keys {
			orderKeys = append(orderKeys, oracleIdent(k))
		}
	} else {
		pkIndex, ok := idx[strings.ToUpper(req.PrimaryKey)]
		if !ok {
			return nil, errors.New("primary key not selected")
		}
		_ = pkIndex
		conditions = append(conditions, oracleIdent(req.PrimaryKey)+">="+strconv.FormatInt(req.StartPK, 10), oracleIdent(req.PrimaryKey)+"<="+strconv.FormatInt(req.EndPK, 10))
		if req.HasAfter {
			conditions = append(conditions, oracleIdent(req.PrimaryKey)+">"+strconv.FormatInt(req.AfterPK, 10))
		}
		orderKeys = []string{oracleIdent(req.PrimaryKey)}
	}
	if req.HashBuckets > 0 {
		if req.HashBucket < 0 || req.HashBucket >= req.HashBuckets {
			return nil, fmt.Errorf("invalid hash bucket %d/%d", req.HashBucket, req.HashBuckets)
		}
		keys := append([]string(nil), req.PrimaryKeys...)
		if len(keys) == 0 && req.PrimaryKey != "" {
			keys = []string{req.PrimaryKey}
		}
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = `NVL(TO_CHAR(` + oracleIdent(k) + `),'<NULL>')`
		}
		conditions = append(conditions, `ORA_HASH(`+strings.Join(parts, `||'#'||`)+`,`+strconv.Itoa(req.HashBuckets-1)+`)`+`=`+strconv.Itoa(req.HashBucket))
	}
	if strings.TrimSpace(req.CustomWhere) != "" {
		conditions = append(conditions, "("+strings.TrimSpace(req.CustomWhere)+")")
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	from := c.oracleValidationFrom(req.Schema, req.Table, req.Partition)
	// Use an outer ROWNUM filter instead of FETCH FIRST so the native reader
	// remains compatible with Oracle 11g while preserving ORDER BY semantics.
	inner := `SELECT ` + strings.Join(selected, ",") + ` FROM ` + from + where + ` ORDER BY ` + strings.Join(orderKeys, ",")
	q := `SELECT * FROM (` + inner + `) WHERE ROWNUM <= ` + strconv.Itoa(req.Limit)
	rr, err := c.querySQL(ctx, q, minInt(req.Limit, 256), req.Limit)
	if err != nil {
		return nil, err
	}
	batch := &connector.RowBatch{Rows: make([][]connector.Value, 0, len(rr.Rows))}
	for _, row := range rr.Rows {
		vals, err := oracleRowValues(row)
		if err != nil {
			return nil, err
		}
		if len(vals) != len(req.Columns) {
			return nil, fmt.Errorf("Oracle row returned %d columns for %d selected", len(vals), len(req.Columns))
		}
		for _, v := range vals {
			if !v.Null {
				batch.Bytes += int64(len(v.Raw))
			}
		}
		batch.Rows = append(batch.Rows, vals)
		if req.UseKeyset {
			keys := append([]string(nil), req.PrimaryKeys...)
			if len(keys) == 0 {
				keys = []string{req.PrimaryKey}
			}
			batch.LastKey = batch.LastKey[:0]
			for _, k := range keys {
				v := vals[idx[strings.ToUpper(k)]]
				v.Raw = append([]byte(nil), v.Raw...)
				batch.LastKey = append(batch.LastKey, v)
			}
		} else {
			v := vals[idx[strings.ToUpper(req.PrimaryKey)]]
			if !v.Null {
				batch.LastPK, err = strconv.ParseInt(strings.TrimSpace(string(v.Raw)), 10, 64)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return batch, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func requireOracleTargetEnabled() error {
	if !experimentalOracleTargetEnabled() {
		return errors.New("Oracle target operations require QMIGRATION_EXPERIMENTAL_ORACLE_TARGET=1 together with QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE=1")
	}
	return nil
}

func (c *Connector) WriteBatch(ctx context.Context, req connector.WriteBatchRequest) (int64, error) {
	if err := c.rejectValidationSnapshotWrite(); err != nil {
		return 0, err
	}
	if err := requireOracleTargetEnabled(); err != nil {
		return 0, err
	}
	return c.writeBatchNative(ctx, req)
}

func (c *Connector) ReadByKey(ctx context.Context, req connector.ReadByKeyRequest) ([]connector.Value, bool, error) {
	if len(req.PrimaryKeys) == 0 || len(req.PrimaryKeys) != len(req.KeyColumns) || len(req.PrimaryKeys) != len(req.KeyValues) || len(req.Columns) == 0 {
		return nil, false, errors.New("Oracle point lookup key/column metadata is incomplete")
	}
	where := make([]string, len(req.PrimaryKeys))
	for i := range req.PrimaryKeys {
		if req.KeyValues[i].Null {
			return nil, false, fmt.Errorf("point lookup primary key %s cannot be null", req.PrimaryKeys[i])
		}
		lit, err := oracleValueLiteral(req.KeyValues[i], req.KeyColumns[i])
		if err != nil {
			return nil, false, fmt.Errorf("Oracle point lookup key %s: %w", req.PrimaryKeys[i], err)
		}
		where[i] = oracleIdent(req.PrimaryKeys[i]) + `=` + lit
	}
	cols := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		cols[i] = oracleIdent(col.Name)
	}
	q := `SELECT ` + strings.Join(cols, ",") + ` FROM ` + c.oracleValidationFrom(req.Schema, req.Table, "") + ` WHERE ` + strings.Join(where, " AND ") + ` AND ROWNUM <= 1`
	rr, err := c.querySQL(ctx, q, 1, 1)
	if err != nil {
		return nil, false, err
	}
	if len(rr.Rows) == 0 {
		return nil, false, nil
	}
	vals, err := oracleRowValues(rr.Rows[0])
	return vals, true, err
}

func (c *Connector) DeleteByKey(ctx context.Context, req connector.DeleteByKeyRequest) error {
	if err := requireOracleTargetEnabled(); err != nil {
		return err
	}
	keys := append([]string(nil), req.PrimaryKeys...)
	cols := append([]domain.ColumnInfo(nil), req.Columns...)
	vals := append([]connector.Value(nil), req.Values...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys, cols, vals = []string{req.PrimaryKey}, []domain.ColumnInfo{req.Column}, []connector.Value{req.Value}
	}
	c.mu.Lock()
	if err := c.ensureNativeSessionLocked(ctx); err != nil {
		c.mu.Unlock()
		return err
	}
	charset := c.proto.ServerCharset
	outerTx := c.inTransaction
	c.mu.Unlock()
	where, binds, err := oracleBoundKeyPredicate(keys, cols, vals, charset, 1)
	if err != nil {
		return err
	}
	if _, err = c.execBound(ctx, `DELETE FROM `+oracleQualified(req.Schema, req.Table)+` WHERE `+where, [][]oracleTTCBind{binds}, false); err != nil {
		if !outerTx {
			_, _ = c.execSQL(context.Background(), "ROLLBACK")
		}
		return err
	}
	if !outerTx {
		_, err = c.execSQL(ctx, "COMMIT")
	}
	return err
}

func (c *Connector) BeginCDCTransaction(ctx context.Context) error {
	if err := requireOracleTargetEnabled(); err != nil {
		return err
	}
	if _, err := c.execSQL(ctx, `SET TRANSACTION READ WRITE`); err != nil {
		return err
	}
	c.mu.Lock()
	c.inTransaction = true
	c.mu.Unlock()
	return nil
}
func (c *Connector) CommitCDCTransaction(ctx context.Context) error {
	if err := requireOracleTargetEnabled(); err != nil {
		return err
	}
	_, err := c.execSQL(ctx, `COMMIT`)
	c.mu.Lock()
	c.inTransaction = false
	c.mu.Unlock()
	return err
}
func (c *Connector) RollbackCDCTransaction(ctx context.Context) error {
	if err := requireOracleTargetEnabled(); err != nil {
		return err
	}
	_, err := c.execSQL(ctx, `ROLLBACK`)
	c.mu.Lock()
	c.inTransaction = false
	c.mu.Unlock()
	return err
}

func (c *Connector) ExecDDL(ctx context.Context, schema, ddl string) error {
	if err := requireOracleTargetEnabled(); err != nil {
		return err
	}
	if strings.TrimSpace(schema) != "" {
		if _, err := c.execSQL(ctx, `ALTER SESSION SET CURRENT_SCHEMA=`+oracleIdent(schema)); err != nil {
			return err
		}
	}
	_, err := c.execSQL(ctx, ddl)
	return err
}

func (c *Connector) EnsureSchema(ctx context.Context, schema string) error {
	rr, err := c.querySQL(ctx, `SELECT USERNAME FROM ALL_USERS WHERE USERNAME=`+oracleString(strings.ToUpper(strings.TrimSpace(schema))), 2, 2)
	if err != nil {
		return err
	}
	if len(rr.Rows) == 0 {
		return fmt.Errorf("Oracle schema/user %s does not exist; pre-create it before migration", schema)
	}
	return nil
}

var oracleTypeSafe = regexp.MustCompile(`^[A-Za-z0-9_(), .]+$`)
var oracleNumberLiteralSafe = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

func oracleColumnDDL(col domain.ColumnInfo) string {
	ct := strings.TrimSpace(col.ColumnType)
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	if ct != "" && oracleTypeSafe.MatchString(ct) {
		lower := strings.ToLower(ct)
		for _, prefix := range []string{"varchar2", "nvarchar2", "char", "nchar", "number", "decimal", "numeric", "date", "timestamp", "raw", "blob", "clob", "nclob", "binary_float", "binary_double", "float"} {
			if strings.HasPrefix(lower, prefix) {
				return strings.ToUpper(ct)
			}
		}
	}
	switch {
	case dt == "tinyint" || dt == "smallint" || dt == "integer" || dt == "int":
		return "NUMBER(10)"
	case dt == "bigint":
		return "NUMBER(19)"
	case dt == "decimal" || dt == "numeric" || dt == "number":
		return "NUMBER(38,10)"
	case dt == "float" || dt == "real" || dt == "binary_float":
		return "BINARY_FLOAT"
	case dt == "double" || dt == "double precision" || dt == "binary_double":
		return "BINARY_DOUBLE"
	case dt == "date":
		return "DATE"
	case strings.Contains(dt, "timestamp") || strings.Contains(dt, "datetime"):
		return "TIMESTAMP(9)"
	case strings.Contains(dt, "blob") || dt == "bytea" || dt == "binary" || dt == "varbinary" || dt == "raw" || dt == "long raw":
		return "BLOB"
	case strings.Contains(dt, "clob") || strings.Contains(dt, "text") || dt == "json" || dt == "jsonb":
		return "CLOB"
	case dt == "boolean" || dt == "bool":
		return "NUMBER(1)"
	default:
		return "VARCHAR2(4000)"
	}
}

func (c *Connector) CreateTable(ctx context.Context, schema, table string, cols []domain.ColumnInfo, pk string) error {
	pks := []string{}
	if pk != "" {
		pks = []string{pk}
	}
	return c.CreateTableWithPrimaryKeys(ctx, schema, table, cols, pks)
}
func (c *Connector) CreateTableWithPrimaryKeys(ctx context.Context, schema, table string, cols []domain.ColumnInfo, pks []string) error {
	if err := requireOracleTargetEnabled(); err != nil {
		return err
	}
	if err := c.EnsureSchema(ctx, schema); err != nil {
		return err
	}
	defs := make([]string, len(cols))
	for i, col := range cols {
		defs[i] = oracleIdent(col.Name) + " " + oracleColumnDDL(col)
		if !col.Nullable {
			defs[i] += " NOT NULL"
		}
	}
	if len(pks) > 0 {
		ks := make([]string, len(pks))
		for i, k := range pks {
			ks[i] = oracleIdent(k)
		}
		defs = append(defs, `PRIMARY KEY (`+strings.Join(ks, ",")+`)`)
	}
	_, err := c.execSQL(ctx, `CREATE TABLE `+oracleQualified(schema, table)+` (`+strings.Join(defs, ",")+`)`)
	return err
}

func (c *Connector) CreateIndex(ctx context.Context, schema, table string, idx domain.IndexInfo) error {
	if err := requireOracleTargetEnabled(); err != nil {
		return err
	}
	if len(idx.Columns) == 0 {
		return errors.New("index has no columns")
	}
	cols := make([]string, len(idx.Columns))
	for i, v := range idx.Columns {
		cols[i] = oracleIdent(v)
	}
	name := idx.Name
	if name == "" {
		name = "QM_IDX_" + table + "_" + strconv.FormatInt(time.Now().UnixNano()%1000000, 10)
	}
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	_, err := c.execSQL(ctx, `CREATE `+unique+`INDEX `+oracleQualified(schema, name)+` ON `+oracleQualified(schema, table)+` (`+strings.Join(cols, ",")+`)`)
	return err
}

func (c *Connector) CreateForeignKey(ctx context.Context, schema, table string, fk domain.ForeignKeyInfo) error {
	if err := requireOracleTargetEnabled(); err != nil {
		return err
	}
	if len(fk.Columns) == 0 || len(fk.Columns) != len(fk.ReferencedColumns) || fk.ReferencedTable == "" {
		return errors.New("invalid foreign key metadata")
	}
	cols := make([]string, len(fk.Columns))
	refs := make([]string, len(fk.ReferencedColumns))
	for i := range cols {
		cols[i] = oracleIdent(fk.Columns[i])
		refs[i] = oracleIdent(fk.ReferencedColumns[i])
	}
	refSchema := fk.ReferencedSchema
	if refSchema == "" {
		refSchema = schema
	}
	name := fk.Name
	if name == "" {
		name = "QM_FK_" + table + "_" + strconv.FormatInt(time.Now().UnixNano()%1000000, 10)
	}
	_, err := c.execSQL(ctx, `ALTER TABLE `+oracleQualified(schema, table)+` ADD CONSTRAINT `+oracleIdent(name)+` FOREIGN KEY (`+strings.Join(cols, ",")+`) REFERENCES `+oracleQualified(refSchema, fk.ReferencedTable)+` (`+strings.Join(refs, ",")+`)`)
	return err
}

func (c *Connector) SampleRuntimeLoad(ctx context.Context) (domain.DatabaseRuntimeLoad, error) {
	q := `SELECT (SELECT COUNT(*) FROM V$SESSION WHERE TYPE='USER'),(SELECT COUNT(*) FROM V$SESSION WHERE TYPE='USER' AND STATUS='ACTIVE'),(SELECT TO_NUMBER(VALUE) FROM V$PARAMETER WHERE NAME='sessions') FROM DUAL`
	rr, err := c.querySQL(ctx, q, 2, 2)
	if err != nil {
		return domain.DatabaseRuntimeLoad{}, err
	}
	var out domain.DatabaseRuntimeLoad
	if len(rr.Rows) > 0 {
		out.Connections = oracleCellInt64(rr.Rows[0], 0)
		out.RunningQueries = oracleCellInt64(rr.Rows[0], 1)
		out.MaxConnections = oracleCellInt64(rr.Rows[0], 2)
	}
	if out.MaxConnections > 0 {
		out.ConnectionUsagePct = float64(out.Connections) * 100 / float64(out.MaxConnections)
	}
	return out, nil
}

func (c *Connector) MigrationPrechecks(ctx context.Context, needCDC bool) []domain.PrecheckItem {
	out := []domain.PrecheckItem{}
	if err := c.TestConnection(ctx); err != nil {
		return []domain.PrecheckItem{{Name: "oracle_connection", Level: domain.PrecheckFailed, Message: err.Error()}}
	}
	if rr, err := c.querySQL(ctx, `SELECT PARAMETER,VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER IN ('NLS_CHARACTERSET','NLS_NCHAR_CHARACTERSET') ORDER BY PARAMETER`, 16, 16); err == nil {
		vals := []string{}
		for _, r := range rr.Rows {
			vals = append(vals, oracleCellString(r, 0)+"="+oracleCellString(r, 1))
		}
		out = append(out, domain.PrecheckItem{Name: "oracle_charset", Level: domain.PrecheckPass, Message: strings.Join(vals, " ")})
	}
	if !needCDC {
		return out
	}
	if rr, err := c.querySQL(ctx, `SELECT LOG_MODE,SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE`, 2, 2); err != nil {
		out = append(out, domain.PrecheckItem{Name: "oracle_logminer_database", Level: domain.PrecheckFailed, Message: err.Error()})
	} else if len(rr.Rows) > 0 {
		logMode := oracleCellString(rr.Rows[0], 0)
		supp := oracleCellString(rr.Rows[0], 1)
		level := domain.PrecheckPass
		msg := "log_mode=" + logMode + " supplemental_log_data_min=" + supp
		if !strings.EqualFold(logMode, "ARCHIVELOG") || !(strings.EqualFold(supp, "YES") || strings.EqualFold(supp, "IMPLICIT")) {
			level = domain.PrecheckFailed
			msg += "; LogMiner CDC requires ARCHIVELOG and minimal supplemental logging"
		}
		out = append(out, domain.PrecheckItem{Name: "oracle_logminer_database", Level: level, Message: msg})
	}
	if rr, err := c.querySQL(ctx, `SELECT COUNT(*) FROM SESSION_PRIVS WHERE PRIVILEGE IN ('LOGMINING','SELECT ANY TRANSACTION')`, 2, 2); err == nil && len(rr.Rows) > 0 {
		n := oracleCellInt64(rr.Rows[0], 0)
		level := domain.PrecheckPass
		msg := fmt.Sprintf("matching privileges=%d", n)
		if n == 0 {
			level = domain.PrecheckFailed
			msg = "current user requires LOGMINING (or equivalent managed-service LogMiner privilege)"
		}
		out = append(out, domain.PrecheckItem{Name: "oracle_logminer_privilege", Level: level, Message: msg})
	}
	return out
}

func (c *Connector) ListSchemaObjects(ctx context.Context, schema string) ([]domain.SchemaObject, error) {
	owner := strings.ToUpper(strings.TrimSpace(schema))
	q := `SELECT OBJECT_NAME,OBJECT_TYPE FROM ALL_OBJECTS WHERE OWNER=` + oracleString(owner) + ` AND OBJECT_TYPE IN ('VIEW','SEQUENCE','TRIGGER','FUNCTION','PROCEDURE') ORDER BY OBJECT_TYPE,OBJECT_NAME`
	rr, err := c.querySQL(ctx, q, 128, 16384)
	if err != nil {
		return nil, err
	}
	out := make([]domain.SchemaObject, 0, len(rr.Rows))
	for _, row := range rr.Rows {
		name, typ := oracleCellString(row, 0), strings.ToUpper(oracleCellString(row, 1))
		if name == "" {
			continue
		}
		var st domain.SchemaObjectType
		switch typ {
		case "VIEW":
			st = domain.SchemaObjectView
		case "SEQUENCE":
			st = domain.SchemaObjectSequence
		case "TRIGGER":
			st = domain.SchemaObjectTrigger
		case "FUNCTION":
			st = domain.SchemaObjectFunction
		case "PROCEDURE":
			st = domain.SchemaObjectProcedure
		default:
			continue
		}
		obj := domain.SchemaObject{Schema: schema, Name: name, Type: st}
		ddlQ := `SELECT DBMS_METADATA.GET_DDL(` + oracleString(typ) + `,` + oracleString(name) + `,` + oracleString(owner) + `) FROM DUAL`
		if dr, e := c.querySQL(ctx, ddlQ, 1, 1); e == nil && len(dr.Rows) > 0 {
			obj.DDL = oracleCellString(dr.Rows[0], 0)
			obj.Definition = obj.DDL
		}
		out = append(out, obj)
	}
	return out, nil
}

var _ connector.DataConnector = (*Connector)(nil)
var _ connector.KeysetBoundaryConnector = (*Connector)(nil)
var _ connector.PartitionConnector = (*Connector)(nil)
var _ connector.RuntimeLoadConnector = (*Connector)(nil)
var _ connector.CDCApplyConnector = (*Connector)(nil)
var _ connector.PointLookupConnector = (*Connector)(nil)
var _ connector.TransactionalCDCApplyConnector = (*Connector)(nil)
var _ connector.DDLApplyConnector = (*Connector)(nil)
var _ connector.SchemaConnector = (*Connector)(nil)
var _ connector.CompositeSchemaConnector = (*Connector)(nil)
var _ connector.PostLoadSchemaConnector = (*Connector)(nil)
var _ connector.SchemaObjectConnector = (*Connector)(nil)
var _ connector.MigrationPrecheckConnector = (*Connector)(nil)
