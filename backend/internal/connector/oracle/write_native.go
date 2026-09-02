package oracleconnector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

const (
	oracleMaxInlineBindBytes = 24 << 10
	oracleLOBChunkBytes      = 16 << 10
	oracleMaxArrayRows       = 256
)

func oracleColumnFamily(col domain.ColumnInfo) string {
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	ct := strings.ToLower(strings.TrimSpace(col.ColumnType))
	joined := dt + " " + ct
	switch {
	case strings.Contains(joined, "blob") || dt == "raw" || dt == "long raw" || dt == "bytea" || strings.Contains(dt, "binary"):
		if strings.Contains(joined, "blob") || strings.Contains(dt, "long") || dt == "bytea" {
			return "blob"
		}
		return "raw"
	case strings.Contains(joined, "clob") || strings.Contains(dt, "text") || dt == "json" || dt == "jsonb" || dt == "long":
		return "clob"
	case dt == "number" || dt == "numeric" || dt == "decimal" || dt == "integer" || dt == "int" || dt == "bigint" || dt == "smallint":
		return "number"
	case dt == "boolean" || dt == "bool":
		return "boolean"
	case dt == "date":
		return "date"
	case strings.Contains(dt, "timestamp with time zone") || strings.Contains(ct, "timestamp with time zone") || strings.Contains(dt, "timestamptz"):
		return "timestamptz"
	case strings.Contains(dt, "timestamp") || strings.Contains(dt, "datetime"):
		return "timestamp"
	case dt == "float" || dt == "real" || dt == "double" || dt == "double precision" || dt == "binary_float" || dt == "binary_double":
		return "float"
	default:
		return "string"
	}
}

func oracleBindExpr(v connector.Value, col domain.ColumnInfo, position int, charset uint16) (string, oracleTTCBind, error) {
	ph := fmt.Sprintf(":%d", position)
	family := oracleColumnFamily(col)
	switch family {
	case "number":
		base, err := oracleNumberInputBind("0")
		if err != nil {
			return "", oracleTTCBind{}, err
		}
		if v.Null {
			return ph, oracleNullInputBind(base), nil
		}
		b, err := oracleNumberInputBind(strings.TrimSpace(string(v.Raw)))
		return ph, b, err
	case "boolean":
		base, _ := oracleNumberInputBind("0")
		if v.Null {
			return ph, oracleNullInputBind(base), nil
		}
		raw := strings.TrimSpace(string(v.Raw))
		switch {
		case raw == "1" || strings.EqualFold(raw, "true") || strings.EqualFold(raw, "t") || strings.EqualFold(raw, "yes"):
			raw = "1"
		case raw == "0" || strings.EqualFold(raw, "false") || strings.EqualFold(raw, "f") || strings.EqualFold(raw, "no"):
			raw = "0"
		default:
			return "", oracleTTCBind{}, fmt.Errorf("invalid Oracle boolean %q", raw)
		}
		b, err := oracleNumberInputBind(raw)
		return ph, b, err
	case "raw":
		base := oracleRawInputBind(nil)
		if v.Null {
			return ph, oracleNullInputBind(base), nil
		}
		if len(v.Raw) > 32767 {
			return "", oracleTTCBind{}, fmt.Errorf("Oracle RAW bind exceeds 32767 bytes")
		}
		return ph, oracleRawInputBind(v.Raw), nil
	case "date":
		base := oracleStringInputBind(nil, charset)
		if v.Null {
			return `TO_DATE(` + ph + `,'YYYY-MM-DD HH24:MI:SS')`, oracleNullInputBind(base), nil
		}
		return `TO_DATE(` + ph + `,'YYYY-MM-DD HH24:MI:SS')`, oracleStringInputBind(v.Raw, charset), nil
	case "timestamp":
		base := oracleStringInputBind(nil, charset)
		expr := `TO_TIMESTAMP(REPLACE(` + ph + `,'T',' '),'YYYY-MM-DD HH24:MI:SS.FF')`
		if v.Null {
			return expr, oracleNullInputBind(base), nil
		}
		return expr, oracleStringInputBind(v.Raw, charset), nil
	case "timestamptz":
		base := oracleStringInputBind(nil, charset)
		expr := `TO_TIMESTAMP_TZ(REPLACE(` + ph + `,'T',' '),'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM')`
		if v.Null {
			return expr, oracleNullInputBind(base), nil
		}
		return expr, oracleStringInputBind(v.Raw, charset), nil
	case "float":
		base, _ := oracleNumberInputBind("0")
		if v.Null {
			return ph, oracleNullInputBind(base), nil
		}
		raw := strings.TrimSpace(string(v.Raw))
		if !oracleNumberLiteralSafe.MatchString(raw) {
			return "", oracleTTCBind{}, fmt.Errorf("invalid Oracle floating literal %q", raw)
		}
		b, err := oracleNumberInputBind(raw)
		return ph, b, err
	case "blob":
		base := oracleRawInputBind(nil)
		if v.Null {
			return `TO_BLOB(` + ph + `)`, oracleNullInputBind(base), nil
		}
		if len(v.Raw) > oracleMaxInlineBindBytes {
			return "", oracleTTCBind{}, errors.New("large Oracle BLOB requires locator write plan")
		}
		return `TO_BLOB(` + ph + `)`, oracleRawInputBind(v.Raw), nil
	case "clob":
		base := oracleStringInputBind(nil, charset)
		if v.Null {
			return `TO_CLOB(` + ph + `)`, oracleNullInputBind(base), nil
		}
		if len(v.Raw) > oracleMaxInlineBindBytes {
			return "", oracleTTCBind{}, errors.New("large Oracle CLOB requires locator write plan")
		}
		return `TO_CLOB(` + ph + `)`, oracleStringInputBind(v.Raw, charset), nil
	default:
		base := oracleStringInputBind(nil, charset)
		if v.Null {
			return ph, oracleNullInputBind(base), nil
		}
		if len(v.Raw) > 32767 {
			return "", oracleTTCBind{}, fmt.Errorf("Oracle string bind exceeds 32767 bytes; map column to CLOB")
		}
		return ph, oracleStringInputBind(v.Raw, charset), nil
	}
}

type oracleWritePlan struct {
	SQL       string
	Binds     []oracleTTCBind
	LargeLOBs []oracleLargeLOB
	PLSQL     bool
}

type oracleLargeLOB struct {
	Column domain.ColumnInfo
	Value  connector.Value
}

func oracleKeylessLargeLOBPlan(req connector.WriteBatchRequest, row []connector.Value, charset uint16) (oracleWritePlan, error) {
	var out oracleWritePlan
	if len(row) != len(req.Columns) {
		return out, fmt.Errorf("row has %d values for %d columns", len(row), len(req.Columns))
	}
	names := make([]string, len(req.Columns))
	exprs := make([]string, len(req.Columns))
	decls := []string{}
	prep := []string{}
	cleanup := []string{}
	bindPos := 1
	for i, col := range req.Columns {
		names[i] = oracleIdent(col.Name)
		family := oracleColumnFamily(col)
		largeLOB := (family == "blob" || family == "clob") && !row[i].Null && len(row[i].Raw) > oracleMaxInlineBindBytes
		if !largeLOB {
			expr, bind, err := oracleBindExpr(row[i], col, bindPos, charset)
			if err != nil {
				return out, fmt.Errorf("column %s: %w", col.Name, err)
			}
			exprs[i] = expr
			out.Binds = append(out.Binds, bind)
			bindPos++
			continue
		}

		lobVar := fmt.Sprintf("QM_L%d", i+1)
		lobType := "BLOB"
		var chunks [][]byte
		var err error
		if family == "clob" {
			lobType = "CLOB"
			chunks, err = splitUTF8Chunks(row[i].Raw, oracleLOBChunkBytes)
		} else {
			for b := row[i].Raw; len(b) > 0; {
				n := oracleLOBChunkBytes
				if n > len(b) {
					n = len(b)
				}
				chunks = append(chunks, append([]byte(nil), b[:n]...))
				b = b[n:]
			}
		}
		if err != nil {
			return out, fmt.Errorf("column %s: %w", col.Name, err)
		}
		decls = append(decls, lobVar+" "+lobType)
		prep = append(prep, `DBMS_LOB.CREATETEMPORARY(`+lobVar+`,TRUE,DBMS_LOB.CALL);`)
		for _, chunk := range chunks {
			ph := fmt.Sprintf(":%d", bindPos)
			if family == "clob" {
				out.Binds = append(out.Binds, oracleStringInputBind(chunk, charset))
				prep = append(prep, `DBMS_LOB.WRITEAPPEND(`+lobVar+`,LENGTH(`+ph+`),`+ph+`);`)
			} else {
				out.Binds = append(out.Binds, oracleRawInputBind(chunk))
				prep = append(prep, `DBMS_LOB.WRITEAPPEND(`+lobVar+`,UTL_RAW.LENGTH(`+ph+`),`+ph+`);`)
			}
			bindPos++
		}
		exprs[i] = lobVar
		cleanup = append(cleanup, `IF DBMS_LOB.ISTEMPORARY(`+lobVar+`)=1 THEN DBMS_LOB.FREETEMPORARY(`+lobVar+`); END IF;`)
	}
	if len(decls) == 0 {
		return out, errors.New("Oracle keyless large-LOB plan requested without a large LOB")
	}
	out.SQL = `DECLARE ` + strings.Join(decls, `; `) + `; BEGIN ` + strings.Join(prep, ` `) + ` INSERT INTO ` + oracleQualified(req.Schema, req.Table) + ` (` + strings.Join(names, `,`) + `) VALUES (` + strings.Join(exprs, `,`) + `); ` + strings.Join(cleanup, ` `) + ` END;`
	out.PLSQL = true
	return out, nil
}

func oracleWriteRowPlan(req connector.WriteBatchRequest, row []connector.Value, charset uint16) (oracleWritePlan, error) {
	var out oracleWritePlan
	if len(row) != len(req.Columns) {
		return out, fmt.Errorf("row has %d values for %d columns", len(row), len(req.Columns))
	}
	keys := append([]string(nil), req.PrimaryKeys...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
	}
	colIndex := map[string]int{}
	names := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		colIndex[strings.ToUpper(col.Name)] = i
		names[i] = oracleIdent(col.Name)
	}
	keyed := len(keys) > 0
	if !keyed {
		for i, col := range req.Columns {
			family := oracleColumnFamily(col)
			if (family == "blob" || family == "clob") && !row[i].Null && len(row[i].Raw) > oracleMaxInlineBindBytes {
				return oracleKeylessLargeLOBPlan(req, row, charset)
			}
		}
	}
	exprs := make([]string, len(req.Columns))
	bindPos := 1
	for i, col := range req.Columns {
		family := oracleColumnFamily(col)
		if (family == "blob" || family == "clob") && keyed {
			// Keyed writes use EMPTY_LOB + DBMS_LOB.WRITEAPPEND so arbitrarily
			// large LOB values never need to fit in one TNS DATA packet.
			flag := "1"
			if !row[i].Null {
				flag = "0"
			}
			b, _ := oracleNumberInputBind(flag)
			ph := fmt.Sprintf(":%d", bindPos)
			bindPos++
			if family == "blob" {
				exprs[i] = `CASE WHEN ` + ph + `=1 THEN NULL ELSE EMPTY_BLOB() END`
			} else {
				exprs[i] = `CASE WHEN ` + ph + `=1 THEN NULL ELSE EMPTY_CLOB() END`
			}
			out.Binds = append(out.Binds, b)
			if !row[i].Null {
				out.LargeLOBs = append(out.LargeLOBs, oracleLargeLOB{Column: col, Value: row[i]})
			}
			continue
		}
		expr, bind, err := oracleBindExpr(row[i], col, bindPos, charset)
		if err != nil {
			return out, fmt.Errorf("column %s: %w", col.Name, err)
		}
		bindPos++
		exprs[i] = expr
		out.Binds = append(out.Binds, bind)
	}
	target := oracleQualified(req.Schema, req.Table)
	if len(keys) == 0 {
		out.SQL = `INSERT INTO ` + target + ` (` + strings.Join(names, ",") + `) VALUES (` + strings.Join(exprs, ",") + `)`
		return out, nil
	}
	src := make([]string, len(exprs))
	for i := range exprs {
		src[i] = exprs[i] + ` AS ` + names[i]
	}
	on := make([]string, len(keys))
	keySet := map[string]bool{}
	for i, k := range keys {
		ci, ok := colIndex[strings.ToUpper(k)]
		if !ok {
			return out, fmt.Errorf("primary key %s is not in write columns", k)
		}
		if row[ci].Null {
			return out, fmt.Errorf("primary key %s cannot be null", k)
		}
		keySet[strings.ToUpper(k)] = true
		on[i] = `T.` + oracleIdent(k) + `=S.` + oracleIdent(k)
	}
	sets := []string{}
	for _, col := range req.Columns {
		if !keySet[strings.ToUpper(col.Name)] {
			sets = append(sets, `T.`+oracleIdent(col.Name)+`=S.`+oracleIdent(col.Name))
		}
	}
	out.SQL = `MERGE INTO ` + target + ` T USING (SELECT ` + strings.Join(src, ",") + ` FROM DUAL) S ON (` + strings.Join(on, " AND ") + `)`
	if len(sets) > 0 {
		out.SQL += ` WHEN MATCHED THEN UPDATE SET ` + strings.Join(sets, ",")
	}
	ins := make([]string, len(names))
	for i, n := range names {
		ins[i] = `S.` + n
	}
	out.SQL += ` WHEN NOT MATCHED THEN INSERT (` + strings.Join(names, ",") + `) VALUES (` + strings.Join(ins, ",") + `)`
	return out, nil
}

func oracleBoundKeyPredicate(keys []string, cols []domain.ColumnInfo, vals []connector.Value, charset uint16, start int) (string, []oracleTTCBind, error) {
	if len(keys) == 0 || len(keys) != len(cols) || len(keys) != len(vals) {
		return "", nil, errors.New("Oracle bound key predicate shape mismatch")
	}
	parts := make([]string, len(keys))
	binds := make([]oracleTTCBind, 0, len(keys))
	for i := range keys {
		if vals[i].Null {
			return "", nil, fmt.Errorf("Oracle key %s cannot be NULL", keys[i])
		}
		expr, b, err := oracleBindExpr(vals[i], cols[i], start+i, charset)
		if err != nil {
			return "", nil, fmt.Errorf("Oracle key %s: %w", keys[i], err)
		}
		parts[i] = oracleIdent(keys[i]) + "=" + expr
		binds = append(binds, b)
	}
	return strings.Join(parts, " AND "), binds, nil
}

func oracleKeyBindWhere(req connector.WriteBatchRequest, row []connector.Value, charset uint16, start int) (string, []oracleTTCBind, error) {
	keys := append([]string(nil), req.PrimaryKeys...)
	if len(keys) == 0 && req.PrimaryKey != "" {
		keys = []string{req.PrimaryKey}
	}
	if len(keys) == 0 {
		return "", nil, errors.New("Oracle LOB streaming requires a migration key")
	}
	byName := map[string]int{}
	for i, col := range req.Columns {
		byName[strings.ToUpper(col.Name)] = i
	}
	where := make([]string, len(keys))
	binds := make([]oracleTTCBind, 0, len(keys))
	pos := start
	for i, key := range keys {
		ci, ok := byName[strings.ToUpper(key)]
		if !ok || ci >= len(row) {
			return "", nil, fmt.Errorf("Oracle LOB key %s missing", key)
		}
		if row[ci].Null {
			return "", nil, fmt.Errorf("Oracle LOB key %s is NULL", key)
		}
		expr, b, err := oracleBindExpr(row[ci], req.Columns[ci], pos, charset)
		if err != nil {
			return "", nil, err
		}
		where[i] = oracleIdent(key) + `=` + expr
		binds = append(binds, b)
		pos++
	}
	return strings.Join(where, " AND "), binds, nil
}

func splitUTF8Chunks(b []byte, max int) ([][]byte, error) {
	if !utf8.Valid(b) {
		return nil, errors.New("Oracle CLOB input is not valid UTF-8")
	}
	out := [][]byte{}
	for len(b) > 0 {
		n := max
		if n > len(b) {
			n = len(b)
		}
		for n > 0 && n < len(b) && !utf8.RuneStart(b[n]) {
			n--
		}
		if n <= 0 {
			return nil, errors.New("unable to split Oracle CLOB UTF-8 chunk")
		}
		out = append(out, append([]byte(nil), b[:n]...))
		b = b[n:]
	}
	return out, nil
}

func (c *Connector) appendOracleLOBByKey(ctx context.Context, req connector.WriteBatchRequest, row []connector.Value, lob oracleLargeLOB) error {
	family := oracleColumnFamily(lob.Column)
	var chunks [][]byte
	var err error
	if family == "clob" {
		chunks, err = splitUTF8Chunks(lob.Value.Raw, oracleLOBChunkBytes)
	} else {
		for b := lob.Value.Raw; len(b) > 0; {
			n := oracleLOBChunkBytes
			if n > len(b) {
				n = len(b)
			}
			chunks = append(chunks, append([]byte(nil), b[:n]...))
			b = b[n:]
		}
	}
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		return nil
	}
	charset := c.proto.ServerCharset
	for _, chunk := range chunks {
		where, keyBinds, err := oracleKeyBindWhere(req, row, charset, 1)
		if err != nil {
			return err
		}
		chunkPos := len(keyBinds) + 1
		var chunkBind oracleTTCBind
		var writeCall string
		if family == "clob" {
			chunkBind = oracleStringInputBind(chunk, charset)
			writeCall = fmt.Sprintf(`DBMS_LOB.WRITEAPPEND(L,LENGTH(:%d),:%d);`, chunkPos, chunkPos)
		} else {
			chunkBind = oracleRawInputBind(chunk)
			writeCall = fmt.Sprintf(`DBMS_LOB.WRITEAPPEND(L,UTL_RAW.LENGTH(:%d),:%d);`, chunkPos, chunkPos)
		}
		binds := append(keyBinds, chunkBind)
		lobType := "BLOB"
		if family == "clob" {
			lobType = "CLOB"
		}
		plsql := `DECLARE L ` + lobType + `; BEGIN SELECT ` + oracleIdent(lob.Column.Name) + ` INTO L FROM ` + oracleQualified(req.Schema, req.Table) + ` WHERE ` + where + ` FOR UPDATE; ` + writeCall + ` END;`
		if _, err = c.execBound(ctx, plsql, [][]oracleTTCBind{binds}, true); err != nil {
			return fmt.Errorf("Oracle %s append %s: %w", family, lob.Column.Name, err)
		}
	}
	return nil
}

func (c *Connector) writeBatchNative(ctx context.Context, req connector.WriteBatchRequest) (written int64, err error) {
	if len(req.Columns) == 0 || len(req.Rows) == 0 {
		return 0, nil
	}
	// Ensure the session exists before planning binds so its negotiated database
	// charset id is available in bind descriptors.
	c.mu.Lock()
	if err = c.ensureNativeSessionLocked(ctx); err != nil {
		c.mu.Unlock()
		return 0, err
	}
	charset := c.proto.ServerCharset
	outerTx := c.inTransaction
	c.mu.Unlock()

	plans := make([]oracleWritePlan, len(req.Rows))
	for i, row := range req.Rows {
		plans[i], err = oracleWriteRowPlan(req, row, charset)
		if err != nil {
			return written, fmt.Errorf("Oracle write row %d: %w", i, err)
		}
	}
	rollback := func() {
		if !outerTx {
			_, _ = c.execSQL(context.Background(), "ROLLBACK")
		}
	}
	for i := 0; i < len(plans); {
		plan := plans[i]
		if len(plan.LargeLOBs) > 0 {
			if _, err = c.execBound(ctx, plan.SQL, [][]oracleTTCBind{plan.Binds}, plan.PLSQL); err != nil {
				rollback()
				return written, fmt.Errorf("Oracle write row %d: %w", i, err)
			}
			for _, lob := range plan.LargeLOBs {
				if err = c.appendOracleLOBByKey(ctx, req, req.Rows[i], lob); err != nil {
					rollback()
					return written, fmt.Errorf("Oracle write row %d LOB %s: %w", i, lob.Column.Name, err)
				}
			}
			written++
			i++
			continue
		}
		// Consecutive scalar DML rows with the same SQL/bind descriptor use TTC
		// array binding. Anonymous PL/SQL LOB builders stay one-row-per-call.
		end := i + 1
		rows := [][]oracleTTCBind{plan.Binds}
		for !plan.PLSQL && end < len(plans) && end-i < oracleMaxArrayRows && !plans[end].PLSQL && len(plans[end].LargeLOBs) == 0 && plans[end].SQL == plan.SQL {
			candidate := append(append([][]oracleTTCBind(nil), rows...), plans[end].Binds)
			if _, _, e := buildTTCBindStatementRequest(plan.SQL, c.data.TTCVersion, candidate, false); e != nil {
				break
			}
			rows = candidate
			end++
		}
		if _, err = c.execBound(ctx, plan.SQL, rows, plan.PLSQL); err != nil {
			rollback()
			return written, fmt.Errorf("Oracle write rows %d..%d: %w", i, end-1, err)
		}
		written += int64(end - i)
		i = end
	}
	if !outerTx {
		if _, err = c.execSQL(ctx, "COMMIT"); err != nil {
			rollback()
			return written, err
		}
	}
	return written, nil
}
