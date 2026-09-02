package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"qmigration/backend/internal/cdc/db2log"
	"qmigration/backend/internal/connector"
	db2connector "qmigration/backend/internal/connector/db2"
	"qmigration/backend/internal/domain"
)

const toolVersion = "0.15.0-rc49"

type status string

const (
	pass status = "PASS"
	fail status = "FAIL"
	skip status = "SKIP"
)

type check struct {
	Name       string         `json:"name"`
	Status     status         `json:"status"`
	DurationMS int64          `json:"duration_ms"`
	Message    string         `json:"message,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
}
type report struct {
	ToolVersion   string               `json:"tool_version"`
	GeneratedAt   string               `json:"generated_at_utc"`
	Target        map[string]any       `json:"target"`
	Descriptor    connector.Descriptor `json:"descriptor"`
	ServerVersion string               `json:"server_version,omitempty"`
	Checks        []check              `json:"checks"`
	Passed        int                  `json:"passed"`
	Failed        int                  `json:"failed"`
	Skipped       int                  `json:"skipped"`
	Qualified     bool                 `json:"qualified"`
}
type runner struct {
	ctx context.Context
	rep *report
}

func (r *runner) run(name string, fn func() (string, map[string]any, error)) bool {
	st := time.Now()
	msg, d, e := fn()
	x := check{Name: name, DurationMS: time.Since(st).Milliseconds(), Message: msg, Details: d}
	if e != nil {
		x.Status = fail
		if msg == "" {
			x.Message = e.Error()
		} else {
			x.Message += ": " + e.Error()
		}
		r.rep.Failed++
	} else {
		x.Status = pass
		r.rep.Passed++
	}
	r.rep.Checks = append(r.rep.Checks, x)
	return e == nil
}
func (r *runner) skip(name, reason string) {
	r.rep.Checks = append(r.rep.Checks, check{Name: name, Status: skip, Message: reason})
	r.rep.Skipped++
}
func readFile(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	b, e := os.ReadFile(path)
	return string(b), e
}

func main() {
	var (
		host          = flag.String("host", "", "DB2 LUW host")
		port          = flag.Int("port", 50000, "DB2 DRDA port")
		database      = flag.String("database", "", "DB2 RDB/database name")
		user          = flag.String("user", "", "DB2 username")
		passwordEnv   = flag.String("password-env", "DB2_PASSWORD", "environment variable containing password")
		schema        = flag.String("schema", "", "schema to inspect; defaults to upper-case user")
		table         = flag.String("table", "", "optional table to sample/read")
		sampleRows    = flag.Int("sample-rows", 16, "maximum sample rows")
		targetWrite   = flag.Bool("target-write", false, "run destructive target write qualification using a temporary table")
		targetVector  = flag.Bool("target-vector", false, "with --target-write, qualify Db2 12.1.2+ VECTOR create/prepared write/read round-trip")
		cdc           = flag.Bool("cdc", false, "qualify DB2 db2ReadLog source CDC through QMigration DB2 Log Agent")
		cdcURL        = flag.String("cdc-url", "", "QMigration DB2 Log Agent URL (db2log:// or db2logs://)")
		cdcTokenEnv   = flag.String("cdc-token-env", "QMIGRATION_DB2_LOG_TOKEN", "optional environment variable containing Log Agent bearer token")
		cdcCAFile     = flag.String("cdc-ca-file", "", "optional PEM CA for DB2 Log Agent HTTPS")
		cdcServerName = flag.String("cdc-server-name", "", "optional DB2 Log Agent TLS server name")
		timeout       = flag.Duration("timeout", 90*time.Second, "overall qualification timeout")
		tlsMode       = flag.String("tls-mode", "PREFERRED", "DISABLE, PREFERRED, or REQUIRED")
		tlsServerName = flag.String("tls-server-name", "", "TLS certificate server name")
		caFile        = flag.String("tls-ca-file", "", "CA PEM file")
		certFile      = flag.String("tls-cert-file", "", "client certificate PEM file")
		keyFile       = flag.String("tls-key-file", "", "client private key PEM file")
		outFile       = flag.String("output", "", "optional JSON report output file")
	)
	flag.Parse()
	if strings.TrimSpace(*host) == "" || strings.TrimSpace(*database) == "" || strings.TrimSpace(*user) == "" {
		fmt.Fprintln(os.Stderr, "--host, --database and --user are required")
		os.Exit(2)
	}
	if *targetVector && !*targetWrite {
		fmt.Fprintln(os.Stderr, "--target-vector requires --target-write")
		os.Exit(2)
	}
	if *sampleRows < 1 || *sampleRows > 1024 {
		fmt.Fprintln(os.Stderr, "--sample-rows must be between 1 and 1024")
		os.Exit(2)
	}
	password := os.Getenv(*passwordEnv)
	if password == "" {
		fmt.Fprintf(os.Stderr, "password environment variable %s is empty\n", *passwordEnv)
		os.Exit(2)
	}
	ca, e := readFile(*caFile)
	fatal(e)
	cert, e := readFile(*certFile)
	fatal(e)
	key, e := readFile(*keyFile)
	fatal(e)
	cdcCA, e := readFile(*cdcCAFile)
	fatal(e)
	_ = os.Setenv("QMIGRATION_EXPERIMENTAL_DB2_NATIVE", "1")
	if *cdc {
		_ = os.Setenv("QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC", "1")
	}
	ds := domain.DataSource{Type: domain.DataSourceDB2, Host: strings.TrimSpace(*host), Port: *port, Username: strings.TrimSpace(*user), Password: password, Database: strings.TrimSpace(*database), Schema: strings.TrimSpace(*schema), CDCURL: strings.TrimSpace(*cdcURL), TLSMode: domain.TLSMode(strings.ToUpper(strings.TrimSpace(*tlsMode))), TLSServerName: strings.TrimSpace(*tlsServerName), TLSCACert: ca, TLSClientCert: cert, TLSClientKey: key}
	f := db2connector.NewFactory()
	base, e := f.New(ds)
	fatal(e)
	defer base.Close()
	rep := report{ToolVersion: toolVersion, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Target: map[string]any{"host": ds.Host, "port": ds.Port, "database": ds.Database, "user": ds.Username, "tls_mode": ds.TLSMode, "cdc": *cdc, "cdc_url": ds.CDCURL}, Descriptor: f.Capabilities(domain.DataSourceDB2)}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	r := &runner{ctx: ctx, rep: &rep}
	if !r.run("connection", func() (string, map[string]any, error) {
		return "DRDA authentication succeeded", nil, base.TestConnection(ctx)
	}) {
		finish(rep, *outFile)
		os.Exit(1)
	}
	r.run("version", func() (string, map[string]any, error) {
		v, e := base.GetVersion(ctx)
		if e == nil {
			rep.ServerVersion = v
		}
		return v, nil, e
	})
	if pc, ok := base.(connector.MigrationPrecheckConnector); ok {
		r.run("migration-prechecks", func() (string, map[string]any, error) {
			items := pc.MigrationPrechecks(ctx, *cdc)
			var failed, warn []string
			for _, it := range items {
				if it.Level == domain.PrecheckFailed {
					failed = append(failed, it.Name+": "+it.Message)
				} else if it.Level == domain.PrecheckWarning {
					warn = append(warn, it.Name+": "+it.Message)
				}
			}
			if len(failed) > 0 {
				return strings.Join(warn, "; "), map[string]any{"items": items}, errors.New(strings.Join(failed, "; "))
			}
			return strings.Join(warn, "; "), map[string]any{"items": items}, nil
		})
	}
	r.run("metadata-schemas", func() (string, map[string]any, error) {
		xs, e := base.ListSchemas(ctx)
		return fmt.Sprintf("%d schemas visible", len(xs)), map[string]any{"count": len(xs)}, e
	})
	selectedSchema := strings.TrimSpace(*schema)
	if selectedSchema == "" {
		selectedSchema = strings.ToUpper(strings.TrimSpace(*user))
	}
	var tables []domain.TableInfo
	r.run("metadata-tables", func() (string, map[string]any, error) {
		var e error
		tables, e = base.ListTables(ctx, selectedSchema)
		return fmt.Sprintf("%d tables visible in %s", len(tables), selectedSchema), map[string]any{"count": len(tables), "schema": selectedSchema}, e
	})
	selectedTable := strings.TrimSpace(*table)
	if selectedTable == "" && len(tables) > 0 {
		selectedTable = tables[0].Name
	}
	var meta *domain.TableMetadata
	if selectedTable == "" {
		r.skip("table-metadata", "no table supplied or visible")
		r.skip("full-read-sample", "no table supplied or visible")
	} else {
		r.run("table-metadata", func() (string, map[string]any, error) {
			var e error
			meta, e = base.GetTableMetadata(ctx, selectedSchema, selectedTable)
			if e != nil {
				return "", nil, e
			}
			return fmt.Sprintf("%s.%s columns=%d estimated_rows=%d", selectedSchema, selectedTable, len(meta.Columns), meta.EstimatedRows), map[string]any{"columns": len(meta.Columns), "primary_keys": meta.PrimaryKeys, "estimated_rows": meta.EstimatedRows}, nil
		})
		if dc, ok := base.(connector.DataConnector); ok && meta != nil {
			r.run("full-read-sample", func() (string, map[string]any, error) {
				req := connector.ReadBatchRequest{Schema: selectedSchema, Table: selectedTable, Columns: meta.Columns, Limit: *sampleRows, PrimaryKey: meta.PrimaryKey, PrimaryKeys: meta.PrimaryKeys}
				if len(meta.PrimaryKeys) > 0 {
					req.UseKeyset = true
				}
				b, e := dc.ReadBatch(ctx, req)
				if e != nil {
					return "", nil, e
				}
				return fmt.Sprintf("sampled %d rows (%d bytes)", len(b.Rows), b.Bytes), map[string]any{"rows": len(b.Rows), "bytes": b.Bytes}, nil
			})
		}
	}
	if sc, ok := base.(connector.SchemaObjectConnector); ok {
		r.run("schema-objects", func() (string, map[string]any, error) {
			objs, e := sc.ListSchemaObjects(ctx, selectedSchema)
			return fmt.Sprintf("%d schema objects discovered", len(objs)), map[string]any{"count": len(objs)}, e
		})
	}
	if *cdc {
		if selectedTable == "" {
			r.run("source-cdc", func() (string, map[string]any, error) {
				return "", nil, errors.New("--cdc requires --table or a visible table")
			})
		} else {
			r.run("source-cdc-selection", func() (string, map[string]any, error) {
				v, ok := base.(connector.CDCSelectionValidator)
				if !ok {
					return "", nil, errors.New("DB2 connector does not implement CDC selection validation")
				}
				m := domain.TableMapping{SourceSchema: selectedSchema, SourceTable: selectedTable}
				if err := v.ValidateCDCSelection(ctx, []domain.TableMapping{m}); err != nil {
					return "", nil, err
				}
				return "DATA CAPTURE CHANGES + primary key + logged LOB/XML CDC prerequisites + Log Agent health passed", map[string]any{"table": selectedSchema + "." + selectedTable}, nil
			})
			r.run("source-cdc-position-descriptor", func() (string, map[string]any, error) {
				src, ok := base.(*db2connector.Connector)
				if !ok {
					return "", nil, errors.New("DB2 native connector type assertion failed")
				}
				pos, err := src.CurrentCDCPosition(ctx)
				if err != nil {
					return "", nil, err
				}
				specs, err := src.CDCSelections(ctx, []string{selectedSchema + "." + selectedTable})
				if err != nil {
					return "", nil, err
				}
				if len(specs) != 1 {
					return "", nil, errors.New("unexpected DB2 CDC selection count")
				}
				agent, err := db2log.NewClient(ds.CDCURL, cdcCA, *cdcServerName, os.Getenv(*cdcTokenEnv))
				if err != nil {
					return "", nil, err
				}
				lri, err := db2log.ParseLRI(pos.PositionValue)
				if err != nil {
					return "", nil, err
				}
				sp := specs[0]
				boot, err := agent.Bootstrap(ctx, db2log.BootstrapRequest{EndLRI: lri, Tables: []db2log.TableIdentity{{Schema: sp.Schema, Table: sp.Table, TablespaceID: sp.TablespaceID, TableID: sp.TableID}}})
				if err != nil {
					return "", nil, err
				}
				if len(boot.Records) != 1 {
					return "", nil, fmt.Errorf("descriptor bootstrap returned %d records", len(boot.Records))
				}
				parsed, err := db2log.ParseDataManager(boot.Records[0], nil, nil)
				if err != nil {
					return "", nil, err
				}
				if parsed == nil || parsed.Descriptor == nil || len(parsed.Descriptor.Fields) != len(sp.Columns) {
					return "", nil, errors.New("DB2 Initialize Table descriptor did not match catalog columns")
				}
				return "DB2_LRI captured and Initialize Table descriptor bootstrapped", map[string]any{"position": pos.PositionValue, "tablespace_id": sp.TablespaceID, "table_id": sp.TableID, "descriptor_columns": len(parsed.Descriptor.Fields)}, nil
			})
		}
	} else {
		r.skip("source-cdc", "enable --cdc with a qualified QMigration DB2 Log Agent to validate db2ReadLog")
	}
	if *targetWrite {
		runTargetWrite(r, base, selectedSchema, *targetVector)
	} else {
		r.skip("target-write-transaction", "destructive target test disabled; enable with --target-write")
	}
	finish(rep, *outFile)
	if rep.Failed > 0 {
		os.Exit(1)
	}
}

func runTargetWrite(r *runner, base connector.Connector, schema string, targetVector bool) {
	sc, ok1 := base.(connector.CompositeSchemaConnector)
	dc, ok2 := base.(connector.DataConnector)
	pc, ok3 := base.(connector.PointLookupConnector)
	tx, ok4 := base.(connector.TransactionalCDCApplyConnector)
	ddl, ok5 := base.(connector.DDLApplyConnector)
	post, ok6 := base.(connector.PostLoadSchemaConnector)
	state, ok7 := base.(connector.GeneratedValueStateConnector)
	finalizer, ok8 := base.(connector.CutoverGeneratedValueConnector)
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7 && ok8) {
		r.skip("target-write-transaction", "required DB2 target SPI capabilities are incomplete")
		return
	}
	table := fmt.Sprintf("QMQUAL_%X", time.Now().UnixNano()&0xffffff)
	cols := []domain.ColumnInfo{
		{Name: "ID", DataType: "bigint", ColumnType: "BIGINT", Nullable: false, Extra: "IDENTITY_ALWAYS(100,5)"},
		{Name: "NAME", DataType: "varchar", ColumnType: "VARCHAR(256)", Nullable: true},
		{Name: "PAYLOAD", DataType: "blob", ColumnType: "BLOB", Nullable: true},
		{Name: "NOTE", DataType: "clob", ColumnType: "CLOB", Nullable: true},
	}
	drop := fmt.Sprintf(`DROP TABLE "%s"."%s"`, strings.ReplaceAll(schema, `"`, `""`), table)
	_ = ddl.ExecDDL(r.ctx, schema, drop)
	defer func() { _ = ddl.ExecDDL(context.Background(), schema, drop) }()
	if !r.run("target-write-create", func() (string, map[string]any, error) {
		e := sc.CreateTableWithPrimaryKeys(r.ctx, schema, table, cols, []string{"ID"})
		return table, map[string]any{"table": table}, e
	}) {
		return
	}
	r.run("target-write-prepared-extdta-lob", func() (string, map[string]any, error) {
		blob := bytes.Repeat([]byte{0x00, 0x7f, 0xff, 0x42}, 512*1024) // 2 MiB
		clob := bytes.Repeat([]byte("QMigration-DB2-EXTDTA-中文-"), 8192)
		rows := [][]connector.Value{{
			{Raw: []byte("100")},
			{Raw: []byte("db2-native")},
			{Raw: blob},
			{Raw: clob},
		}}
		_, e := dc.WriteBatch(r.ctx, connector.WriteBatchRequest{Schema: schema, Table: table, Columns: cols, PrimaryKeys: []string{"ID"}, Rows: rows})
		if e != nil {
			return "", nil, e
		}
		vals, found, e := pc.ReadByKey(r.ctx, connector.ReadByKeyRequest{
			Schema: schema, Table: table, PrimaryKeys: []string{"ID"}, KeyColumns: []domain.ColumnInfo{cols[0]},
			KeyValues: []connector.Value{{Raw: []byte("100")}}, Columns: cols,
		})
		if e != nil {
			return "", nil, e
		}
		if !found || len(vals) != 4 || string(vals[1].Raw) != "db2-native" || !bytes.Equal(vals[2].Raw, blob) || !bytes.Equal(vals[3].Raw, clob) {
			return "", nil, errors.New("DB2 prepared/EXTDTA target LOB round-trip mismatch")
		}
		return "prepared MERGE + multi-segment BLOB/CLOB EXTDTA round-trip passed", map[string]any{"blob_bytes": len(blob), "clob_bytes": len(clob)}, nil
	})
	r.run("target-write-identity-state", func() (string, map[string]any, error) {
		if e := state.SyncGeneratedValueState(r.ctx, schema, table, cols); e != nil {
			return "", nil, e
		}
		if e := ddl.ExecDDL(r.ctx, schema, fmt.Sprintf(`INSERT INTO "%s"."%s" ("NAME") VALUES ('identity-generated')`, strings.ReplaceAll(schema, `"`, `""`), table)); e != nil {
			return "", nil, e
		}
		_, found, e := pc.ReadByKey(r.ctx, connector.ReadByKeyRequest{Schema: schema, Table: table, PrimaryKeys: []string{"ID"}, KeyColumns: []domain.ColumnInfo{cols[0]}, KeyValues: []connector.Value{{Raw: []byte("105")}}, Columns: cols})
		if e != nil {
			return "", nil, e
		}
		if !found {
			return "", nil, errors.New("identity RESTART WITH did not generate expected next value 105")
		}
		return "identity START/INCREMENT + RESTART WITH passed", map[string]any{"expected_generated_id": 105}, nil
	})
	r.run("target-write-rollback", func() (string, map[string]any, error) {
		if e := tx.BeginCDCTransaction(r.ctx); e != nil {
			return "", nil, e
		}
		rows := [][]connector.Value{{
			{Raw: []byte("200")},
			{Raw: []byte("rollback")},
			{Null: true},
			{Null: true},
		}}
		_, e := dc.WriteBatch(r.ctx, connector.WriteBatchRequest{Schema: schema, Table: table, Columns: cols, PrimaryKeys: []string{"ID"}, Rows: rows})
		if e != nil {
			_ = tx.RollbackCDCTransaction(r.ctx)
			return "", nil, e
		}
		if e = tx.RollbackCDCTransaction(r.ctx); e != nil {
			return "", nil, e
		}
		_, found, e := pc.ReadByKey(r.ctx, connector.ReadByKeyRequest{
			Schema: schema, Table: table, PrimaryKeys: []string{"ID"}, KeyColumns: []domain.ColumnInfo{cols[0]},
			KeyValues: []connector.Value{{Raw: []byte("200")}}, Columns: cols,
		})
		if e != nil {
			return "", nil, e
		}
		if found {
			return "", nil, errors.New("rollback row is still visible")
		}
		return "transaction rollback passed", nil, nil
	})
	r.run("target-write-cdc-identity-state", func() (string, map[string]any, error) {
		if e := tx.BeginCDCTransaction(r.ctx); e != nil {
			return "", nil, e
		}
		rows := [][]connector.Value{{{Raw: []byte("200")}, {Raw: []byte("cdc-identity")}, {Null: true}, {Null: true}}}
		if _, e := dc.WriteBatch(r.ctx, connector.WriteBatchRequest{Schema: schema, Table: table, Columns: cols, PrimaryKeys: []string{"ID"}, Rows: rows}); e != nil {
			_ = tx.RollbackCDCTransaction(r.ctx)
			return "", nil, e
		}
		if e := tx.CommitCDCTransaction(r.ctx); e != nil {
			return "", nil, e
		}
		if e := ddl.ExecDDL(r.ctx, schema, fmt.Sprintf(`INSERT INTO "%s"."%s" ("NAME") VALUES ('post-cdc-generated')`, strings.ReplaceAll(schema, `"`, `""`), table)); e != nil {
			return "", nil, e
		}
		_, found, e := pc.ReadByKey(r.ctx, connector.ReadByKeyRequest{Schema: schema, Table: table, PrimaryKeys: []string{"ID"}, KeyColumns: []domain.ColumnInfo{cols[0]}, KeyValues: []connector.Value{{Raw: []byte("205")}}, Columns: cols})
		if e != nil {
			return "", nil, e
		}
		if !found {
			return "", nil, errors.New("committed CDC identity state did not advance to 205")
		}
		return "transactional CDC identity synchronization passed", map[string]any{"expected_generated_id": 205}, nil
	})
	r.run("target-write-index", func() (string, map[string]any, error) {
		e := post.CreateIndex(r.ctx, schema, table, domain.IndexInfo{Name: table + "_NAME_IDX", Columns: []string{"NAME"}})
		return "secondary index create passed", nil, e
	})
	if targetVector {
		runTargetVectorWrite(r, sc, dc, pc, ddl, schema)
	} else {
		r.skip("target-write-vector", "Db2 VECTOR target test disabled; enable with --target-write --target-vector on Db2 12.1.2+")
	}
	r.run("target-write-cutover-identity-mode", func() (string, map[string]any, error) {
		if e := finalizer.FinalizeGeneratedValueModes(r.ctx, schema, table, cols); e != nil {
			return "", nil, e
		}
		// A source GENERATED ALWAYS column is staged as BY DEFAULT so exact
		// values can be propagated. After finalization, an explicit value must
		// once again be rejected by Db2 LUW.
		_, e := dc.WriteBatch(r.ctx, connector.WriteBatchRequest{
			Schema: schema, Table: table, Columns: cols, PrimaryKeys: []string{"ID"},
			Rows: [][]connector.Value{{{Raw: []byte("999")}, {Raw: []byte("must-fail")}, {Null: true}, {Null: true}}},
		})
		if e == nil {
			return "", nil, errors.New("GENERATED ALWAYS finalization did not reject explicit identity value")
		}
		if e := ddl.ExecDDL(r.ctx, schema, fmt.Sprintf(`INSERT INTO "%s"."%s" ("NAME") VALUES ('post-cutover-generated')`, strings.ReplaceAll(schema, `"`, `""`), table)); e != nil {
			return "", nil, e
		}
		_, found, e := pc.ReadByKey(r.ctx, connector.ReadByKeyRequest{Schema: schema, Table: table, PrimaryKeys: []string{"ID"}, KeyColumns: []domain.ColumnInfo{cols[0]}, KeyValues: []connector.Value{{Raw: []byte("210")}}, Columns: cols})
		if e != nil {
			return "", nil, e
		}
		if !found {
			return "", nil, errors.New("post-cutover GENERATED ALWAYS identity did not generate expected value 210")
		}
		return "migration-stage BY DEFAULT restored to source GENERATED ALWAYS", map[string]any{"expected_generated_id": 210}, nil
	})
}

func runTargetVectorWrite(r *runner, sc connector.CompositeSchemaConnector, dc connector.DataConnector, pc connector.PointLookupConnector, ddl connector.DDLApplyConnector, schema string) {
	table := fmt.Sprintf("QMQUALV_%X", time.Now().UnixNano()&0xffffff)
	cols := []domain.ColumnInfo{
		{Name: "ID", DataType: "bigint", ColumnType: "BIGINT", Nullable: false},
		{Name: "VF", DataType: "vector", ColumnType: "VECTOR(3,FLOAT32)", Nullable: true},
		{Name: "VI", DataType: "vector", ColumnType: "VECTOR(4,INT8)", Nullable: true},
	}
	drop := fmt.Sprintf(`DROP TABLE "%s"."%s"`, strings.ReplaceAll(schema, `"`, `""`), table)
	_ = ddl.ExecDDL(r.ctx, schema, drop)
	defer func() { _ = ddl.ExecDDL(context.Background(), schema, drop) }()
	if !r.run("target-write-vector-create", func() (string, map[string]any, error) {
		e := sc.CreateTableWithPrimaryKeys(r.ctx, schema, table, cols, []string{"ID"})
		return "VECTOR(3,FLOAT32) + VECTOR(4,INT8) create passed", map[string]any{"table": table}, e
	}) {
		return
	}
	r.run("target-write-vector", func() (string, map[string]any, error) {
		rows := [][]connector.Value{{
			{Raw: []byte("1")},
			{Raw: []byte("[0.5,-0.25,3.5]")},
			{Raw: []byte("[-128,0,1,127]")},
		}}
		if _, e := dc.WriteBatch(r.ctx, connector.WriteBatchRequest{Schema: schema, Table: table, Columns: cols, PrimaryKeys: []string{"ID"}, Rows: rows}); e != nil {
			return "", nil, e
		}
		vals, found, e := pc.ReadByKey(r.ctx, connector.ReadByKeyRequest{
			Schema: schema, Table: table, PrimaryKeys: []string{"ID"}, KeyColumns: []domain.ColumnInfo{cols[0]},
			KeyValues: []connector.Value{{Raw: []byte("1")}}, Columns: cols,
		})
		if e != nil {
			return "", nil, e
		}
		if !found || len(vals) != 3 {
			return "", nil, errors.New("DB2 VECTOR target row was not readable after prepared write")
		}
		vf := strings.ReplaceAll(strings.TrimSpace(string(vals[1].Raw)), " ", "")
		vi := strings.ReplaceAll(strings.TrimSpace(string(vals[2].Raw)), " ", "")
		if vf != "[0.5,-0.25,3.5]" || vi != "[-128,0,1,127]" {
			return "", nil, fmt.Errorf("DB2 VECTOR round-trip mismatch VF=%q VI=%q", vf, vi)
		}
		return "VECTOR constructor + prepared bind + VECTOR_SERIALIZE round-trip passed", map[string]any{"float32": vf, "int8": vi}, nil
	})
}

func finish(rep report, out string) {
	rep.Qualified = rep.Failed == 0
	b, e := json.MarshalIndent(rep, "", "  ")
	fatal(e)
	fmt.Println(string(b))
	if strings.TrimSpace(out) != "" {
		fatal(os.WriteFile(strings.TrimSpace(out), append(b, '\n'), 0600))
	}
}
func fatal(e error) {
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
