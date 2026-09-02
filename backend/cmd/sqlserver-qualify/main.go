package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"qmigration/backend/internal/connector"
	sqlserverconnector "qmigration/backend/internal/connector/sqlserver"
	"qmigration/backend/internal/domain"
)

const toolVersion = "0.15.0-rc49"

type checkStatus string

const (
	statusPass checkStatus = "PASS"
	statusFail checkStatus = "FAIL"
	statusSkip checkStatus = "SKIP"
)

type checkResult struct {
	Name       string         `json:"name"`
	Status     checkStatus    `json:"status"`
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
	Checks        []checkResult        `json:"checks"`
	Passed        int                  `json:"passed"`
	Failed        int                  `json:"failed"`
	Skipped       int                  `json:"skipped"`
	Qualified     bool                 `json:"qualified"`
}

type runner struct {
	ctx    context.Context
	conn   connector.Connector
	report *report
}

func (r *runner) run(name string, fn func() (string, map[string]any, error)) bool {
	started := time.Now()
	msg, details, err := fn()
	item := checkResult{Name: name, DurationMS: time.Since(started).Milliseconds(), Message: msg, Details: details}
	if err != nil {
		item.Status = statusFail
		if item.Message == "" {
			item.Message = err.Error()
		} else {
			item.Message += ": " + err.Error()
		}
		r.report.Failed++
	} else {
		item.Status = statusPass
		r.report.Passed++
	}
	r.report.Checks = append(r.report.Checks, item)
	return err == nil
}

func (r *runner) skip(name, reason string) {
	r.report.Checks = append(r.report.Checks, checkResult{Name: name, Status: statusSkip, Message: reason})
	r.report.Skipped++
}

func readFile(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func main() {
	var (
		host          = flag.String("host", "", "SQL Server host")
		port          = flag.Int("port", 1433, "SQL Server port")
		database      = flag.String("database", "", "database name")
		user          = flag.String("user", "", "login username")
		passwordEnv   = flag.String("password-env", "SQLSERVER_PASSWORD", "environment variable containing password")
		schema        = flag.String("schema", "dbo", "schema to inspect")
		table         = flag.String("table", "", "optional table to sample/read")
		sampleRows    = flag.Int("sample-rows", 16, "maximum sample rows")
		cdc           = flag.Bool("cdc", false, "qualify SQL Server CDC prerequisites and current LSN")
		targetWrite   = flag.Bool("target-write", false, "run destructive target write qualification using a temporary table")
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
	password := os.Getenv(*passwordEnv)
	if password == "" {
		fmt.Fprintf(os.Stderr, "password environment variable %s is empty\n", *passwordEnv)
		os.Exit(2)
	}
	if *sampleRows < 1 || *sampleRows > 1024 {
		fmt.Fprintln(os.Stderr, "--sample-rows must be between 1 and 1024")
		os.Exit(2)
	}
	ca, err := readFile(*caFile)
	fatalIf(err)
	cert, err := readFile(*certFile)
	fatalIf(err)
	key, err := readFile(*keyFile)
	fatalIf(err)
	_ = os.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	if *cdc {
		_ = os.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC", "1")
	}

	ds := domain.DataSource{Type: domain.DataSourceSQLServer, Host: strings.TrimSpace(*host), Port: *port, Username: strings.TrimSpace(*user), Password: password, Database: strings.TrimSpace(*database), TLSMode: domain.TLSMode(strings.ToUpper(strings.TrimSpace(*tlsMode))), TLSServerName: strings.TrimSpace(*tlsServerName), TLSCACert: ca, TLSClientCert: cert, TLSClientKey: key}
	factory := sqlserverconnector.NewFactory()
	base, err := factory.New(ds)
	fatalIf(err)
	defer base.Close()
	rep := report{ToolVersion: toolVersion, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Target: map[string]any{"host": ds.Host, "port": ds.Port, "database": ds.Database, "user": ds.Username, "tls_mode": ds.TLSMode}, Descriptor: factory.Capabilities(domain.DataSourceSQLServer)}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	r := &runner{ctx: ctx, conn: base, report: &rep}

	if !r.run("connection", func() (string, map[string]any, error) {
		return "TDS LOGIN7 authentication succeeded", nil, base.TestConnection(ctx)
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
			failed := []string{}
			warnings := []string{}
			for _, item := range items {
				if item.Level == domain.PrecheckFailed {
					failed = append(failed, item.Name+": "+item.Message)
				} else if item.Level == domain.PrecheckWarning {
					warnings = append(warnings, item.Name+": "+item.Message)
				}
			}
			if len(failed) > 0 {
				return strings.Join(warnings, "; "), map[string]any{"items": items}, errors.New(strings.Join(failed, "; "))
			}
			return strings.Join(warnings, "; "), map[string]any{"items": items}, nil
		})
	}
	var tables []domain.TableInfo
	r.run("metadata-schemas", func() (string, map[string]any, error) {
		xs, e := base.ListSchemas(ctx)
		return fmt.Sprintf("%d schemas visible", len(xs)), map[string]any{"count": len(xs)}, e
	})
	selectedSchema := strings.TrimSpace(*schema)
	if selectedSchema == "" {
		selectedSchema = "dbo"
	}
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
		r.skip("partition-discovery", "no table supplied or visible")
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
		if pc, ok := base.(connector.PartitionConnector); ok {
			r.run("partition-discovery", func() (string, map[string]any, error) {
				parts, e := pc.ListTablePartitions(ctx, selectedSchema, selectedTable)
				return fmt.Sprintf("%d partitions", len(parts)), map[string]any{"partitions": parts}, e
			})
		}
	}
	if lc, ok := base.(connector.RuntimeLoadConnector); ok {
		r.run("runtime-load", func() (string, map[string]any, error) {
			load, e := lc.SampleRuntimeLoad(ctx)
			return "runtime pressure sample collected", map[string]any{"load": load}, e
		})
	}
	if sc, ok := base.(connector.SchemaObjectConnector); ok {
		r.run("schema-objects", func() (string, map[string]any, error) {
			objs, e := sc.ListSchemaObjects(ctx, selectedSchema)
			return fmt.Sprintf("%d schema objects discovered", len(objs)), map[string]any{"count": len(objs)}, e
		})
	}
	if *cdc {
		if src, ok := base.(connector.CDCSource); ok {
			r.run("cdc-current-lsn", func() (string, map[string]any, error) {
				pos, e := src.CurrentCDCPosition(ctx)
				if e != nil {
					return "", nil, e
				}
				details := map[string]any{"position": pos}
				if concrete, ok := base.(*sqlserverconnector.Connector); ok {
					if retention, re := concrete.CDCRetentionMinutes(ctx); re == nil {
						details["retention_minutes"] = retention
					}
				}
				return pos.PositionValue, details, nil
			})
		} else {
			r.skip("cdc-current-lsn", "cdc-position capability unavailable")
		}
	} else {
		r.skip("cdc-current-lsn", "enable with --cdc")
	}
	if *targetWrite {
		runTargetWrite(r, base, selectedSchema)
	} else {
		r.skip("target-write-transaction", "destructive target test disabled; enable with --target-write")
	}
	finish(rep, *outFile)
	if rep.Failed > 0 {
		os.Exit(1)
	}
}

func runTargetWrite(r *runner, base connector.Connector, schema string) {
	sc, ok1 := base.(connector.CompositeSchemaConnector)
	dc, ok2 := base.(connector.DataConnector)
	pc, ok3 := base.(connector.PointLookupConnector)
	tx, ok4 := base.(connector.TransactionalCDCApplyConnector)
	ddl, ok5 := base.(connector.DDLApplyConnector)
	if !(ok1 && ok2 && ok3 && ok4 && ok5) {
		r.skip("target-write-transaction", "required SQL Server target SPI capabilities are incomplete")
		return
	}
	table := fmt.Sprintf("QMQUAL_%08X", uint32(time.Now().UnixNano()))
	cols := []domain.ColumnInfo{{Name: "ID", DataType: "bigint", ColumnType: "bigint", Nullable: false, PrimaryKey: true, Extra: "IDENTITY(100,5)", Ordinal: 1}, {Name: "TXT", DataType: "nvarchar", ColumnType: "nvarchar(200)", Nullable: true, Ordinal: 2}, {Name: "NVAL", DataType: "decimal", ColumnType: "decimal(38,10)", Nullable: true, Ordinal: 3}, {Name: "NOTE", DataType: "text", ColumnType: "text", Nullable: true, Ordinal: 4}, {Name: "BIN", DataType: "varbinary", ColumnType: "varbinary(max)", Nullable: true, Ordinal: 5}}
	r.run("target-write-transaction", func() (string, map[string]any, error) {
		if err := sc.CreateTableWithPrimaryKeys(r.ctx, schema, table, cols, []string{"ID"}); err != nil {
			return "create qualification table", nil, err
		}
		defer func() {
			_ = ddl.ExecDDL(context.Background(), schema, "DROP TABLE "+"["+strings.ReplaceAll(schema, "]", "]]")+"]."+"["+strings.ReplaceAll(table, "]", "]]")+"]")
		}()
		largeText := strings.Repeat("QMigration-SQLServer-LOB-你", 2500)
		blob := make([]byte, 48<<10)
		for i := range blob {
			blob[i] = byte(i % 251)
		}
		rows := [][]connector.Value{{{Raw: []byte("100")}, {Raw: []byte("alpha")}, {Raw: []byte("12345678901234567890.1234567890")}, {Raw: []byte(largeText)}, {Raw: blob}}, {{Raw: []byte("105")}, {Raw: []byte("beta")}, {Raw: []byte("-0.0000000001")}, {Null: true}, {Null: true}}}
		if n, e := dc.WriteBatch(r.ctx, connector.WriteBatchRequest{Schema: schema, Table: table, Columns: cols, Rows: rows, PrimaryKeys: []string{"ID"}}); e != nil || n != 2 {
			if e == nil {
				e = fmt.Errorf("expected 2 rows, got %d", n)
			}
			return "full writer", nil, e
		}
		lookup := func(id string) ([]connector.Value, bool, error) {
			return pc.ReadByKey(r.ctx, connector.ReadByKeyRequest{Schema: schema, Table: table, PrimaryKeys: []string{"ID"}, KeyColumns: cols[:1], KeyValues: []connector.Value{{Raw: []byte(id)}}, Columns: cols})
		}
		row, found, e := lookup("100")
		if e != nil || !found {
			if e == nil {
				e = errors.New("row 1 not found")
			}
			return "point lookup", nil, e
		}
		if len(row) < 5 || row[3].Null || string(row[3].Raw) != largeText || row[4].Null || len(row[4].Raw) != len(blob) {
			return "large value round-trip", map[string]any{"text_bytes": len(row[3].Raw), "binary_bytes": len(row[4].Raw)}, errors.New("large nvarchar/varbinary mismatch")
		}
		if e := tx.BeginCDCTransaction(r.ctx); e != nil {
			return "begin transaction", nil, e
		}
		if _, e = tx.WriteBatch(r.ctx, connector.WriteBatchRequest{Schema: schema, Table: table, Columns: cols, Rows: [][]connector.Value{{{Raw: []byte("100")}, {Raw: []byte("rollback")}, {Raw: []byte("7")}, {Null: true}, {Null: true}}}, PrimaryKeys: []string{"ID"}}); e != nil {
			_ = tx.RollbackCDCTransaction(context.Background())
			return "transactional write", nil, e
		}
		if e = tx.RollbackCDCTransaction(r.ctx); e != nil {
			return "rollback", nil, e
		}
		row, found, e = lookup("100")
		if e != nil || !found || string(row[1].Raw) != "alpha" {
			if e == nil {
				e = errors.New("rollback not atomic")
			}
			return "rollback verification", nil, e
		}
		if e = tx.BeginCDCTransaction(r.ctx); e != nil {
			return "begin commit", nil, e
		}
		if _, e = tx.WriteBatch(r.ctx, connector.WriteBatchRequest{Schema: schema, Table: table, Columns: cols, Rows: [][]connector.Value{{{Raw: []byte("100")}, {Raw: []byte("commit")}, {Raw: []byte("8")}, {Null: true}, {Null: true}}}, PrimaryKeys: []string{"ID"}}); e != nil {
			_ = tx.RollbackCDCTransaction(context.Background())
			return "commit write", nil, e
		}
		if e = tx.CommitCDCTransaction(r.ctx); e != nil {
			return "commit", nil, e
		}
		row, found, e = lookup("100")
		if e != nil || !found || string(row[1].Raw) != "commit" {
			if e == nil {
				e = errors.New("commit not visible")
			}
			return "commit verification", nil, e
		}
		if e = tx.DeleteByKey(r.ctx, connector.DeleteByKeyRequest{Schema: schema, Table: table, PrimaryKeys: []string{"ID"}, Columns: cols[:1], Values: []connector.Value{{Raw: []byte("105")}}}); e != nil {
			return "delete", nil, e
		}
		if pli, ok := base.(connector.PostLoadSchemaConnector); ok {
			if e = pli.CreateIndex(r.ctx, schema, table, domain.IndexInfo{Name: "QMQUAL_I1", Columns: []string{"TXT"}}); e != nil {
				return "post-load index", nil, e
			}
		}
		return "full write, decimal safety, large Unicode/binary values, rollback/commit, delete and index passed", map[string]any{"table": schema + "." + table, "text_bytes": len(largeText), "binary_bytes": len(blob)}, nil
	})
}

func finish(rep report, out string) {
	rep.Qualified = rep.Failed == 0
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if strings.TrimSpace(out) != "" {
		if err := os.WriteFile(out, append(b, '\n'), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, "write report:", err)
		}
	}
}
func fatalIf(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
