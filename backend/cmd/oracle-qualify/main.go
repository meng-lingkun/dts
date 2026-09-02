package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"qmigration/backend/internal/connector"
	oracleconnector "qmigration/backend/internal/connector/oracle"
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

type qualificationReport struct {
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
	report *qualificationReport
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
		host          = flag.String("host", "", "Oracle host or SCAN address")
		port          = flag.Int("port", 1521, "Oracle listener port")
		service       = flag.String("service", "", "Oracle service name")
		user          = flag.String("user", "", "Oracle username")
		passwordEnv   = flag.String("password-env", "ORACLE_PASSWORD", "environment variable containing Oracle password")
		schema        = flag.String("schema", "", "schema to inspect; defaults to username")
		table         = flag.String("table", "", "optional table to sample/read")
		sampleRows    = flag.Int("sample-rows", 16, "maximum sample rows")
		cdc           = flag.Bool("cdc", false, "qualify LogMiner prerequisites and current SCN")
		targetWrite   = flag.Bool("target-write", false, "run destructive target write qualification using a temporary table")
		timeout       = flag.Duration("timeout", 90*time.Second, "overall qualification timeout")
		tlsMode       = flag.String("tls-mode", "DISABLE", "DISABLE, PREFERRED, or REQUIRED")
		tlsServerName = flag.String("tls-server-name", "", "TCPS certificate server name")
		caFile        = flag.String("tls-ca-file", "", "CA PEM file")
		certFile      = flag.String("tls-cert-file", "", "client certificate PEM file")
		keyFile       = flag.String("tls-key-file", "", "client private key PEM file")
		outFile       = flag.String("output", "", "optional JSON report output file")
	)
	flag.Parse()

	if strings.TrimSpace(*host) == "" || strings.TrimSpace(*service) == "" || strings.TrimSpace(*user) == "" {
		fmt.Fprintln(os.Stderr, "--host, --service and --user are required")
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

	// Qualification intentionally enables only this process's experimental gates.
	_ = os.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE", "1")
	if *cdc {
		_ = os.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC", "1")
	}
	if *targetWrite {
		_ = os.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_TARGET", "1")
	}

	ds := domain.DataSource{
		Type: domain.DataSourceOracle, Host: strings.TrimSpace(*host), Port: *port,
		Username: strings.TrimSpace(*user), Password: password, Database: strings.TrimSpace(*service),
		TLSMode: domain.TLSMode(strings.ToUpper(strings.TrimSpace(*tlsMode))), TLSServerName: strings.TrimSpace(*tlsServerName),
		TLSCACert: ca, TLSClientCert: cert, TLSClientKey: key,
	}
	factory := oracleconnector.NewFactory()
	base, err := factory.New(ds)
	fatalIf(err)
	defer base.Close()

	report := qualificationReport{
		ToolVersion: toolVersion,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Target:      map[string]any{"host": ds.Host, "port": ds.Port, "service": ds.Database, "user": ds.Username, "tls_mode": ds.TLSMode},
		Descriptor:  factory.Capabilities(domain.DataSourceOracle),
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	r := &runner{ctx: ctx, conn: base, report: &report}

	connected := r.run("connection", func() (string, map[string]any, error) {
		return "Oracle Net/TTC authentication succeeded", nil, base.TestConnection(ctx)
	})
	if !connected {
		finish(report, *outFile)
		os.Exit(1)
	}

	r.run("version", func() (string, map[string]any, error) {
		v, e := base.GetVersion(ctx)
		if e == nil {
			report.ServerVersion = v
		}
		return v, nil, e
	})

	if pc, ok := base.(connector.MigrationPrecheckConnector); ok {
		r.run("migration-prechecks", func() (string, map[string]any, error) {
			items := pc.MigrationPrechecks(ctx, *cdc)
			failed := []string{}
			warn := []string{}
			for _, item := range items {
				if item.Level == domain.PrecheckFailed {
					failed = append(failed, item.Name+": "+item.Message)
				} else if item.Level == domain.PrecheckWarning {
					warn = append(warn, item.Name+": "+item.Message)
				}
			}
			details := map[string]any{"items": items}
			if len(failed) > 0 {
				return strings.Join(warn, "; "), details, errors.New(strings.Join(failed, "; "))
			}
			return strings.Join(warn, "; "), details, nil
		})
	} else {
		r.skip("migration-prechecks", "connector does not expose migration-precheck")
	}

	var schemas []domain.SchemaInfo
	r.run("metadata-schemas", func() (string, map[string]any, error) {
		var e error
		schemas, e = base.ListSchemas(ctx)
		return fmt.Sprintf("%d schemas visible", len(schemas)), map[string]any{"count": len(schemas)}, e
	})

	selectedSchema := strings.ToUpper(strings.TrimSpace(*schema))
	if selectedSchema == "" {
		selectedSchema = strings.ToUpper(ds.Username)
	}
	var tables []domain.TableInfo
	r.run("metadata-tables", func() (string, map[string]any, error) {
		var e error
		tables, e = base.ListTables(ctx, selectedSchema)
		return fmt.Sprintf("%d tables visible in %s", len(tables), selectedSchema), map[string]any{"schema": selectedSchema, "count": len(tables)}, e
	})

	selectedTable := strings.ToUpper(strings.TrimSpace(*table))
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
			return fmt.Sprintf("%s.%s columns=%d estimated_rows=%d", selectedSchema, selectedTable, len(meta.Columns), meta.EstimatedRows), map[string]any{"primary_keys": meta.PrimaryKeys, "columns": len(meta.Columns), "estimated_rows": meta.EstimatedRows}, nil
		})
		if dc, ok := base.(connector.DataConnector); ok && meta != nil {
			r.run("full-read-sample", func() (string, map[string]any, error) {
				batch, e := dc.ReadBatch(ctx, connector.ReadBatchRequest{Schema: selectedSchema, Table: selectedTable, Columns: meta.Columns, Limit: *sampleRows})
				if e != nil {
					return "", nil, e
				}
				return fmt.Sprintf("sampled %d rows (%d bytes)", len(batch.Rows), batch.Bytes), map[string]any{"rows": len(batch.Rows), "bytes": batch.Bytes}, nil
			})
		} else {
			r.skip("full-read-sample", "full-read capability unavailable")
		}
		if pc, ok := base.(connector.PartitionConnector); ok {
			r.run("partition-discovery", func() (string, map[string]any, error) {
				parts, e := pc.ListTablePartitions(ctx, selectedSchema, selectedTable)
				return fmt.Sprintf("%d partitions", len(parts)), map[string]any{"partitions": parts}, e
			})
		} else {
			r.skip("partition-discovery", "partition capability unavailable")
		}
	}

	if lc, ok := base.(connector.RuntimeLoadConnector); ok {
		r.run("runtime-load", func() (string, map[string]any, error) {
			load, e := lc.SampleRuntimeLoad(ctx)
			return "runtime pressure sample collected", map[string]any{"load": load}, e
		})
	} else {
		r.skip("runtime-load", "runtime-load capability unavailable")
	}

	if sc, ok := base.(connector.SchemaObjectConnector); ok {
		r.run("schema-objects", func() (string, map[string]any, error) {
			objs, e := sc.ListSchemaObjects(ctx, selectedSchema)
			return fmt.Sprintf("%d schema objects discovered", len(objs)), map[string]any{"count": len(objs)}, e
		})
	} else {
		r.skip("schema-objects", "schema-object capability unavailable")
	}

	if *cdc {
		if src, ok := base.(connector.CDCSource); ok {
			r.run("cdc-current-scn", func() (string, map[string]any, error) {
				pos, e := src.CurrentCDCPosition(ctx)
				if e != nil {
					return "", nil, e
				}
				return pos.PositionValue, map[string]any{"position": pos}, nil
			})
		} else {
			r.skip("cdc-current-scn", "cdc-position capability unavailable")
		}
	} else {
		r.skip("cdc-current-scn", "enable with --cdc")
	}

	if *targetWrite {
		runTargetWriteQualification(r, base, selectedSchema)
	} else {
		r.skip("target-write-bind-lob-transaction", "destructive target test disabled; enable with --target-write")
	}

	report.Qualified = report.Failed == 0
	finish(report, *outFile)
	if !report.Qualified {
		os.Exit(1)
	}
}

func runTargetWriteQualification(r *runner, base connector.Connector, schema string) {
	sc, ok1 := base.(connector.CompositeSchemaConnector)
	dc, ok2 := base.(connector.DataConnector)
	pc, ok3 := base.(connector.PointLookupConnector)
	tx, ok4 := base.(connector.TransactionalCDCApplyConnector)
	ddl, ok5 := base.(connector.DDLApplyConnector)
	if !(ok1 && ok2 && ok3 && ok4 && ok5) {
		r.skip("target-write-bind-lob-transaction", "required Oracle target SPI capabilities are incomplete")
		return
	}
	table := fmt.Sprintf("QMQUAL_%08X", uint32(time.Now().UnixNano()))
	cols := []domain.ColumnInfo{
		{Name: "ID", DataType: "number", ColumnType: "NUMBER(19)", Nullable: false, PrimaryKey: true, Ordinal: 1},
		{Name: "TXT", DataType: "varchar2", ColumnType: "VARCHAR2(200)", Nullable: true, Ordinal: 2},
		{Name: "NVAL", DataType: "number", ColumnType: "NUMBER(38,10)", Nullable: true, Ordinal: 3},
		{Name: "CLOB_DATA", DataType: "clob", ColumnType: "CLOB", Nullable: true, Ordinal: 4},
		{Name: "BLOB_DATA", DataType: "blob", ColumnType: "BLOB", Nullable: true, Ordinal: 5},
	}
	cleanup := func() {
		_ = ddl.ExecDDL(context.Background(), schema, `DROP TABLE "`+strings.ReplaceAll(table, `"`, `""`)+`" PURGE`)
	}

	r.run("target-write-bind-lob-transaction", func() (string, map[string]any, error) {
		if err := sc.CreateTableWithPrimaryKeys(r.ctx, schema, table, cols, []string{"ID"}); err != nil {
			return "create qualification table", map[string]any{"table": schema + "." + table}, err
		}
		defer cleanup()
		largeText := strings.Repeat("QMigration-Oracle-LOB-你", 1800)
		largeBlob := make([]byte, 48<<10)
		for i := range largeBlob {
			largeBlob[i] = byte(i % 251)
		}
		rows := [][]connector.Value{
			{{Raw: []byte("1")}, {Raw: []byte("alpha")}, {Raw: []byte("12345678901234567890.1234567890")}, {Raw: []byte(largeText)}, {Raw: largeBlob}},
			{{Raw: []byte("2")}, {Raw: []byte("beta")}, {Raw: []byte("-0.0000000001")}, {Null: true}, {Null: true}},
		}
		if n, err := dc.WriteBatch(r.ctx, connector.WriteBatchRequest{Schema: schema, Table: table, Columns: cols, Rows: rows, PrimaryKeys: []string{"ID"}}); err != nil || n != 2 {
			if err == nil {
				err = fmt.Errorf("expected 2 written rows, got %d", n)
			}
			return "array-bind/full-write", map[string]any{"table": schema + "." + table}, err
		}
		lookup := func(id string) ([]connector.Value, bool, error) {
			return pc.ReadByKey(r.ctx, connector.ReadByKeyRequest{Schema: schema, Table: table, PrimaryKeys: []string{"ID"}, KeyColumns: cols[:1], KeyValues: []connector.Value{{Raw: []byte(id)}}, Columns: cols})
		}
		row1, found, err := lookup("1")
		if err != nil || !found || len(row1) != len(cols) {
			if err == nil {
				err = errors.New("row 1 not found after write")
			}
			return "point lookup after write", nil, err
		}
		if row1[2].Null || string(row1[2].Raw) != "12345678901234567890.123456789" && string(row1[2].Raw) != "12345678901234567890.1234567890" {
			return "exact NUMBER round-trip", map[string]any{"actual": string(row1[2].Raw)}, errors.New("NUMBER precision mismatch")
		}
		if row1[3].Null || string(row1[3].Raw) != largeText || row1[4].Null || len(row1[4].Raw) != len(largeBlob) {
			return "large LOB round-trip", map[string]any{"clob_bytes": len(row1[3].Raw), "blob_bytes": len(row1[4].Raw)}, errors.New("large CLOB/BLOB round-trip mismatch")
		}
		if err := tx.BeginCDCTransaction(r.ctx); err != nil {
			return "begin CDC transaction", nil, err
		}
		updated := [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("rollback-value")}, {Raw: []byte("7")}, {Null: true}, {Null: true}}}
		if _, err := tx.WriteBatch(r.ctx, connector.WriteBatchRequest{Schema: schema, Table: table, Columns: cols, Rows: updated, PrimaryKeys: []string{"ID"}}); err != nil {
			_ = tx.RollbackCDCTransaction(context.Background())
			return "transactional update", nil, err
		}
		if err := tx.RollbackCDCTransaction(r.ctx); err != nil {
			return "rollback CDC transaction", nil, err
		}
		afterRollback, found, err := lookup("1")
		if err != nil || !found || string(afterRollback[1].Raw) != "alpha" {
			if err == nil {
				err = errors.New("rollback was not atomic")
			}
			return "rollback verification", nil, err
		}
		if err := tx.BeginCDCTransaction(r.ctx); err != nil {
			return "begin commit transaction", nil, err
		}
		committed := [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("commit-value")}, {Raw: []byte("8")}, {Null: true}, {Null: true}}}
		if _, err := tx.WriteBatch(r.ctx, connector.WriteBatchRequest{Schema: schema, Table: table, Columns: cols, Rows: committed, PrimaryKeys: []string{"ID"}}); err != nil {
			_ = tx.RollbackCDCTransaction(context.Background())
			return "transactional commit update", nil, err
		}
		if err := tx.CommitCDCTransaction(r.ctx); err != nil {
			return "commit CDC transaction", nil, err
		}
		afterCommit, found, err := lookup("1")
		if err != nil || !found || string(afterCommit[1].Raw) != "commit-value" {
			if err == nil {
				err = errors.New("commit was not visible")
			}
			return "commit verification", nil, err
		}
		if err := tx.DeleteByKey(r.ctx, connector.DeleteByKeyRequest{Schema: schema, Table: table, PrimaryKeys: []string{"ID"}, Columns: cols[:1], Values: []connector.Value{{Raw: []byte("2")}}}); err != nil {
			return "bound delete", nil, err
		}
		_, found, err = lookup("2")
		if err != nil || found {
			if err == nil {
				err = errors.New("deleted row still exists")
			}
			return "delete verification", nil, err
		}
		if pli, ok := base.(connector.PostLoadSchemaConnector); ok {
			if err := pli.CreateIndex(r.ctx, schema, table, domain.IndexInfo{Name: "QMQUAL_I1", Columns: []string{"TXT"}}); err != nil {
				return "post-load index", nil, err
			}
		}
		return "bind, array-bind, prepared DML, exact NUMBER, large CLOB/BLOB, transaction rollback/commit and delete passed", map[string]any{"table": schema + "." + table, "clob_bytes": len(largeText), "blob_bytes": len(largeBlob)}, nil
	})
}

func finish(report qualificationReport, outFile string) {
	report.Qualified = report.Failed == 0
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	if strings.TrimSpace(outFile) != "" {
		if err := os.WriteFile(outFile, append(b, '\n'), 0o600); err != nil {
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

func sortedCaps(in []connector.Capability) []string {
	out := make([]string, len(in))
	for i, c := range in {
		out[i] = string(c)
	}
	sort.Strings(out)
	return out
}
