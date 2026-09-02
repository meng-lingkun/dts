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

	"qmigration/backend/internal/cdc/obbinlog"
	"qmigration/backend/internal/connector"
	mysqlconnector "qmigration/backend/internal/connector/mysql"
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
	started := time.Now()
	msg, details, err := fn()
	item := check{Name: name, DurationMS: time.Since(started).Milliseconds(), Message: msg, Details: details}
	if err != nil {
		item.Status = fail
		if msg == "" {
			item.Message = err.Error()
		} else {
			item.Message += ": " + err.Error()
		}
		r.rep.Failed++
	} else {
		item.Status = pass
		r.rep.Passed++
	}
	r.rep.Checks = append(r.rep.Checks, item)
	return err == nil
}

func (r *runner) skip(name, reason string) {
	r.rep.Checks = append(r.rep.Checks, check{Name: name, Status: skip, Message: reason})
	r.rep.Skipped++
}

func main() {
	var (
		host        = flag.String("host", "", "OceanBase MySQL SQL/ODP host used for full load")
		port        = flag.Int("port", 2883, "OceanBase MySQL SQL/ODP port")
		database    = flag.String("database", "", "OceanBase database/schema")
		user        = flag.String("user", "", "OceanBase tenant username, for example root@tenant")
		passwordEnv = flag.String("password-env", "OCEANBASE_PASSWORD", "environment variable containing OceanBase password")
		cdcURL      = flag.String("cdc-url", "", "Binlog subscription endpoint, e.g. obbinlog://odp-host:2883")
		schema      = flag.String("schema", "", "schema/database to inspect; defaults to --database")
		table       = flag.String("table", "", "optional table for a full-read sample")
		sampleRows  = flag.Int("sample-rows", 16, "maximum full-read sample rows")
		timeout     = flag.Duration("timeout", 60*time.Second, "overall qualification timeout")
		tlsMode     = flag.String("tls-mode", "DISABLE", "DISABLE, PREFERRED, or REQUIRED for the SQL endpoint")
		outFile     = flag.String("output", "", "optional JSON report output file")
	)
	flag.Parse()
	if strings.TrimSpace(*host) == "" || strings.TrimSpace(*database) == "" || strings.TrimSpace(*user) == "" || strings.TrimSpace(*cdcURL) == "" {
		fmt.Fprintln(os.Stderr, "--host, --database, --user and --cdc-url are required")
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
	ep, err := obbinlog.ParseEndpoint(*cdcURL)
	fatal(err)
	ds := domain.DataSource{
		Type: domain.DataSourceOceanBase, Host: strings.TrimSpace(*host), Port: *port,
		Username: strings.TrimSpace(*user), Password: password, Database: strings.TrimSpace(*database),
		Schema: strings.TrimSpace(*schema), CDCURL: strings.TrimSpace(*cdcURL),
		TLSMode: domain.TLSMode(strings.ToUpper(strings.TrimSpace(*tlsMode))),
	}
	factory := mysqlconnector.NewFactory()
	base, err := factory.New(ds)
	fatal(err)
	defer base.Close()
	rep := report{
		ToolVersion: toolVersion, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Target:     map[string]any{"sql_host": ds.Host, "sql_port": ds.Port, "database": ds.Database, "user": ds.Username, "sql_tls_mode": ds.TLSMode, "binlog_subscription_endpoint": ep.URL, "binlog_tls": ep.TLS},
		Descriptor: factory.Capabilities(domain.DataSourceOceanBase),
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	r := &runner{ctx: ctx, rep: &rep}
	if !r.run("oceanbase-sql-connection", func() (string, map[string]any, error) {
		return "OceanBase MySQL-wire authentication succeeded", nil, base.TestConnection(ctx)
	}) {
		finish(rep, *outFile)
		os.Exit(1)
	}
	r.run("oceanbase-version", func() (string, map[string]any, error) {
		v, e := base.GetVersion(ctx)
		if e == nil {
			rep.ServerVersion = v
		}
		return v, nil, e
	})
	selectedSchema := strings.TrimSpace(*schema)
	if selectedSchema == "" {
		selectedSchema = ds.Database
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
	if selectedTable != "" {
		meta, e := base.GetTableMetadata(ctx, selectedSchema, selectedTable)
		if e != nil {
			r.run("table-metadata", func() (string, map[string]any, error) { return "", nil, e })
		} else {
			r.run("table-metadata", func() (string, map[string]any, error) {
				return fmt.Sprintf("%s.%s columns=%d", selectedSchema, selectedTable, len(meta.Columns)), map[string]any{"columns": len(meta.Columns), "primary_keys": meta.PrimaryKeys}, nil
			})
			if dc, ok := base.(connector.DataConnector); ok {
				r.run("full-read-sample", func() (string, map[string]any, error) {
					req := connector.ReadBatchRequest{Schema: selectedSchema, Table: selectedTable, Columns: meta.Columns, Limit: *sampleRows, PrimaryKey: meta.PrimaryKey, PrimaryKeys: meta.PrimaryKeys}
					if len(meta.PrimaryKeys) > 0 {
						req.UseKeyset = true
					}
					batch, e := dc.ReadBatch(ctx, req)
					if e != nil {
						return "", nil, e
					}
					return fmt.Sprintf("sampled %d rows (%d bytes)", len(batch.Rows), batch.Bytes), map[string]any{"rows": len(batch.Rows), "bytes": batch.Bytes}, nil
				})
			}
		}
	} else {
		r.skip("table-metadata", "no table supplied or visible")
		r.skip("full-read-sample", "no table supplied or visible")
	}
	if pc, ok := base.(connector.MigrationPrecheckConnector); ok {
		r.run("oceanbase-binlog-prechecks", func() (string, map[string]any, error) {
			items := pc.MigrationPrechecks(ctx, true)
			var failed []string
			for _, item := range items {
				if item.Level == domain.PrecheckFailed {
					failed = append(failed, item.Name+": "+item.Message)
				}
			}
			if len(failed) > 0 {
				return "", map[string]any{"items": items}, errors.New(strings.Join(failed, "; "))
			}
			return "OceanBase SQL + ODP/Binlog Service prechecks passed", map[string]any{"items": items}, nil
		})
	}
	if src, ok := base.(connector.CDCSource); ok {
		r.run("oceanbase-binlog-position", func() (string, map[string]any, error) {
			pos, e := src.CurrentCDCPosition(ctx)
			if e != nil {
				return "", nil, e
			}
			return pos.PositionType + " " + pos.PositionValue, map[string]any{"position": pos}, nil
		})
	} else {
		r.run("oceanbase-binlog-position", func() (string, map[string]any, error) {
			return "", nil, errors.New("OceanBase CDC source capability unavailable")
		})
	}
	finish(rep, *outFile)
	if rep.Failed > 0 {
		os.Exit(1)
	}
}

func finish(rep report, outFile string) {
	rep.Qualified = rep.Failed == 0
	data, err := json.MarshalIndent(rep, "", "  ")
	fatal(err)
	fmt.Println(string(data))
	if strings.TrimSpace(outFile) != "" {
		fatal(os.WriteFile(strings.TrimSpace(outFile), append(data, '\n'), 0600))
	}
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
