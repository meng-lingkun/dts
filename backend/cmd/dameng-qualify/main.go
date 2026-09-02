package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"qmigration/backend/internal/connector"
	damengconnector "qmigration/backend/internal/connector/dameng"
	"qmigration/backend/internal/domain"
)

const toolVersion = "0.15.0-rc49"

type check struct {
	Name       string         `json:"name"`
	Status     string         `json:"status"`
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
	start := time.Now()
	msg, details, err := fn()
	c := check{Name: name, DurationMS: time.Since(start).Milliseconds(), Message: msg, Details: details}
	if err != nil {
		c.Status = "FAIL"
		if c.Message == "" {
			c.Message = err.Error()
		} else {
			c.Message += ": " + err.Error()
		}
		r.rep.Failed++
	} else {
		c.Status = "PASS"
		r.rep.Passed++
	}
	r.rep.Checks = append(r.rep.Checks, c)
	return err == nil
}
func (r *runner) skip(name, reason string) {
	r.rep.Checks = append(r.rep.Checks, check{Name: name, Status: "SKIP", Message: reason})
	r.rep.Skipped++
}
func finish(rep report, output string) {
	rep.Qualified = rep.Failed == 0
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if strings.TrimSpace(output) != "" {
		_ = os.WriteFile(output, append(b, '\n'), 0600)
	}
}
func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func main() {
	var (
		host        = flag.String("host", "", "Dameng host")
		port        = flag.Int("port", 5236, "Dameng SQL port")
		user        = flag.String("user", "", "Dameng username")
		passwordEnv = flag.String("password-env", "DAMENG_PASSWORD", "environment variable containing password")
		schema      = flag.String("schema", "", "schema to inspect; defaults to username")
		table       = flag.String("table", "", "optional table for metadata/full-read qualification")
		sampleRows  = flag.Int("sample-rows", 16, "maximum sample rows")
		targetWrite = flag.Bool("target-write", false, "create/write/read/delete/drop a temporary table in the selected schema")
		cdc         = flag.Bool("cdc", false, "qualify experimental DM8 archived-log DBMS_LOGMNR CDC + exact DM_LSN flashback snapshot")
		timeout     = flag.Duration("timeout", 90*time.Second, "overall timeout")
		output      = flag.String("output", "", "optional JSON report path")
	)
	flag.Parse()
	if strings.TrimSpace(*host) == "" || strings.TrimSpace(*user) == "" {
		fmt.Fprintln(os.Stderr, "--host and --user are required")
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
	if strings.TrimSpace(*schema) == "" {
		*schema = strings.ToUpper(strings.TrimSpace(*user))
	}
	_ = os.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE", "1")
	if *cdc {
		_ = os.Setenv("QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC", "1")
	}
	ds := domain.DataSource{Type: domain.DataSourceDameng, Host: strings.TrimSpace(*host), Port: *port, Username: strings.TrimSpace(*user), Password: password, Schema: strings.TrimSpace(*schema)}
	f := damengconnector.NewFactory()
	base, err := f.New(ds)
	fatal(err)
	defer base.Close()
	driverName := strings.TrimSpace(os.Getenv("QMIGRATION_DAMENG_SQL_DRIVER"))
	if driverName == "" {
		driverName = "dm"
	}
	rep := report{ToolVersion: toolVersion, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Target: map[string]any{"host": ds.Host, "port": ds.Port, "user": ds.Username, "schema": ds.Schema, "driver": driverName, "provider_plugin_configured": strings.TrimSpace(os.Getenv("QMIGRATION_DAMENG_DRIVER_PLUGIN")) != ""}, Descriptor: f.Capabilities(domain.DataSourceDameng)}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	r := &runner{ctx: ctx, rep: &rep}
	if !r.run("connection", func() (string, map[string]any, error) {
		return "DM database/sql authentication and SELECT 1", nil, base.TestConnection(ctx)
	}) {
		finish(rep, *output)
		os.Exit(1)
	}
	r.run("version", func() (string, map[string]any, error) {
		v, e := base.GetVersion(ctx)
		if e == nil {
			rep.ServerVersion = v
		}
		return v, nil, e
	})
	r.run("schemas", func() (string, map[string]any, error) {
		xs, e := base.ListSchemas(ctx)
		return fmt.Sprintf("%d visible schemas", len(xs)), map[string]any{"count": len(xs)}, e
	})
	if strings.TrimSpace(*table) == "" {
		r.skip("table-metadata", "--table not supplied")
		r.skip("full-read", "--table not supplied")
	} else {
		var md *domain.TableMetadata
		ok := r.run("table-metadata", func() (string, map[string]any, error) {
			var e error
			md, e = base.GetTableMetadata(ctx, ds.Schema, strings.TrimSpace(*table))
			if e != nil {
				return "", nil, e
			}
			return fmt.Sprintf("columns=%d pk=%v indexes=%d fks=%d", len(md.Columns), md.PrimaryKeys, len(md.Indexes), len(md.ForeignKeys)), map[string]any{"columns": len(md.Columns), "primary_keys": md.PrimaryKeys, "indexes": len(md.Indexes), "foreign_keys": len(md.ForeignKeys)}, nil
		})
		if !ok || md == nil || len(md.PrimaryKeys) == 0 {
			r.skip("full-read", "metadata failed or table has no migration key")
		} else if dc, ok := base.(connector.DataConnector); ok {
			r.run("full-read", func() (string, map[string]any, error) {
				b, e := dc.ReadBatch(ctx, connector.ReadBatchRequest{Schema: ds.Schema, Table: strings.TrimSpace(*table), PrimaryKey: md.PrimaryKey, PrimaryKeys: md.PrimaryKeys, Columns: md.Columns, UseKeyset: true, Limit: *sampleRows})
				if e != nil {
					return "", nil, e
				}
				return fmt.Sprintf("sample rows=%d bytes=%d", len(b.Rows), b.Bytes), map[string]any{"rows": len(b.Rows), "bytes": b.Bytes}, nil
			})
		}
	}
	if !*cdc {
		r.skip("cdc-precheck", "--cdc not requested")
		r.skip("cdc-position", "--cdc not requested")
		r.skip("cdc-validation-snapshot", "--cdc not requested")
	} else {
		c, ok := base.(*damengconnector.Connector)
		if !ok {
			r.skip("cdc-precheck", "unexpected connector type")
			r.skip("cdc-position", "unexpected connector type")
			r.skip("cdc-validation-snapshot", "unexpected connector type")
		} else {
			r.run("cdc-precheck", func() (string, map[string]any, error) {
				items := c.MigrationPrechecks(ctx, true)
				failed := []string{}
				for _, item := range items {
					if item.Level == domain.PrecheckFailed {
						failed = append(failed, item.Name+": "+item.Message)
					}
				}
				if len(failed) > 0 {
					return "", map[string]any{"items": items}, fmt.Errorf("%s", strings.Join(failed, "; "))
				}
				return "ARCH_INI/RLOG_APPEND_LOGIC/flashback/local archive prerequisites passed", map[string]any{"items": items}, nil
			})
			var pos *domain.CDCPosition
			positionOK := r.run("cdc-position", func() (string, map[string]any, error) {
				var e error
				pos, e = c.CurrentCDCPosition(ctx)
				if e != nil {
					return "", nil, e
				}
				return "captured newest fully archived DM_LSN", map[string]any{"position_type": pos.PositionType, "position_value": pos.PositionValue, "resource": pos.Resource}, nil
			})
			if strings.TrimSpace(*table) == "" || !positionOK || pos == nil {
				r.skip("cdc-validation-snapshot", "--table and a valid archived DM_LSN are required")
			} else {
				r.run("cdc-validation-snapshot", func() (string, map[string]any, error) {
					if err := c.ValidateCDCSelection(ctx, []domain.TableMapping{{SourceSchema: ds.Schema, SourceTable: strings.TrimSpace(*table), TargetSchema: ds.Schema, TargetTable: strings.TrimSpace(*table)}}); err != nil {
						return "", nil, err
					}
					snapRaw, err := c.OpenValidationSnapshot(ctx, *pos)
					if err != nil {
						return "", nil, err
					}
					defer snapRaw.Close()
					md, err := snapRaw.GetTableMetadata(ctx, ds.Schema, strings.TrimSpace(*table))
					if err != nil {
						return "", nil, err
					}
					dc, ok := snapRaw.(connector.DataConnector)
					if !ok {
						return "", nil, fmt.Errorf("validation snapshot does not expose data connector")
					}
					b, err := dc.ReadBatch(ctx, connector.ReadBatchRequest{Schema: ds.Schema, Table: strings.TrimSpace(*table), PrimaryKey: md.PrimaryKey, PrimaryKeys: md.PrimaryKeys, Columns: md.Columns, UseKeyset: true, Limit: *sampleRows})
					if err != nil {
						return "", nil, err
					}
					return fmt.Sprintf("AS OF SCN %s sample rows=%d", pos.PositionValue, len(b.Rows)), map[string]any{"lsn": pos.PositionValue, "rows": len(b.Rows)}, nil
				})
			}
		}
	}
	if !*targetWrite {
		r.skip("target-write", "--target-write not requested")
	} else {
		c, ok := base.(*damengconnector.Connector)
		if !ok {
			r.skip("target-write", "unexpected connector type")
		} else {
			r.run("target-write", func() (string, map[string]any, error) {
				name := fmt.Sprintf("QMIGRATION_Q_%d", time.Now().UnixNano())
				cols := []domain.ColumnInfo{{Name: "ID", DataType: "bigint", Nullable: false}, {Name: "TXT", DataType: "varchar", ColumnType: "VARCHAR(200)", Nullable: true}, {Name: "AMOUNT", DataType: "decimal", ColumnType: "DECIMAL(20,4)", Nullable: true}, {Name: "PAYLOAD", DataType: "blob", Nullable: true}}
				if err := c.CreateTableWithPrimaryKeys(ctx, ds.Schema, name, cols, []string{"ID"}); err != nil {
					return "", nil, err
				}
				defer func() {
					qualified := `"` + strings.ReplaceAll(ds.Schema, `"`, `""`) + `"."` + strings.ReplaceAll(name, `"`, `""`) + `"`
					_ = c.ExecDDL(context.Background(), ds.Schema, `DROP TABLE `+qualified)
				}()
				_, err := c.WriteBatch(ctx, connector.WriteBatchRequest{Schema: ds.Schema, Table: name, Columns: cols, PrimaryKeys: []string{"ID"}, Rows: [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("hello")}, {Raw: []byte("123.4500")}, {Raw: []byte{0, 1, 2, 3}}}}})
				if err != nil {
					return "", nil, err
				}
				row, found, err := c.ReadByKey(ctx, connector.ReadByKeyRequest{Schema: ds.Schema, Table: name, PrimaryKeys: []string{"ID"}, KeyColumns: cols[:1], KeyValues: []connector.Value{{Raw: []byte("1")}}, Columns: cols})
				if err != nil || !found || len(row) != len(cols) {
					if err == nil {
						err = fmt.Errorf("round-trip row not found")
					}
					return "", nil, err
				}
				if err := c.DeleteByKey(ctx, connector.DeleteByKeyRequest{Schema: ds.Schema, Table: name, PrimaryKeys: []string{"ID"}, Columns: cols[:1], Values: []connector.Value{{Raw: []byte("1")}}}); err != nil {
					return "", nil, err
				}
				return "prepared MERGE/BLOB/read/delete succeeded", map[string]any{"table": name}, nil
			})
		}
	}
	finish(rep, *output)
	if rep.Failed > 0 {
		os.Exit(1)
	}
}
