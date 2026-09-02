package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"qmigration/backend/internal/cdc/gbase8scdc"
	"qmigration/backend/internal/connector"
	gbase8sconnector "qmigration/backend/internal/connector/gbase8s"
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

type runner struct{ rep *report }

func (r *runner) run(name string, fn func() (string, map[string]any, error)) bool {
	started := time.Now()
	message, details, err := fn()
	item := check{Name: name, DurationMS: time.Since(started).Milliseconds(), Message: message, Details: details}
	if err != nil {
		item.Status = "FAIL"
		if item.Message == "" {
			item.Message = err.Error()
		} else {
			item.Message += ": " + err.Error()
		}
		r.rep.Failed++
	} else {
		item.Status = "PASS"
		r.rep.Passed++
	}
	r.rep.Checks = append(r.rep.Checks, item)
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

func main() {
	host := flag.String("host", "", "GBase 8s SQL endpoint host")
	port := flag.Int("port", 9088, "GBase 8s SQL port")
	user := flag.String("user", "", "GBase 8s username")
	passwordEnv := flag.String("password-env", "GBASE8S_PASSWORD", "environment variable containing the datasource password")
	database := flag.String("database", "", "GBase 8s database")
	schema := flag.String("schema", "", "owner/schema to inspect; defaults to username")
	table := flag.String("table", "", "optional table for metadata/full-read qualification")
	driver := flag.String("driver", "odbc", "registered database/sql ODBC driver name")
	dsnEnv := flag.String("dsn-env", "GBASE8S_ODBC_DSN", "environment variable containing a non-secret ODBC DSN/name")
	sampleRows := flag.Int("sample-rows", 16, "maximum sample rows")
	targetWrite := flag.Bool("target-write", false, "create/write/replay/read/delete/drop a temporary target table")
	cdc := flag.Bool("cdc", false, "validate the syscdcv1/CSDK CDC provider and capture a restart checkpoint; requires --table")
	cdcSmartLOB := flag.Bool("cdc-smart-lob", false, "require RC28 event-owned smart BLOB/CLOB image contract for the selected --table; implies --cdc semantics")
	cdcURL := flag.String("cdc-url", strings.TrimSpace(os.Getenv("GBASE8S_CDC_URL")), "GBase 8s CSDK CDC provider URL")
	timeout := flag.Duration("timeout", 90*time.Second, "overall timeout")
	output := flag.String("output", "", "optional JSON report path")
	flag.Parse()

	if strings.TrimSpace(*host) == "" || strings.TrimSpace(*user) == "" || strings.TrimSpace(*database) == "" {
		fmt.Fprintln(os.Stderr, "--host, --user and --database are required")
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
	dsn := strings.TrimSpace(os.Getenv(*dsnEnv))
	if dsn == "" {
		fmt.Fprintf(os.Stderr, "ODBC DSN environment variable %s is empty\n", *dsnEnv)
		os.Exit(2)
	}
	if strings.TrimSpace(*schema) == "" {
		*schema = strings.TrimSpace(*user)
	}
	_ = os.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE", "1")
	if *cdcSmartLOB {
		*cdc = true
	}
	if *cdc {
		_ = os.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8S_CDC", "1")
		if *cdcSmartLOB {
			_ = os.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8S_SMART_LOB_CDC", "1")
		}
		if strings.TrimSpace(*cdcURL) == "" || strings.TrimSpace(*table) == "" {
			fmt.Fprintln(os.Stderr, "--cdc requires --table and --cdc-url (or GBASE8S_CDC_URL)")
			os.Exit(2)
		}
	}

	ds := domain.DataSource{
		Type: domain.DataSourceGBase8s, Host: strings.TrimSpace(*host), Port: *port,
		Username: strings.TrimSpace(*user), Password: password, Database: strings.TrimSpace(*database), Schema: strings.TrimSpace(*schema),
		JDBCURL: "odbc:" + dsn, DriverClass: strings.TrimSpace(*driver), CDCURL: strings.TrimSpace(*cdcURL), TLSMode: domain.TLSModeDisable,
	}
	factory := gbase8sconnector.NewFactory()
	raw, err := factory.New(ds)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer raw.Close()

	rep := report{
		ToolVersion: toolVersion,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Target: map[string]any{
			"host": ds.Host, "port": ds.Port, "user": ds.Username, "database": ds.Database, "schema": ds.Schema,
			"product_family": "GBase 8s V8.8", "driver": ds.DriverClass,
			"odbc_dsn_configured": true, "provider_plugin_configured": strings.TrimSpace(os.Getenv("QMIGRATION_GBASE8S_DRIVER_PLUGIN")) != "", "cdc_provider_configured": strings.TrimSpace(*cdcURL) != "",
		},
		Descriptor: factory.Capabilities(domain.DataSourceGBase8s),
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	rr := &runner{rep: &rep}

	if !rr.run("connection", func() (string, map[string]any, error) {
		return "GBase Client-SDK ODBC authentication + DBINFO version probe", nil, raw.TestConnection(ctx)
	}) {
		finish(rep, *output)
		os.Exit(1)
	}
	rr.run("version", func() (string, map[string]any, error) {
		version, err := raw.GetVersion(ctx)
		if err == nil {
			rep.ServerVersion = version
		}
		return version, nil, err
	})
	rr.run("prechecks", func() (string, map[string]any, error) {
		pc := raw.(connector.MigrationPrecheckConnector).MigrationPrechecks(ctx, *cdc)
		failed := 0
		for _, item := range pc {
			if item.Level == domain.PrecheckFailed {
				failed++
			}
		}
		if failed > 0 {
			return fmt.Sprintf("%d precheck(s) failed", failed), map[string]any{"items": pc}, fmt.Errorf("GBase 8s precheck failure")
		}
		return fmt.Sprintf("%d precheck item(s)", len(pc)), map[string]any{"items": pc}, nil
	})
	rr.run("schemas", func() (string, map[string]any, error) {
		xs, err := raw.ListSchemas(ctx)
		return fmt.Sprintf("%d visible owners", len(xs)), map[string]any{"count": len(xs)}, err
	})

	var selectedMD *domain.TableMetadata
	if strings.TrimSpace(*table) == "" {
		rr.skip("table-metadata", "--table not supplied")
		rr.skip("full-read", "--table not supplied")
		rr.skip("keyset-boundary", "--table not supplied")
	} else {
		var md *domain.TableMetadata
		ok := rr.run("table-metadata", func() (string, map[string]any, error) {
			var err error
			md, err = raw.GetTableMetadata(ctx, ds.Schema, strings.TrimSpace(*table))
			if err != nil {
				return "", nil, err
			}
			return fmt.Sprintf("columns=%d pk=%v indexes=%d", len(md.Columns), md.PrimaryKeys, len(md.Indexes)), map[string]any{"columns": len(md.Columns), "primary_keys": md.PrimaryKeys, "indexes": len(md.Indexes)}, nil
		})
		selectedMD = md
		if !ok || md == nil || len(md.PrimaryKeys) == 0 {
			rr.skip("full-read", "metadata failed or table has no stable migration key")
			rr.skip("keyset-boundary", "metadata failed or table has no stable migration key")
		} else {
			dc := raw.(connector.DataConnector)
			rr.run("full-read", func() (string, map[string]any, error) {
				batch, err := dc.ReadBatch(ctx, connector.ReadBatchRequest{Schema: ds.Schema, Table: strings.TrimSpace(*table), PrimaryKey: md.PrimaryKey, PrimaryKeys: md.PrimaryKeys, Columns: md.Columns, UseKeyset: true, Limit: *sampleRows})
				if err != nil {
					return "", nil, err
				}
				return fmt.Sprintf("sample rows=%d bytes=%d", len(batch.Rows), batch.Bytes), map[string]any{"rows": len(batch.Rows), "bytes": batch.Bytes}, nil
			})
			kb := raw.(connector.KeysetBoundaryConnector)
			rr.run("keyset-boundary", func() (string, map[string]any, error) {
				bs, err := kb.PlanKeysetBoundaries(ctx, connector.KeysetBoundaryRequest{Schema: ds.Schema, Table: strings.TrimSpace(*table), Keys: md.PrimaryKeys, Columns: md.Columns, Partitions: 2})
				return fmt.Sprintf("boundaries=%d", len(bs)), map[string]any{"count": len(bs)}, err
			})
		}
	}

	if !*cdc {
		rr.skip("cdc-agent", "--cdc not requested")
		rr.skip("source-cdc", "--cdc not requested")
	} else {
		rr.run("cdc-agent", func() (string, map[string]any, error) {
			client, err := gbase8scdc.NewClient(strings.TrimSpace(*cdcURL), os.Getenv("QMIGRATION_GBASE8S_CDC_CA_PEM"), os.Getenv("QMIGRATION_GBASE8S_CDC_SERVER_NAME"), os.Getenv("QMIGRATION_GBASE8S_CDC_TOKEN"))
			if err != nil {
				return "", nil, err
			}
			info, err := client.HealthInfo(ctx)
			if err != nil {
				return "", nil, err
			}
			return "CDC agent/provider health succeeded", map[string]any{"api_version": info.APIVersion, "provider_kind": info.Provider.Kind, "provider_abi": info.Provider.ABIVersion, "sha256_pinned": info.Provider.SHA256Pinned}, nil
		})
		rr.run("cdc-agent-status", func() (string, map[string]any, error) {
			client, err := gbase8scdc.NewClient(strings.TrimSpace(*cdcURL), os.Getenv("QMIGRATION_GBASE8S_CDC_CA_PEM"), os.Getenv("QMIGRATION_GBASE8S_CDC_SERVER_NAME"), os.Getenv("QMIGRATION_GBASE8S_CDC_TOKEN"))
			if err != nil {
				return "", nil, err
			}
			status, err := client.StatusInfo(ctx)
			if err != nil {
				return "", nil, err
			}
			return "CDC agent observability endpoint succeeded", map[string]any{
				"status": status.Status, "api_version": status.APIVersion, "uptime_seconds": status.UptimeSeconds, "busy": status.Busy,
				"health_calls": status.HealthCalls, "checkpoint_calls": status.CheckpointCalls, "read_calls": status.ReadCalls,
				"read_errors": status.ReadErrors, "records_returned": status.RecordsReturned, "provider_kind": status.Provider.Kind,
			}, nil
		})
		rr.run("source-cdc", func() (string, map[string]any, error) {
			src := raw.(connector.CDCSelectionPositionSource)
			mapping := domain.TableMapping{SourceSchema: ds.Schema, SourceTable: strings.TrimSpace(*table)}
			if v, ok := raw.(connector.CDCSelectionValidator); ok {
				if err := v.ValidateCDCSelection(ctx, []domain.TableMapping{mapping}); err != nil {
					return "", nil, err
				}
			}
			pos, err := src.CurrentCDCPositionForSelection(ctx, []domain.TableMapping{mapping})
			if err != nil {
				return "", nil, err
			}
			return "CSDK CDC provider created a selected-table restart checkpoint", map[string]any{"position_type": pos.PositionType, "position_value": pos.PositionValue, "resource_configured": pos.Resource != ""}, nil
		})
		if *cdcSmartLOB {
			rr.run("source-cdc-smart-lob-contract", func() (string, map[string]any, error) {
				if selectedMD == nil {
					return "", nil, fmt.Errorf("selected table metadata unavailable")
				}
				hasLOB := false
				for _, c := range selectedMD.Columns {
					t := strings.ToLower(c.DataType + " " + c.ColumnType)
					if strings.Contains(t, "blob") || strings.Contains(t, "clob") {
						hasLOB = true
						break
					}
				}
				if !hasLOB {
					return "", nil, fmt.Errorf("--cdc-smart-lob requires selected table to contain BLOB/CLOB")
				}
				sel, err := gbase8scdc.BuildTableSelection(ds.Schema, strings.TrimSpace(*table), selectedMD.Columns, selectedMD.PrimaryKeys)
				if err != nil {
					return "", nil, err
				}
				client, err := gbase8scdc.NewClient(strings.TrimSpace(*cdcURL), os.Getenv("QMIGRATION_GBASE8S_CDC_CA_PEM"), os.Getenv("QMIGRATION_GBASE8S_CDC_SERVER_NAME"), os.Getenv("QMIGRATION_GBASE8S_CDC_TOKEN"))
				if err != nil {
					return "", nil, err
				}
				cp, err := client.Checkpoint(ctx, gbase8scdc.CheckpointRequest{Database: ds.Database, Tables: []gbase8scdc.TableSelection{sel}})
				if err != nil {
					return "", nil, err
				}
				if cp.SmartLOBImageContract != gbase8scdc.SmartLOBImageContract {
					return "", nil, fmt.Errorf("provider contract=%q", cp.SmartLOBImageContract)
				}
				return "provider attests event-owned smart-LOB historical images; current-row SELECT fallback is forbidden", map[string]any{"contract": cp.SmartLOBImageContract, "provider_api": gbase8scdc.AgentAPIVersion}, nil
			})
		}
	}

	if !*targetWrite {
		rr.skip("target-write", "--target-write not requested")
	} else {
		c := raw.(*gbase8sconnector.Connector)
		rr.run("target-write", func() (string, map[string]any, error) {
			name := fmt.Sprintf("qmigration_q_%x", uint64(time.Now().UnixNano()))
			cols := []domain.ColumnInfo{
				{Name: "id", DataType: "bigint", ColumnType: "BIGINT", Nullable: false},
				{Name: "txt", DataType: "varchar", ColumnType: "VARCHAR(200)", Nullable: true},
				{Name: "payload", DataType: "blob", ColumnType: "BLOB", Nullable: true},
			}
			if err := c.CreateTableWithPrimaryKeys(ctx, ds.Schema, name, cols, []string{"id"}); err != nil {
				return "", nil, err
			}
			defer c.DropQualificationTable(context.Background(), ds.Schema, name)
			first := connector.WriteBatchRequest{Schema: ds.Schema, Table: name, Columns: cols, PrimaryKeys: []string{"id"}, Rows: [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("first")}, {Raw: []byte{0, 1, 2, 3}}}}}
			if _, err := c.WriteBatch(ctx, first); err != nil {
				return "", nil, err
			}
			first.Rows[0][1].Raw = []byte("updated")
			if _, err := c.WriteBatch(ctx, first); err != nil {
				return "", nil, err
			}
			row, found, err := c.ReadByKey(ctx, connector.ReadByKeyRequest{Schema: ds.Schema, Table: name, PrimaryKeys: []string{"id"}, KeyColumns: cols[:1], KeyValues: []connector.Value{{Raw: []byte("1")}}, Columns: cols})
			if err != nil {
				return "", nil, err
			}
			if !found || len(row) != 3 || string(row[1].Raw) != "updated" || row[2].Null || !bytes.Equal(row[2].Raw, []byte{0, 1, 2, 3}) {
				return "", nil, fmt.Errorf("idempotent target/binary round-trip mismatch")
			}
			if err := c.BeginCDCTransaction(ctx); err != nil {
				return "", nil, err
			}
			if err := c.DeleteByKey(ctx, connector.DeleteByKeyRequest{Schema: ds.Schema, Table: name, PrimaryKeys: []string{"id"}, Columns: cols[:1], Values: []connector.Value{{Raw: []byte("1")}}}); err != nil {
				_ = c.RollbackCDCTransaction(ctx)
				return "", nil, err
			}
			if err := c.CommitCDCTransaction(ctx); err != nil {
				return "", nil, err
			}
			return "keyed update/existence/insert replay + exact BLOB round-trip + transactional target delete succeeded", map[string]any{"table": name}, nil
		})
	}

	finish(rep, *output)
	if rep.Failed > 0 {
		os.Exit(1)
	}
}
