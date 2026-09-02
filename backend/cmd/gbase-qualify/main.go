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

	"qmigration/backend/internal/connector"
	gbaseconnector "qmigration/backend/internal/connector/gbase"
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
	st := time.Now()
	msg, details, err := fn()
	c := check{Name: name, DurationMS: time.Since(st).Milliseconds(), Message: msg, Details: details}
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
func (r *runner) skip(name, why string) {
	r.rep.Checks = append(r.rep.Checks, check{Name: name, Status: "SKIP", Message: why})
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
	host := flag.String("host", "", "GBase 8a gcluster host")
	port := flag.Int("port", 5258, "GBase 8a SQL port")
	user := flag.String("user", "", "GBase username")
	passwordEnv := flag.String("password-env", "GBASE_PASSWORD", "environment variable containing password")
	database := flag.String("database", "", "database/schema to inspect")
	table := flag.String("table", "", "optional table for metadata/full-read qualification")
	sampleRows := flag.Int("sample-rows", 16, "maximum sample rows")
	targetWrite := flag.Bool("target-write", false, "create and exercise a temporary GBase 8a target table")
	targetCDC := flag.Bool("target-cdc", false, "exercise retry-idempotent non-transactional target CDC apply")
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
	_ = os.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE", "1")
	if *targetCDC {
		_ = os.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8A_TARGET_CDC", "1")
	}
	ds := domain.DataSource{Type: domain.DataSourceGBase, Host: strings.TrimSpace(*host), Port: *port, Username: strings.TrimSpace(*user), Password: password, Database: strings.TrimSpace(*database)}
	f := gbaseconnector.NewFactory()
	raw, err := f.New(ds)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer raw.Close()
	rep := report{ToolVersion: toolVersion, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Target: map[string]any{"host": ds.Host, "port": ds.Port, "user": ds.Username, "database": ds.Database, "product_family": "GBase 8a MPP Cluster"}, Descriptor: f.Capabilities(domain.DataSourceGBase)}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	rr := &runner{ctx: ctx, rep: &rep}
	if !rr.run("connection", func() (string, map[string]any, error) {
		return "GBase 8a packet authentication and SELECT 1", nil, raw.TestConnection(ctx)
	}) {

		if !*targetCDC {
			rr.skip("target-cdc", "--target-cdc not requested")
		} else {
			c := raw.(*gbaseconnector.Connector)
			rr.run("target-cdc", func() (string, map[string]any, error) {
				desc := f.Capabilities(domain.DataSourceGBase)
				if !desc.Has(connector.CapabilityCDCApply) || desc.Has(connector.CapabilityCDCTransactional) {
					return "", nil, fmt.Errorf("unexpected GBase target CDC capability contract: %+v", desc.Capabilities)
				}
				name := fmt.Sprintf("qmigration_cdc_q_%x", uint64(time.Now().UnixNano()))
				cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint", Nullable: false}, {Name: "txt", DataType: "varchar", ColumnType: "varchar(200)", Nullable: true}}
				if err := c.CreateTableWithPrimaryKeys(ctx, ds.Database, name, cols, []string{"id"}); err != nil {
					return "", nil, err
				}
				defer c.DropQualificationTable(context.Background(), ds.Database, name)
				apply := connector.CDCApplyConnector(c)
				req := connector.WriteBatchRequest{Schema: ds.Database, Table: name, Columns: cols, PrimaryKeys: []string{"id"}, Rows: [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("v1")}}}}
				if _, err := apply.WriteBatch(ctx, req); err != nil {
					return "", nil, err
				}
				req.Rows[0][1].Raw = []byte("v2")
				if _, err := apply.WriteBatch(ctx, req); err != nil {
					return "", nil, err
				}
				lookup := connector.PointLookupConnector(c)
				row, exists, err := lookup.ReadByKey(ctx, connector.ReadByKeyRequest{Schema: ds.Database, Table: name, PrimaryKeys: []string{"id"}, KeyColumns: []domain.ColumnInfo{cols[0]}, KeyValues: []connector.Value{{Raw: []byte("1")}}, Columns: cols})
				if err != nil {
					return "", nil, err
				}
				if !exists || len(row) != 2 || string(row[1].Raw) != "v2" {
					return "", nil, fmt.Errorf("target CDC MERGE replay mismatch: exists=%v row=%v", exists, row)
				}
				if err := apply.DeleteByKey(ctx, connector.DeleteByKeyRequest{Schema: ds.Database, Table: name, PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{cols[0]}, Values: []connector.Value{{Raw: []byte("1")}}}); err != nil {
					return "", nil, err
				}
				_, exists, err = lookup.ReadByKey(ctx, connector.ReadByKeyRequest{Schema: ds.Database, Table: name, PrimaryKeys: []string{"id"}, KeyColumns: []domain.ColumnInfo{cols[0]}, KeyValues: []connector.Value{{Raw: []byte("1")}}, Columns: cols})
				if err != nil {
					return "", nil, err
				}
				if exists {
					return "", nil, fmt.Errorf("target CDC delete did not remove qualification row")
				}
				return "retry-idempotent HASH MERGE update + keyed delete succeeded; source-transaction atomicity intentionally not claimed", map[string]any{"table": name, "transactional": false}, nil
			})
		}
		finish(rep, *output)
		os.Exit(1)
	}
	rr.run("version", func() (string, map[string]any, error) {
		v, e := raw.GetVersion(ctx)
		if e == nil {
			rep.ServerVersion = v
		}
		return v, nil, e
	})
	rr.run("prechecks", func() (string, map[string]any, error) {
		p := raw.(connector.MigrationPrecheckConnector).MigrationPrechecks(ctx, false)
		failed := 0
		for _, x := range p {
			if x.Level == domain.PrecheckFailed {
				failed++
			}
		}
		if failed > 0 {
			return fmt.Sprintf("%d precheck(s) failed", failed), map[string]any{"items": p}, fmt.Errorf("GBase precheck failure")
		}
		return fmt.Sprintf("%d precheck item(s)", len(p)), map[string]any{"items": p}, nil
	})
	rr.run("schemas", func() (string, map[string]any, error) {
		xs, e := raw.ListSchemas(ctx)
		return fmt.Sprintf("%d visible databases", len(xs)), map[string]any{"count": len(xs)}, e
	})
	if strings.TrimSpace(*table) == "" {
		rr.skip("table-metadata", "--table not supplied")
		rr.skip("full-read", "--table not supplied")
	} else {
		var md *domain.TableMetadata
		ok := rr.run("table-metadata", func() (string, map[string]any, error) {
			var e error
			md, e = raw.GetTableMetadata(ctx, ds.Database, strings.TrimSpace(*table))
			if e != nil {
				return "", nil, e
			}
			return fmt.Sprintf("columns=%d pk=%v indexes=%d", len(md.Columns), md.PrimaryKeys, len(md.Indexes)), map[string]any{"columns": len(md.Columns), "primary_keys": md.PrimaryKeys, "indexes": len(md.Indexes)}, nil
		})
		if !ok || md == nil || len(md.PrimaryKeys) == 0 {
			rr.skip("full-read", "metadata failed or table has no migration key")
		} else {
			dc := raw.(connector.DataConnector)
			rr.run("full-read", func() (string, map[string]any, error) {
				b, e := dc.ReadBatch(ctx, connector.ReadBatchRequest{Schema: ds.Database, Table: strings.TrimSpace(*table), PrimaryKey: md.PrimaryKey, PrimaryKeys: md.PrimaryKeys, Columns: md.Columns, UseKeyset: true, Limit: *sampleRows})
				if e != nil {
					return "", nil, e
				}
				return fmt.Sprintf("sample rows=%d bytes=%d", len(b.Rows), b.Bytes), map[string]any{"rows": len(b.Rows), "bytes": b.Bytes}, nil
			})
			if kb, ok := raw.(connector.KeysetBoundaryConnector); ok {
				rr.run("keyset-boundary", func() (string, map[string]any, error) {
					bs, e := kb.PlanKeysetBoundaries(ctx, connector.KeysetBoundaryRequest{Schema: ds.Database, Table: strings.TrimSpace(*table), Keys: md.PrimaryKeys, Columns: md.Columns, Partitions: 2})
					return fmt.Sprintf("boundaries=%d", len(bs)), map[string]any{"count": len(bs)}, e
				})
			}
		}
	}
	if !*targetWrite {
		rr.skip("target-write", "--target-write not requested")
	} else {
		c := raw.(*gbaseconnector.Connector)
		rr.run("target-write", func() (string, map[string]any, error) {
			name := fmt.Sprintf("qmigration_q_%x", uint64(time.Now().UnixNano()))
			cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint", Nullable: false}, {Name: "txt", DataType: "varchar", ColumnType: "varchar(200)", Nullable: true}, {Name: "payload", DataType: "longblob", Nullable: true}}
			if err := c.CreateTableWithPrimaryKeys(ctx, ds.Database, name, cols, []string{"id"}); err != nil {
				return "", nil, err
			}
			defer c.DropQualificationTable(context.Background(), ds.Database, name)
			dc := connector.DataConnector(c)
			first := connector.WriteBatchRequest{Schema: ds.Database, Table: name, Columns: cols, PrimaryKeys: []string{"id"}, Rows: [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("first")}, {Raw: []byte{0, 1, 2, 3}}}}}
			if _, err := dc.WriteBatch(ctx, first); err != nil {
				return "", nil, err
			}
			first.Rows[0][1].Raw = []byte("updated")
			if _, err := dc.WriteBatch(ctx, first); err != nil {
				return "", nil, err
			}
			b, err := dc.ReadBatch(ctx, connector.ReadBatchRequest{Schema: ds.Database, Table: name, PrimaryKey: "id", PrimaryKeys: []string{"id"}, Columns: cols, UseKeyset: true, Limit: 10})
			if err != nil {
				return "", nil, err
			}
			expectedBinary := []byte{0, 1, 2, 3}
			if len(b.Rows) != 1 || len(b.Rows[0]) != 3 || string(b.Rows[0][1].Raw) != "updated" || b.Rows[0][2].Null || !bytes.Equal(b.Rows[0][2].Raw, expectedBinary) {
				return "", nil, fmt.Errorf("idempotent MERGE/binary round-trip mismatch: rows=%d", len(b.Rows))
			}
			return "EXPRESS HASH create + SHOW CREATE layout validation + staging MERGE replay + exact binary round-trip succeeded", map[string]any{"table": name}, nil
		})
	}
	finish(rep, *output)
	if rep.Failed > 0 {
		os.Exit(1)
	}
}
