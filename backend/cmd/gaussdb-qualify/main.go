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
	postgresconnector "qmigration/backend/internal/connector/postgres"
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

func main() {
	var (
		host          = flag.String("host", "", "GaussDB host")
		port          = flag.Int("port", 8000, "GaussDB SQL port")
		user          = flag.String("user", "", "GaussDB username")
		passwordEnv   = flag.String("password-env", "GAUSSDB_PASSWORD", "environment variable containing password")
		database      = flag.String("database", "", "database name; defaults to username")
		schema        = flag.String("schema", "public", "schema to inspect")
		table         = flag.String("table", "", "optional table for metadata/full-read and CDC selection qualification")
		sampleRows    = flag.Int("sample-rows", 16, "maximum sample rows")
		cdc           = flag.Bool("cdc", false, "qualify SQL logical-decoding prerequisites and temporary mppdb_decoding slot")
		tlsMode       = flag.String("tls-mode", "DISABLE", "DISABLE, PREFERRED or REQUIRED")
		tlsServerName = flag.String("tls-server-name", "", "TLS server name")
		tlsCAFile     = flag.String("tls-ca-file", "", "CA PEM file")
		tlsCertFile   = flag.String("tls-cert-file", "", "client cert PEM file")
		tlsKeyFile    = flag.String("tls-key-file", "", "client key PEM file")
		timeout       = flag.Duration("timeout", 90*time.Second, "overall timeout")
		output        = flag.String("output", "", "optional JSON report path")
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
	if strings.TrimSpace(*database) == "" {
		*database = strings.TrimSpace(*user)
	}
	readFile := func(path string) string {
		if strings.TrimSpace(path) == "" {
			return ""
		}
		b, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read %s: %v\n", path, err)
			os.Exit(2)
		}
		return string(b)
	}

	_ = os.Setenv("QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE", "1")
	if *cdc {
		_ = os.Setenv("QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC", "1")
	}
	ds := domain.DataSource{Type: domain.DataSourceGaussDB, Host: strings.TrimSpace(*host), Port: *port, Username: strings.TrimSpace(*user), Password: password, Database: strings.TrimSpace(*database), Schema: strings.TrimSpace(*schema), TLSMode: domain.TLSMode(strings.ToUpper(strings.TrimSpace(*tlsMode))), TLSServerName: strings.TrimSpace(*tlsServerName), TLSCACert: readFile(*tlsCAFile), TLSClientCert: readFile(*tlsCertFile), TLSClientKey: readFile(*tlsKeyFile)}
	f := postgresconnector.NewFactory()
	base, err := f.New(ds)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer base.Close()
	rep := report{ToolVersion: toolVersion, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Target: map[string]any{"host": ds.Host, "port": ds.Port, "user": ds.Username, "database": ds.Database, "schema": ds.Schema, "cdc": *cdc, "tls_mode": ds.TLSMode}, Descriptor: f.Capabilities(domain.DataSourceGaussDB)}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	r := &runner{ctx: ctx, rep: &rep}
	if !r.run("connection", func() (string, map[string]any, error) {
		return "GaussDB PostgreSQL-wire authentication and SELECT 1", nil, base.TestConnection(ctx)
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

	var md *domain.TableMetadata
	if strings.TrimSpace(*table) == "" {
		r.skip("table-metadata", "--table not supplied")
		r.skip("full-read", "--table not supplied")
	} else {
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
		r.skip("cdc-prechecks", "--cdc not requested")
		r.skip("cdc-position", "--cdc not requested")
		r.skip("cdc-slot", "--cdc not requested")
	} else {
		if pc, ok := base.(connector.MigrationPrecheckConnector); ok {
			r.run("cdc-prechecks", func() (string, map[string]any, error) {
				items := pc.MigrationPrechecks(ctx, true)
				failed := []string{}
				for _, it := range items {
					if it.Level == domain.PrecheckFailed {
						failed = append(failed, it.Name+": "+it.Message)
					}
				}
				if len(failed) > 0 {
					return "", map[string]any{"items": items}, fmt.Errorf("%s", strings.Join(failed, "; "))
				}
				return fmt.Sprintf("%d prerequisite checks", len(items)), map[string]any{"items": items}, nil
			})
		}
		cs, ok := base.(connector.CDCCheckpointSource)
		if !ok {
			r.run("cdc-position", func() (string, map[string]any, error) {
				return "", nil, fmt.Errorf("connector does not implement CDC checkpoint source")
			})
			r.skip("cdc-slot", "CDC source unavailable")
		} else {
			r.run("cdc-position", func() (string, map[string]any, error) {
				pos, e := cs.CurrentCDCPosition(ctx)
				if e != nil {
					return "", nil, e
				}
				return pos.PositionValue, map[string]any{"type": pos.PositionType, "resource": pos.Resource}, nil
			})
			if strings.TrimSpace(*table) != "" {
				if v, ok := base.(connector.CDCSelectionValidator); ok {
					mapping := domain.TableMapping{SourceSchema: ds.Schema, SourceTable: strings.TrimSpace(*table), TargetSchema: ds.Schema, TargetTable: strings.TrimSpace(*table)}
					r.run("cdc-table-selection", func() (string, map[string]any, error) {
						e := v.ValidateCDCSelection(ctx, []domain.TableMapping{mapping})
						return "primary-key/binary-decoder selection", nil, e
					})
				}
			} else {
				r.skip("cdc-table-selection", "--table not supplied")
			}
			slot := fmt.Sprintf("qmigration_q_%d", time.Now().UnixNano())
			var qualifySlot string
			slotOK := r.run("cdc-slot", func() (string, map[string]any, error) {
				pos, e := cs.CreateCDCCheckpoint(ctx, slot)
				if e != nil {
					return "", nil, e
				}
				qualifySlot = pos.Resource
				return fmt.Sprintf("temporary LSN-based mppdb_decoding slot %s at %s", pos.Resource, pos.PositionValue), map[string]any{"slot": pos.Resource, "position_type": pos.PositionType, "position": pos.PositionValue}, nil
			})
			if qualifySlot != "" {
				defer func() { _ = cs.DropCDCCheckpoint(context.Background(), qualifySlot) }()
			}
			if !slotOK {
				r.skip("cdc-binary-peek", "temporary logical slot creation failed")
			} else if strings.TrimSpace(*table) == "" {
				r.skip("cdc-binary-peek", "--table not supplied")
			} else if pg, ok := base.(*postgresconnector.Connector); !ok {
				r.run("cdc-binary-peek", func() (string, map[string]any, error) {
					return "", nil, fmt.Errorf("GaussDB connector concrete type unavailable")
				})
			} else {
				r.run("cdc-binary-peek", func() (string, map[string]any, error) {
					tableName := strings.TrimSpace(ds.Schema) + "." + strings.TrimSpace(*table)
					txs, e := pg.PeekGaussDBTransactions(ctx, qualifySlot, 1, []string{tableName})
					if e != nil {
						return "", nil, e
					}
					return fmt.Sprintf("binary logical peek succeeded; committed selected transactions=%d", len(txs)), map[string]any{"slot": qualifySlot, "decoder": "mppdb_decoding/binary", "transactions": len(txs)}, nil
				})
			}
		}
	}
	finish(rep, *output)
	if rep.Failed > 0 {
		os.Exit(1)
	}
}
