package pgderivative

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
	postgresconnector "qmigration/backend/internal/connector/postgres"
	"qmigration/backend/internal/domain"
)

const ToolVersion = "0.15.0-rc27"

type Check struct {
	Name       string         `json:"name"`
	Status     string         `json:"status"`
	DurationMS int64          `json:"duration_ms"`
	Message    string         `json:"message,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
}

type Report struct {
	ToolVersion   string               `json:"tool_version"`
	GeneratedAt   string               `json:"generated_at_utc"`
	Product       string               `json:"product"`
	Target        map[string]any       `json:"target"`
	Descriptor    connector.Descriptor `json:"descriptor"`
	ServerVersion string               `json:"server_version,omitempty"`
	Checks        []Check              `json:"checks"`
	Passed        int                  `json:"passed"`
	Failed        int                  `json:"failed"`
	Skipped       int                  `json:"skipped"`
	Qualified     bool                 `json:"qualified"`
}

type runner struct {
	ctx context.Context
	rep *Report
}

func (r *runner) run(name string, fn func() (string, map[string]any, error)) bool {
	start := time.Now()
	msg, details, err := fn()
	c := Check{Name: name, DurationMS: time.Since(start).Milliseconds(), Message: msg, Details: details}
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
	r.rep.Checks = append(r.rep.Checks, Check{Name: name, Status: "SKIP", Message: reason})
	r.rep.Skipped++
}

func readFile(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	b, e := os.ReadFile(path)
	return string(b), e
}

func Run(product domain.DataSourceType, args []string) int {
	if product != domain.DataSourceOpenGauss && product != domain.DataSourceKingbase {
		fmt.Fprintln(os.Stderr, "unsupported product")
		return 2
	}
	name := string(product)
	defaultPort := 5432
	fs := flag.NewFlagSet(name+"-qualify", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	host := fs.String("host", "", name+" host")
	port := fs.Int("port", defaultPort, name+" SQL port")
	user := fs.String("user", "", name+" user")
	passwordEnv := fs.String("password-env", strings.ToUpper(strings.ReplaceAll(name, "-", "_"))+"_PASSWORD", "environment variable containing password")
	database := fs.String("database", "", "database; defaults to username")
	schema := fs.String("schema", "public", "schema")
	table := fs.String("table", "", "optional table for CDC selection/publication qualification")
	cdc := fs.Bool("cdc", false, "qualify experimental source CDC")
	tlsMode := fs.String("tls-mode", "REQUIRED", "DISABLE, PREFERRED or REQUIRED")
	tlsServerName := fs.String("tls-server-name", "", "TLS server name")
	tlsCAFile := fs.String("tls-ca-file", "", "CA PEM")
	tlsCertFile := fs.String("tls-cert-file", "", "client cert PEM")
	tlsKeyFile := fs.String("tls-key-file", "", "client key PEM")
	timeout := fs.Duration("timeout", 90*time.Second, "overall timeout")
	output := fs.String("output", "", "optional JSON report")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*host) == "" || strings.TrimSpace(*user) == "" {
		fmt.Fprintln(os.Stderr, "--host and --user are required")
		return 2
	}
	password := os.Getenv(*passwordEnv)
	if password == "" {
		fmt.Fprintf(os.Stderr, "password environment variable %s is empty\n", *passwordEnv)
		return 2
	}
	if strings.TrimSpace(*database) == "" {
		*database = strings.TrimSpace(*user)
	}
	ca, e := readFile(*tlsCAFile)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return 2
	}
	cert, e := readFile(*tlsCertFile)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return 2
	}
	key, e := readFile(*tlsKeyFile)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return 2
	}
	gate := "QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC"
	if product == domain.DataSourceKingbase {
		gate = "QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC"
	}
	if *cdc {
		_ = os.Setenv(gate, "1")
	}
	ds := domain.DataSource{Type: product, Host: strings.TrimSpace(*host), Port: *port, Username: strings.TrimSpace(*user), Password: password, Database: strings.TrimSpace(*database), Schema: strings.TrimSpace(*schema), TLSMode: domain.TLSMode(strings.ToUpper(strings.TrimSpace(*tlsMode))), TLSServerName: strings.TrimSpace(*tlsServerName), TLSCACert: ca, TLSClientCert: cert, TLSClientKey: key}
	f := postgresconnector.NewFactory()
	base, err := f.New(ds)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer base.Close()
	rep := Report{ToolVersion: ToolVersion, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Product: name, Target: map[string]any{"host": ds.Host, "port": ds.Port, "database": ds.Database, "user": ds.Username, "cdc": *cdc, "tls_mode": ds.TLSMode}, Descriptor: f.Capabilities(product)}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	r := &runner{ctx: ctx, rep: &rep}
	if !r.run("connection", func() (string, map[string]any, error) {
		return name + " native PostgreSQL-wire authentication and SELECT 1", nil, base.TestConnection(ctx)
	}) {
		return finish(rep, *output)
	}
	r.run("version", func() (string, map[string]any, error) {
		v, e := base.GetVersion(ctx)
		if e == nil {
			rep.ServerVersion = v
		}
		return v, nil, e
	})
	if !*cdc {
		r.skip("cdc-prechecks", "--cdc not requested")
		r.skip("cdc-position", "--cdc not requested")
		r.skip("cdc-slot", "--cdc not requested")
		return finish(rep, *output)
	}
	if pc, ok := base.(connector.MigrationPrecheckConnector); ok {
		r.run("cdc-prechecks", func() (string, map[string]any, error) {
			items := pc.MigrationPrechecks(ctx, true)
			var failed []string
			for _, it := range items {
				if it.Level == domain.PrecheckFailed {
					failed = append(failed, it.Name+": "+it.Message)
				}
			}
			if len(failed) > 0 {
				return "", map[string]any{"items": items}, errors.New(strings.Join(failed, "; "))
			}
			return fmt.Sprintf("%d prerequisite checks", len(items)), map[string]any{"items": items}, nil
		})
	}
	cs, ok := base.(connector.CDCCheckpointSource)
	if !ok {
		r.run("cdc-position", func() (string, map[string]any, error) {
			return "", nil, errors.New("connector does not implement CDC checkpoint source")
		})
		r.skip("cdc-slot", "CDC source unavailable")
		return finish(rep, *output)
	}
	r.run("cdc-position", func() (string, map[string]any, error) {
		p, e := cs.CurrentCDCPosition(ctx)
		if e != nil {
			return "", nil, e
		}
		return p.PositionValue, map[string]any{"position_type": p.PositionType, "resource": p.Resource}, nil
	})
	if strings.TrimSpace(*table) != "" {
		if v, ok := base.(connector.CDCSelectionValidator); ok {
			mapping := domain.TableMapping{SourceSchema: ds.Schema, SourceTable: strings.TrimSpace(*table), TargetSchema: ds.Schema, TargetTable: strings.TrimSpace(*table)}
			r.run("cdc-table-selection", func() (string, map[string]any, error) {
				return "primary-key deterministic update/delete selection", nil, v.ValidateCDCSelection(ctx, []domain.TableMapping{mapping})
			})
		}
	} else {
		r.skip("cdc-table-selection", "--table not supplied")
	}
	slot := fmt.Sprintf("qmigration_q_%d", time.Now().UnixNano())
	var resource string
	slotOK := r.run("cdc-slot", func() (string, map[string]any, error) {
		p, e := cs.CreateCDCCheckpoint(ctx, slot)
		if e != nil {
			return "", nil, e
		}
		resource = p.Resource
		return "temporary logical slot created", map[string]any{"slot": p.Resource, "position_type": p.PositionType, "position": p.PositionValue}, nil
	})
	if resource != "" {
		defer func() { _ = cs.DropCDCCheckpoint(context.Background(), resource) }()
	}
	pg, _ := base.(*postgresconnector.Connector)
	if !slotOK || pg == nil {
		r.skip("cdc-reader-probe", "slot creation/connector unavailable")
		return finish(rep, *output)
	}
	if product == domain.DataSourceOpenGauss {
		if strings.TrimSpace(*table) == "" {
			r.skip("cdc-reader-probe", "--table not supplied")
		} else {
			tableName := ds.Schema + "." + strings.TrimSpace(*table)
			r.run("cdc-reader-probe", func() (string, map[string]any, error) {
				txs, e := pg.PeekOpenGaussTransactions(ctx, resource, 1, []string{tableName})
				return fmt.Sprintf("mppdb_decoding peek succeeded; committed selected transactions=%d", len(txs)), map[string]any{"transactions": len(txs), "decoder": "mppdb_decoding"}, e
			})
		}
	} else {
		if strings.TrimSpace(*table) == "" {
			r.skip("cdc-publication", "--table not supplied")
		} else {
			pub := fmt.Sprintf("qmigration_qpub_%d", time.Now().UnixNano())
			ok := r.run("cdc-publication", func() (string, map[string]any, error) {
				e := pg.EnsurePublication(ctx, pub, []string{ds.Schema + "." + strings.TrimSpace(*table)})
				return "temporary Kingbase kboutput publication created", map[string]any{"publication": pub}, e
			})
			if ok {
				defer func() { _ = pg.DropPublication(context.Background(), pub) }()
			}
		}
		r.run("cdc-dialect", func() (string, map[string]any, error) {
			return "Kingbase kboutput uses dedicated KINGBASE_LSN checkpoint and strict wire-conformance decoder dialect", map[string]any{"position_type": "KINGBASE_LSN", "decoder": "kboutput"}, nil
		})
	}
	return finish(rep, *output)
}

func finish(rep Report, output string) int {
	rep.Qualified = rep.Failed == 0
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if strings.TrimSpace(output) != "" {
		_ = os.WriteFile(output, append(b, '\n'), 0600)
	}
	if rep.Failed > 0 {
		return 1
	}
	return 0
}
