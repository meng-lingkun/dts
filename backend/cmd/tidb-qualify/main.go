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

	"qmigration/backend/internal/cdc/ticdc"
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
		if item.Message == "" {
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
		host        = flag.String("host", "", "TiDB SQL host")
		port        = flag.Int("port", 4000, "TiDB SQL port")
		database    = flag.String("database", "", "database name")
		user        = flag.String("user", "", "TiDB username")
		passwordEnv = flag.String("password-env", "TIDB_PASSWORD", "environment variable containing TiDB password")
		cdcURL      = flag.String("cdc-url", "", "ticdc://host:8300?brokers=kafka1:9092,kafka2:9092")
		schema      = flag.String("schema", "", "schema/database to inspect; defaults to --database")
		table       = flag.String("table", "", "optional table to sample and use for the lifecycle filter")
		sampleRows  = flag.Int("sample-rows", 16, "maximum full-read sample rows")
		cdc         = flag.Bool("cdc", false, "create/delete an ephemeral TiCDC changefeed and verify its single-partition Kafka topic")
		timeout     = flag.Duration("timeout", 90*time.Second, "overall qualification timeout")
		tlsMode     = flag.String("tls-mode", "DISABLE", "DISABLE, PREFERRED, or REQUIRED for TiDB SQL")
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
	ep, err := ticdc.ParseEndpoint(*cdcURL)
	fatal(err)
	ds := domain.DataSource{
		Type:     domain.DataSourceTiDB,
		Host:     strings.TrimSpace(*host),
		Port:     *port,
		Username: strings.TrimSpace(*user),
		Password: password,
		Database: strings.TrimSpace(*database),
		Schema:   strings.TrimSpace(*schema),
		CDCURL:   strings.TrimSpace(*cdcURL),
		TLSMode:  domain.TLSMode(strings.ToUpper(strings.TrimSpace(*tlsMode))),
	}
	factory := mysqlconnector.NewFactory()
	base, err := factory.New(ds)
	fatal(err)
	defer base.Close()

	rep := report{
		ToolVersion: toolVersion,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Target: map[string]any{
			"host": ds.Host, "port": ds.Port, "database": ds.Database, "user": ds.Username,
			"tls_mode": ds.TLSMode, "ticdc_control": ep.ControlURL, "kafka_brokers": ep.Brokers,
		},
		Descriptor: factory.Capabilities(domain.DataSourceTiDB),
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	r := &runner{ctx: ctx, rep: &rep}

	if !r.run("tidb-connection", func() (string, map[string]any, error) {
		return "TiDB MySQL-wire authentication succeeded", nil, base.TestConnection(ctx)
	}) {
		finish(rep, *outFile)
		os.Exit(1)
	}
	r.run("tidb-version", func() (string, map[string]any, error) {
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
		if meta, e := base.GetTableMetadata(ctx, selectedSchema, selectedTable); e != nil {
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
		r.run("full-cdc-prechecks", func() (string, map[string]any, error) {
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
			return "TiDB/TiCDC/Kafka prechecks passed", map[string]any{"items": items}, nil
		})
	}

	var start *domain.CDCPosition
	if src, ok := base.(connector.CDCSource); ok {
		r.run("tidb-current-tso", func() (string, map[string]any, error) {
			var e error
			start, e = src.CurrentCDCPosition(ctx)
			if e != nil {
				return "", nil, e
			}
			return start.PositionValue, map[string]any{"position": start}, nil
		})
	} else {
		r.run("tidb-current-tso", func() (string, map[string]any, error) {
			return "", nil, errors.New("TiDB CDC position capability unavailable")
		})
	}

	control := ticdc.NewControlClient(ep, nil)
	r.run("ticdc-health", func() (string, map[string]any, error) {
		return ep.ControlURL + " healthy", nil, control.Health(ctx)
	})
	r.run("kafka-bootstrap", func() (string, map[string]any, error) {
		return "Kafka bootstrap broker reachable", map[string]any{"brokers": ep.Brokers}, ticdc.ProbeBrokers(ep, 3*time.Second)
	})

	if *cdc {
		if start == nil {
			r.run("ticdc-changefeed-lifecycle", func() (string, map[string]any, error) { return "", nil, errors.New("current TiDB TSO unavailable") })
		} else {
			pos, e := ticdc.ParsePosition(start.PositionValue)
			if e != nil {
				r.run("ticdc-changefeed-lifecycle", func() (string, map[string]any, error) { return "", nil, e })
			} else {
				runLifecycle(r, control, ep, selectedSchema, selectedTable, pos.TSO)
			}
		}
	} else {
		r.skip("ticdc-changefeed-lifecycle", "enable with --cdc; this check creates an ephemeral changefeed/topic and deletes the TiCDC changefeed after validation")
	}

	finish(rep, *outFile)
	if rep.Failed > 0 {
		os.Exit(1)
	}
}

func runLifecycle(r *runner, control *ticdc.ControlClient, ep ticdc.Endpoint, schema, table string, startTS uint64) {
	id := fmt.Sprintf("qmqual-%x", uint64(time.Now().UnixNano()))
	topic := id
	rule := strings.TrimSpace(schema) + ".*"
	if strings.TrimSpace(table) != "" {
		rule = strings.TrimSpace(schema) + "." + strings.TrimSpace(table)
	}
	r.run("ticdc-changefeed-lifecycle", func() (string, map[string]any, error) {
		if err := control.EnsureChangefeed(r.ctx, ticdc.ChangefeedPlan{ID: id, Topic: topic, StartTS: startTS, Tables: []string{rule}}); err != nil {
			return "create qualification changefeed", nil, err
		}
		defer func() { _ = control.DeleteChangefeed(context.Background(), id) }()
		cf, _, err := control.GetChangefeed(r.ctx, id)
		if err != nil || cf == nil {
			if err == nil {
				err = errors.New("qualification changefeed disappeared")
			}
			return "query qualification changefeed", nil, err
		}
		kafka, err := ticdc.NewKafkaClientForEndpoint(ep, "qmigration-tidb-qualify")
		if err != nil {
			return "create Kafka client", nil, err
		}
		var leader string
		var count int
		deadline := time.Now().Add(15 * time.Second)
		for {
			meta, e := kafka.Metadata(r.ctx, topic)
			if e == nil {
				leader, count = meta.Leader, meta.Count
				break
			}
			if time.Now().After(deadline) {
				return "Kafka topic readiness", map[string]any{"changefeed_id": cf.ID, "state": cf.State, "sink_uri": ticdc.RedactKafkaSinkURI(cf.SinkURI)}, e
			}
			select {
			case <-r.ctx.Done():
				return "Kafka topic readiness", nil, r.ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
		if count != ep.KafkaPartitions {
			return "Kafka topic partition validation", map[string]any{"partitions": count, "configured_partitions": ep.KafkaPartitions}, fmt.Errorf("expected %d Kafka partitions, got %d", ep.KafkaPartitions, count)
		}
		if err := control.DeleteChangefeed(r.ctx, id); err != nil {
			return "delete qualification changefeed", nil, err
		}
		return "ephemeral changefeed created, reached normal state, Kafka partition topology resolved, and changefeed deleted", map[string]any{"changefeed_id": id, "topic": topic, "filter": rule, "leader_partition_0": leader, "partitions": count, "kafka_tls": ep.KafkaTLS, "kafka_sasl_mechanism": ep.KafkaSASLMechanism}, nil
	})
}

func finish(rep report, output string) {
	rep.Qualified = rep.Failed == 0
	b, err := json.MarshalIndent(rep, "", "  ")
	fatal(err)
	b = append(b, '\n')
	if strings.TrimSpace(output) != "" {
		fatal(os.WriteFile(output, b, 0o600))
	}
	_, _ = os.Stdout.Write(b)
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
