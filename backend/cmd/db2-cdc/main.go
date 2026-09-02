package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"qmigration/backend/internal/cdc/db2log"
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	db2connector "qmigration/backend/internal/connector/db2"
	"qmigration/backend/internal/domain"
)

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
func envOn(k string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(k)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

type readyResponse struct {
	Ready      bool                   `json:"ready"`
	TaskStatus domain.MigrationStatus `json:"task_status"`
}

func waitReady(ctx context.Context, client *http.Client, endpoint, token string) error {
	if strings.TrimSpace(endpoint) == "" {
		return nil
	}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
		if err != nil {
			return err
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if wt := strings.TrimSpace(os.Getenv("QMIGRATION_WORKER_TOKEN")); wt != "" {
			req.Header.Set("X-QMigration-Worker-Token", wt)
		}
		resp, err := client.Do(req)
		if err == nil {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				var st readyResponse
				if e := json.Unmarshal(b, &st); e != nil {
					return e
				}
				if st.Ready {
					return nil
				}
				log.Printf("DB2 CDC waiting; task status=%s", st.TaskStatus)
			} else {
				err = fmt.Errorf("CDC readiness returned %s: %s", resp.Status, strings.TrimSpace(string(b)))
			}
		}
		if err != nil {
			log.Printf("CDC readiness check failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}
func postTx(ctx context.Context, client *http.Client, endpoint, token, direction string, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
	b, err := json.Marshal(domain.CDCApplyRequest{Direction: direction, Events: events})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if wt := strings.TrimSpace(os.Getenv("QMIGRATION_WORKER_TOKEN")); wt != "" {
		req.Header.Set("X-QMigration-Worker-Token", wt)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("QMigration returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	var out domain.CDCApplyResult
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			return nil, err
		}
	}
	return &out, nil
}
func run(ctx context.Context) error {
	if !envOn("QMIGRATION_EXPERIMENTAL_DB2_NATIVE") || !envOn("QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC") {
		return errors.New("DB2 CDC requires QMIGRATION_EXPERIMENTAL_DB2_NATIVE=1 and QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC=1")
	}
	taskID := env("QMIGRATION_TASK_ID", "")
	if taskID == "" {
		return errors.New("QMIGRATION_TASK_ID is required")
	}
	server := strings.TrimRight(env("QMIGRATION_SERVER", "http://127.0.0.1:8080"), "/")
	direction := strings.ToLower(env("QMIGRATION_CDC_DIRECTION", "forward"))
	if direction != "forward" && direction != "reverse" {
		return errors.New("QMIGRATION_CDC_DIRECTION must be forward or reverse")
	}
	port, _ := strconv.Atoi(env("QMIGRATION_DB2_PORT", "50000"))
	ds := domain.DataSource{Type: domain.DataSourceDB2, Host: env("QMIGRATION_DB2_HOST", ""), Port: port, Username: env("QMIGRATION_DB2_USER", ""), Password: env("QMIGRATION_DB2_PASSWORD", ""), Database: env("QMIGRATION_DB2_DATABASE", ""), Schema: env("QMIGRATION_DB2_SCHEMA", ""), CDCURL: env("QMIGRATION_DB2_LOG_URL", ""), TLSMode: domain.TLSMode(strings.ToUpper(env("QMIGRATION_DB2_TLS_MODE", "PREFERRED"))), TLSServerName: env("QMIGRATION_DB2_TLS_SERVER_NAME", ""), TLSCACert: os.Getenv("QMIGRATION_DB2_TLS_CA"), TLSClientCert: os.Getenv("QMIGRATION_DB2_TLS_CLIENT_CERT"), TLSClientKey: os.Getenv("QMIGRATION_DB2_TLS_CLIENT_KEY")}
	if ds.Host == "" || ds.Username == "" || ds.Database == "" || ds.CDCURL == "" {
		return errors.New("DB2 HOST, USER, DATABASE and DB2_LOG_URL are required")
	}
	raw, err := db2connector.NewFactory().New(ds)
	if err != nil {
		return err
	}
	src, ok := raw.(*db2connector.Connector)
	if !ok {
		return errors.New("DB2 native connector type assertion failed")
	}
	defer src.Close()
	tables := []string{}
	for _, v := range strings.Split(env("QMIGRATION_DB2_TABLES", ""), ",") {
		if x := strings.TrimSpace(v); x != "" {
			tables = append(tables, x)
		}
	}
	if len(tables) == 0 {
		return errors.New("QMIGRATION_DB2_TABLES is required")
	}
	specs, err := src.CDCSelections(ctx, tables)
	if err != nil {
		return err
	}
	sels := make([]db2log.Selection, 0, len(specs))
	for _, s := range specs {
		sels = append(sels, db2log.Selection{Schema: s.Schema, Table: s.Table, TablespaceID: s.TablespaceID, TableID: s.TableID, Columns: s.Columns, PrimaryKeys: s.PrimaryKeys})
	}
	agent, err := db2log.NewClient(ds.CDCURL, os.Getenv("QMIGRATION_DB2_LOG_TLS_CA"), env("QMIGRATION_DB2_LOG_TLS_SERVER_NAME", ""), os.Getenv("QMIGRATION_DB2_LOG_TOKEN"))
	if err != nil {
		return err
	}
	if err = agent.Health(ctx); err != nil {
		return fmt.Errorf("DB2 Log Agent health: %w", err)
	}
	var reader cdcruntime.Reader
	if rawVector := strings.TrimSpace(os.Getenv("QMIGRATION_DB2_START_VECTOR")); rawVector != "" || envOn("QMIGRATION_EXPERIMENTAL_DB2_PURESCALE") {
		if !envOn("QMIGRATION_EXPERIMENTAL_DB2_PURESCALE") {
			return errors.New("DB2 pureScale vector requires QMIGRATION_EXPERIMENTAL_DB2_PURESCALE=1")
		}
		var startVector *db2log.PureScaleVector
		if rawVector != "" {
			v, e := db2log.ParsePureScaleVector(rawVector)
			if e != nil {
				return e
			}
			startVector = &v
		}
		reader, err = db2log.NewPureScaleReader(ctx, agent, startVector, sels, ds.CDCURL)
	} else {
		start, e := db2log.ParseLRI(env("QMIGRATION_DB2_START_LRI", ""))
		if e != nil {
			return e
		}
		reader, err = db2log.NewReader(ctx, agent, start, sels, ds.CDCURL)
	}
	if err != nil {
		return err
	}
	endpoint := env("QMIGRATION_CDC_ENDPOINT", server+"/api/v1/migrations/"+taskID+"/cdc/events")
	ready := env("QMIGRATION_CDC_READY_ENDPOINT", "")
	token := env("QMIGRATION_API_TOKEN", "")
	client := &http.Client{Timeout: 90 * time.Second}
	total := 0
	return (cdcruntime.Runner{Reader: reader, Gate: func(c context.Context) error { return waitReady(c, client, ready, token) }, Apply: func(c context.Context, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
		return postTx(c, client, endpoint, token, direction, events)
	}, Observe: func(tx *cdcruntime.Transaction, res *domain.CDCApplyResult) {
		if res != nil {
			total += res.Applied
			log.Printf("%s applied=%d total=%d lri=%s", tx.Label, res.Applied, total, tx.Checkpoint.PositionValue)
		}
	}}).Run(ctx)
}
func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
