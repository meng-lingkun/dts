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
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/cdc/sqlservercdc"
	sqlserverconnector "qmigration/backend/internal/connector/sqlserver"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
	"time"
)

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}

type readyResponse struct {
	Ready      bool                   `json:"ready"`
	TaskStatus domain.MigrationStatus `json:"task_status"`
}

func waitCDCReady(ctx context.Context, client *http.Client, endpoint, token string, interval time.Duration) error {
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
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				var st readyResponse
				if e := json.Unmarshal(data, &st); e != nil {
					return e
				}
				if st.Ready {
					return nil
				}
				log.Printf("SQL Server CDC waiting; task status=%s", st.TaskStatus)
			} else {
				err = fmt.Errorf("CDC readiness returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
			}
		}
		if err != nil {
			log.Printf("CDC readiness check failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}
func postTransaction(ctx context.Context, client *http.Client, endpoint, token, direction string, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
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
	if v := strings.ToLower(env("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "")); v != "1" && v != "true" && v != "yes" && v != "on" {
		return errors.New("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE=1 is required")
	}
	if v := strings.ToLower(env("QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC", "")); v != "1" && v != "true" && v != "yes" && v != "on" {
		return errors.New("QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC=1 is required")
	}
	server := strings.TrimRight(env("QMIGRATION_SERVER", "http://127.0.0.1:8080"), "/")
	taskID := env("QMIGRATION_TASK_ID", "")
	if taskID == "" {
		return errors.New("QMIGRATION_TASK_ID is required")
	}
	direction := strings.ToLower(env("QMIGRATION_CDC_DIRECTION", "forward"))
	if direction != "forward" && direction != "reverse" {
		return errors.New("QMIGRATION_CDC_DIRECTION must be forward or reverse")
	}
	host := env("QMIGRATION_SQLSERVER_HOST", "")
	user := env("QMIGRATION_SQLSERVER_USER", "")
	start := env("QMIGRATION_SQLSERVER_START_LSN", "")
	if host == "" || user == "" || start == "" {
		return errors.New("SQLSERVER_HOST, USER and START_LSN are required")
	}
	port, _ := strconv.Atoi(env("QMIGRATION_SQLSERVER_PORT", "1433"))
	if port <= 0 {
		port = 1433
	}
	ds := domain.DataSource{Type: domain.DataSourceSQLServer, Host: host, Port: port, Username: user, Password: env("QMIGRATION_SQLSERVER_PASSWORD", ""), Database: env("QMIGRATION_SQLSERVER_DATABASE", "master"), TLSMode: domain.TLSMode(strings.ToUpper(env("QMIGRATION_SQLSERVER_TLS_MODE", "PREFERRED"))), TLSServerName: env("QMIGRATION_SQLSERVER_TLS_SERVER_NAME", ""), TLSCACert: os.Getenv("QMIGRATION_SQLSERVER_TLS_CA"), TLSClientCert: os.Getenv("QMIGRATION_SQLSERVER_TLS_CLIENT_CERT"), TLSClientKey: os.Getenv("QMIGRATION_SQLSERVER_TLS_CLIENT_KEY")}
	raw, err := sqlserverconnector.NewFactory().New(ds)
	if err != nil {
		return err
	}
	src, ok := raw.(*sqlserverconnector.Connector)
	if !ok {
		return errors.New("SQL Server native connector type assertion failed")
	}
	selected := []string{}
	for _, v := range strings.Split(env("QMIGRATION_SQLSERVER_TABLES", ""), ",") {
		if x := strings.TrimSpace(v); x != "" {
			selected = append(selected, x)
		}
	}
	if len(selected) == 0 {
		return errors.New("QMIGRATION_SQLSERVER_TABLES is required")
	}
	endpoint := env("QMIGRATION_CDC_ENDPOINT", server+"/api/v1/migrations/"+taskID+"/cdc/events")
	ready := env("QMIGRATION_CDC_READY_ENDPOINT", "")
	token := env("QMIGRATION_API_TOKEN", "")
	client := &http.Client{Timeout: 90 * time.Second}
	if err := waitCDCReady(ctx, client, ready, token, 5*time.Second); err != nil {
		raw.Close()
		return err
	}
	poll, _ := time.ParseDuration(env("QMIGRATION_SQLSERVER_CDC_POLL", "1s"))
	maxTx, _ := strconv.Atoi(env("QMIGRATION_SQLSERVER_CDC_MAX_TRANSACTIONS", "256"))
	reader := sqlservercdc.NewReader(src, start, selected, poll, maxTx)
	total := 0
	return (cdcruntime.Runner{Reader: reader, Apply: func(c context.Context, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
		return postTransaction(c, client, endpoint, token, direction, events)
	}, Observe: func(tx *cdcruntime.Transaction, res *domain.CDCApplyResult) {
		if res != nil {
			total += res.Applied
			log.Printf("%s applied=%d total=%d", tx.Label, res.Applied, total)
		}
	}}).Run(ctx)
}
func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
