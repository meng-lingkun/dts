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

	"dts/backend/internal/cdc/damenglog"
	cdcruntime "dts/backend/internal/cdc/runtime"
	damengconnector "dts/backend/internal/connector/dameng"
	"dts/backend/internal/domain"
)

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
func enabled(k string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(k)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
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
		if wt := strings.TrimSpace(os.Getenv("DTS_WORKER_TOKEN")); wt != "" {
			req.Header.Set("X-DTS-Worker-Token", wt)
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
				log.Printf("Dameng CDC waiting; task status=%s", st.TaskStatus)
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
	if wt := strings.TrimSpace(os.Getenv("DTS_WORKER_TOKEN")); wt != "" {
		req.Header.Set("X-DTS-Worker-Token", wt)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("DTS returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
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
	if !enabled("DTS_EXPERIMENTAL_DAMENG_NATIVE") {
		return errors.New("DTS_EXPERIMENTAL_DAMENG_NATIVE=1 is required")
	}
	if !enabled("DTS_EXPERIMENTAL_DAMENG_LOG_CDC") {
		return errors.New("DTS_EXPERIMENTAL_DAMENG_LOG_CDC=1 is required")
	}
	server := strings.TrimRight(env("DTS_SERVER", "http://127.0.0.1:8080"), "/")
	taskID := env("DTS_TASK_ID", "")
	if taskID == "" {
		return errors.New("DTS_TASK_ID is required")
	}
	direction := strings.ToLower(env("DTS_CDC_DIRECTION", "forward"))
	if direction != "forward" && direction != "reverse" {
		return errors.New("DTS_CDC_DIRECTION must be forward or reverse")
	}
	host, user, start := env("DTS_DAMENG_HOST", ""), env("DTS_DAMENG_USER", ""), env("DTS_DAMENG_START_LSN", "")
	if host == "" || user == "" || start == "" {
		return errors.New("DAMENG_HOST, USER and START_LSN are required")
	}
	port, _ := strconv.Atoi(env("DTS_DAMENG_PORT", "5236"))
	if port <= 0 {
		port = 5236
	}
	ds := domain.DataSource{Type: domain.DataSourceDameng, Host: host, Port: port, Username: user, Password: env("DTS_DAMENG_PASSWORD", ""), Schema: env("DTS_DAMENG_SCHEMA", user), Database: env("DTS_DAMENG_DATABASE", ""), JDBCURL: env("DTS_DAMENG_DSN", ""), DriverClass: env("DTS_DAMENG_SQL_DRIVER", ""), TLSMode: domain.TLSMode(strings.ToUpper(env("DTS_DAMENG_TLS_MODE", "DISABLE")))}
	raw, err := damengconnector.NewFactory().New(ds)
	if err != nil {
		return err
	}
	src, ok := raw.(*damengconnector.Connector)
	if !ok {
		return errors.New("Dameng native connector type assertion failed")
	}
	selected := []string{}
	for _, v := range strings.Split(env("DTS_DAMENG_TABLES", ""), ",") {
		if x := strings.TrimSpace(v); x != "" {
			selected = append(selected, x)
		}
	}
	if len(selected) == 0 {
		return errors.New("DTS_DAMENG_TABLES is required")
	}
	endpoint := env("DTS_CDC_ENDPOINT", server+"/api/v1/migrations/"+taskID+"/cdc/events")
	ready := env("DTS_CDC_READY_ENDPOINT", "")
	token := env("DTS_API_TOKEN", "")
	client := &http.Client{Timeout: 5 * time.Minute}
	if err := waitCDCReady(ctx, client, ready, token, 5*time.Second); err != nil {
		raw.Close()
		return err
	}
	poll, _ := time.ParseDuration(env("DTS_DAMENG_CDC_POLL", "2s"))
	span, _ := strconv.ParseUint(env("DTS_DAMENG_CDC_MAX_LSN_SPAN", "100000"), 10, 64)
	reader, err := damenglog.NewReader(src, start, selected, poll, span)
	if err != nil {
		raw.Close()
		return err
	}
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
