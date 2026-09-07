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
	"dts/backend/internal/cdc/ddlsidecar"
	postgresconnector "dts/backend/internal/connector/postgres"
	"dts/backend/internal/domain"
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
func envEnabled(k string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(k))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

type readyResponse struct {
	Ready      bool                   `json:"ready"`
	TaskStatus domain.MigrationStatus `json:"task_status"`
}

func waitCDCReady(ctx context.Context, client *http.Client, endpoint, token string) error {
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
				var state readyResponse
				if err := json.Unmarshal(data, &state); err != nil {
					return err
				}
				if state.Ready {
					return nil
				}
			} else {
				err = fmt.Errorf("CDC readiness returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
			}
		}
		if err != nil {
			log.Printf("openGauss CDC readiness check failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func postTransaction(ctx context.Context, client *http.Client, endpoint, token, direction string, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
	body, err := json.Marshal(domain.CDCApplyRequest{Direction: direction, Events: events})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
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

func parseTables(raw string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.Split(item, ".")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(item, "'\" ;\\") {
			return nil, fmt.Errorf("invalid openGauss table %q", item)
		}
		key := strings.ToLower(item)
		if !seen[key] {
			seen[key] = true
			out = append(out, item)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("DTS_OPENGAUSS_TABLES is required")
	}
	return out, nil
}

func run(ctx context.Context) error {
	if !envEnabled("DTS_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC") {
		return errors.New("openGauss CDC requires DTS_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC=1")
	}
	host, user, slot := env("DTS_OPENGAUSS_HOST", ""), env("DTS_OPENGAUSS_USER", ""), env("DTS_OPENGAUSS_SLOT", "")
	if host == "" || user == "" || slot == "" {
		return errors.New("DTS_OPENGAUSS_HOST, USER and SLOT are required")
	}
	port, _ := strconv.Atoi(env("DTS_OPENGAUSS_PORT", "5432"))
	if port <= 0 {
		port = 5432
	}
	tables, err := parseTables(env("DTS_OPENGAUSS_TABLES", ""))
	if err != nil {
		return err
	}
	ds := domain.DataSource{
		Type: domain.DataSourceOpenGauss, Host: host, Port: port, Username: user,
		Password: os.Getenv(env("DTS_OPENGAUSS_PASSWORD_ENV", "OPENGAUSS_PASSWORD")), Database: env("DTS_OPENGAUSS_DATABASE", user),
		TLSMode: domain.TLSMode(env("DTS_OPENGAUSS_TLS_MODE", "REQUIRED")), TLSServerName: env("DTS_OPENGAUSS_TLS_SERVER_NAME", ""),
		TLSCACert:     os.Getenv(env("DTS_OPENGAUSS_TLS_CA_ENV", "DTS_OPENGAUSS_TLS_CA")),
		TLSClientCert: os.Getenv(env("DTS_OPENGAUSS_TLS_CLIENT_CERT_ENV", "DTS_OPENGAUSS_TLS_CLIENT_CERT")),
		TLSClientKey:  os.Getenv(env("DTS_OPENGAUSS_TLS_CLIENT_KEY_ENV", "DTS_OPENGAUSS_TLS_CLIENT_KEY")),
	}
	raw, err := postgresconnector.NewFactory().New(ds)
	if err != nil {
		return err
	}
	pg := raw.(*postgresconnector.Connector)
	defer pg.Close()
	if err := pg.TestConnection(ctx); err != nil {
		return fmt.Errorf("openGauss connection: %w", err)
	}

	server := strings.TrimRight(env("DTS_SERVER", "http://127.0.0.1:8080"), "/")
	taskID := env("DTS_TASK_ID", "")
	if taskID == "" {
		return errors.New("DTS_TASK_ID is required")
	}
	direction := strings.ToLower(env("DTS_CDC_DIRECTION", "forward"))
	if direction != "forward" && direction != "reverse" {
		return errors.New("invalid CDC direction")
	}
	endpoint := env("DTS_CDC_ENDPOINT", server+"/api/v1/migrations/"+taskID+"/cdc/events")
	ready, token := env("DTS_CDC_READY_ENDPOINT", ""), env("DTS_API_TOKEN", "")
	client := &http.Client{Timeout: 90 * time.Second}
	var ddlClient *ddlsidecar.Client
	if envEnabled("DTS_EXPERIMENTAL_OPENGAUSS_DDL_CDC") {
		url := env("DTS_OPENGAUSS_DDL_SIDECAR_URL", "")
		if url == "" {
			return errors.New("openGauss DDL CDC requires DTS_OPENGAUSS_DDL_SIDECAR_URL")
		}
		ddlClient, err = ddlsidecar.New(url, env("DTS_OPENGAUSS_DDL_SIDECAR_TOKEN", ""), env("DTS_OPENGAUSS_DDL_SIDECAR_SERVER_NAME", ""), os.Getenv(env("DTS_OPENGAUSS_DDL_SIDECAR_CA_ENV", "DTS_OPENGAUSS_DDL_SIDECAR_CA")))
		if err != nil {
			return err
		}
	}
	if err := waitCDCReady(ctx, client, ready, token); err != nil {
		return err
	}

	maxChanges, _ := strconv.Atoi(env("DTS_OPENGAUSS_CDC_MAX_CHANGES", "4096"))
	if maxChanges <= 0 {
		maxChanges = 4096
	}
	poll, err := time.ParseDuration(env("DTS_OPENGAUSS_CDC_POLL_INTERVAL", "1s"))
	if err != nil || poll <= 0 {
		poll = time.Second
	}
	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		txs, err := pg.PeekOpenGaussTransactions(ctx, slot, maxChanges, tables)
		if err != nil {
			return fmt.Errorf("peek openGauss logical slot %s: %w", slot, err)
		}
		if len(txs) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(poll):
				continue
			}
		}
		for _, tx := range txs {
			events := tx.Events
			if ddlClient != nil {
				proof, err := ddlClient.Sequence(ctx, ddlsidecar.Request{Product: "opengauss", PositionType: "OPENGAUSS_LSN", PositionValue: tx.CommitLSN, XID: tx.XID, DMLCount: len(events), Tables: tables})
				if err != nil {
					return fmt.Errorf("openGauss DDL proof xid %s at %s: %w", tx.XID, tx.CommitLSN, err)
				}
				events, err = ddlsidecar.Reconstruct(events, proof, tables)
				if err != nil {
					return fmt.Errorf("openGauss DDL reconstruct xid %s at %s: %w", tx.XID, tx.CommitLSN, err)
				}
			}
			result, err := postTransaction(ctx, client, endpoint, token, direction, events)
			if err != nil {
				return fmt.Errorf("apply openGauss xid %s at %s: %w", tx.XID, tx.CommitLSN, err)
			}
			if err := pg.AcknowledgeOpenGaussTransaction(ctx, slot, tx.CommitLSN, tables); err != nil {
				return fmt.Errorf("ack openGauss xid %s at %s: %w", tx.XID, tx.CommitLSN, err)
			}
			if result != nil {
				total += result.Applied
				log.Printf("openGauss xid=%s applied=%d total=%d checkpoint=%s", tx.XID, result.Applied, total, tx.CommitLSN)
			}
		}
	}
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
