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
	postgresconnector "qmigration/backend/internal/connector/postgres"
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
		if wt := strings.TrimSpace(os.Getenv("QMIGRATION_WORKER_TOKEN")); wt != "" {
			req.Header.Set("X-QMigration-Worker-Token", wt)
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
			log.Printf("GaussDB CDC readiness check failed: %v", err)
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

func parseTables(raw string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.Split(item, ".")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid GaussDB table %q", item)
		}
		k := strings.ToLower(item)
		if !seen[k] {
			seen[k] = true
			out = append(out, item)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("QMIGRATION_GAUSSDB_TABLES is required")
	}
	return out, nil
}

func run(ctx context.Context) error {
	if !envEnabled("QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE") || !envEnabled("QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC") {
		return errors.New("GaussDB CDC requires QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE=1 and QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC=1")
	}
	if rawMulti := strings.TrimSpace(os.Getenv("QMIGRATION_GAUSSDB_PRIMARIES_JSON")); rawMulti != "" {
		return runMultiPrimary(ctx, rawMulti)
	}
	host := env("QMIGRATION_GAUSSDB_HOST", "")
	user := env("QMIGRATION_GAUSSDB_USER", "")
	slot := env("QMIGRATION_GAUSSDB_SLOT", "")
	if host == "" || user == "" || slot == "" {
		return errors.New("QMIGRATION_GAUSSDB_HOST, USER and SLOT are required")
	}
	port, _ := strconv.Atoi(env("QMIGRATION_GAUSSDB_PORT", "8000"))
	if port <= 0 {
		port = 8000
	}
	tables, err := parseTables(env("QMIGRATION_GAUSSDB_TABLES", ""))
	if err != nil {
		return err
	}
	ds := domain.DataSource{
		Type: domain.DataSourceGaussDB, Host: host, Port: port, Username: user,
		Password:      os.Getenv(env("QMIGRATION_GAUSSDB_PASSWORD_ENV", "GAUSSDB_PASSWORD")),
		Database:      env("QMIGRATION_GAUSSDB_DATABASE", user),
		TLSMode:       domain.TLSMode(env("QMIGRATION_GAUSSDB_TLS_MODE", "DISABLE")),
		TLSServerName: env("QMIGRATION_GAUSSDB_TLS_SERVER_NAME", ""),
		TLSCACert:     os.Getenv(env("QMIGRATION_GAUSSDB_TLS_CA_ENV", "QMIGRATION_GAUSSDB_TLS_CA")),
		TLSClientCert: os.Getenv(env("QMIGRATION_GAUSSDB_TLS_CLIENT_CERT_ENV", "QMIGRATION_GAUSSDB_TLS_CLIENT_CERT")),
		TLSClientKey:  os.Getenv(env("QMIGRATION_GAUSSDB_TLS_CLIENT_KEY_ENV", "QMIGRATION_GAUSSDB_TLS_CLIENT_KEY")),
	}
	raw, err := postgresconnector.NewFactory().New(ds)
	if err != nil {
		return err
	}
	pg := raw.(*postgresconnector.Connector)
	defer pg.Close()
	if err := pg.TestConnection(ctx); err != nil {
		return fmt.Errorf("GaussDB connection: %w", err)
	}

	server := strings.TrimRight(env("QMIGRATION_SERVER", "http://127.0.0.1:8080"), "/")
	taskID := env("QMIGRATION_TASK_ID", "")
	if taskID == "" {
		return errors.New("QMIGRATION_TASK_ID is required")
	}
	direction := strings.ToLower(env("QMIGRATION_CDC_DIRECTION", "forward"))
	if direction != "forward" && direction != "reverse" {
		return errors.New("invalid CDC direction")
	}
	endpoint := env("QMIGRATION_CDC_ENDPOINT", server+"/api/v1/migrations/"+taskID+"/cdc/events")
	ready := env("QMIGRATION_CDC_READY_ENDPOINT", "")
	token := env("QMIGRATION_API_TOKEN", "")
	client := &http.Client{Timeout: 90 * time.Second}
	if err := waitCDCReady(ctx, client, ready, token); err != nil {
		return err
	}

	ddlReplay := envEnabled("QMIGRATION_GAUSSDB_DDL_REPLAY")
	// RC49 supports selected-table hybrid DDL+DML by using the text decoding
	// pass only as an ordered DDL/DML template and filling every DML placeholder
	// from the byte-safe binary pass. Cardinality/order disagreement fails closed.
	if ddlReplay {
		r, err := pg.QuerySQL(ctx, "SHOW enable_logical_replication_ddl")
		if err != nil {
			return fmt.Errorf("check GaussDB enable_logical_replication_ddl: %w", err)
		}
		if len(r.Rows) == 0 || len(r.Rows[0]) == 0 {
			return errors.New("GaussDB enable_logical_replication_ddl returned no value")
		}
		v := strings.ToLower(strings.TrimSpace(string(r.Rows[0][0])))
		if v != "on" && v != "true" && v != "1" {
			return fmt.Errorf("GaussDB DDL replay requires enable_logical_replication_ddl=on, got %q", v)
		}
	}

	maxChanges, _ := strconv.Atoi(env("QMIGRATION_GAUSSDB_CDC_MAX_CHANGES", "4096"))
	if maxChanges <= 0 {
		maxChanges = 4096
	}
	poll, err := time.ParseDuration(env("QMIGRATION_GAUSSDB_CDC_POLL_INTERVAL", "1s"))
	if err != nil || poll <= 0 {
		poll = time.Second
	}
	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var txs []postgresconnector.GaussDBTransaction
		if ddlReplay {
			txs, err = pg.PeekGaussDBTransactionsWithDDL(ctx, slot, maxChanges, tables)
		} else {
			txs, err = pg.PeekGaussDBTransactions(ctx, slot, maxChanges, tables)
		}
		if err != nil {
			return fmt.Errorf("peek GaussDB logical slot %s: %w", slot, err)
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
			result, err := postTransaction(ctx, client, endpoint, token, direction, tx.Events)
			if err != nil {
				return fmt.Errorf("apply GaussDB xid %s at %s: %w", tx.XID, tx.CommitLSN, err)
			}
			if err := pg.AcknowledgeGaussDBDecodedTransaction(ctx, slot, tx, tables); err != nil {
				return fmt.Errorf("ack GaussDB xid %s at %s: %w", tx.XID, tx.CommitLSN, err)
			}
			if result != nil {
				total += result.Applied
				log.Printf("GaussDB xid=%s applied=%d total=%d checkpoint=%s", tx.XID, result.Applied, total, tx.CommitLSN)
			}
		}
	}
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
