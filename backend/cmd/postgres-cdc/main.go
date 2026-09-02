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
	"qmigration/backend/internal/cdc/ddlsidecar"
	"qmigration/backend/internal/cdc/pgoutput"
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/connector"
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

type ddlProofReader struct {
	inner   cdcruntime.Reader
	client  *ddlsidecar.Client
	product string
	tables  []string
}

func (r *ddlProofReader) Next(ctx context.Context) (*cdcruntime.Transaction, error) {
	tx, err := r.inner.Next(ctx)
	if err != nil || tx == nil {
		return tx, err
	}
	xid := strings.TrimPrefix(strings.TrimSpace(tx.Label), "xid=")
	proof, err := r.client.Sequence(ctx, ddlsidecar.Request{Product: r.product, PositionType: tx.Checkpoint.PositionType, PositionValue: tx.Checkpoint.PositionValue, XID: xid, DMLCount: len(tx.Events), Tables: r.tables})
	if err != nil {
		return nil, fmt.Errorf("DDL sidecar proof at %s: %w", tx.Checkpoint.PositionValue, err)
	}
	events, err := ddlsidecar.Reconstruct(tx.Events, proof, r.tables)
	if err != nil {
		return nil, fmt.Errorf("DDL sidecar reconstruct at %s: %w", tx.Checkpoint.PositionValue, err)
	}
	for i := range events {
		if events[i].PositionType == "" {
			events[i].PositionType = tx.Checkpoint.PositionType
		}
		if events[i].PositionValue == "" {
			events[i].PositionValue = tx.Checkpoint.PositionValue
		}
	}
	tx.Events = events
	return tx, nil
}
func (r *ddlProofReader) Acknowledge(ctx context.Context, tx *cdcruntime.Transaction) error {
	return r.inner.Acknowledge(ctx, tx)
}
func (r *ddlProofReader) Close() error { return r.inner.Close() }

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
		if workerToken := strings.TrimSpace(os.Getenv("QMIGRATION_WORKER_TOKEN")); workerToken != "" {
			req.Header.Set("X-QMigration-Worker-Token", workerToken)
		}
		resp, err := client.Do(req)
		if err == nil {
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				var state readyResponse
				if err := json.Unmarshal(data, &state); err != nil {
					return fmt.Errorf("decode CDC readiness: %w", err)
				}
				if state.Ready {
					log.Printf("CDC reader released at task status %s", state.TaskStatus)
					return nil
				}
				log.Printf("CDC reader waiting for full load/cutover stage; task status=%s", state.TaskStatus)
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
	if workerToken := strings.TrimSpace(os.Getenv("QMIGRATION_WORKER_TOKEN")); workerToken != "" {
		req.Header.Set("X-QMigration-Worker-Token", workerToken)
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
	server := strings.TrimRight(env("QMIGRATION_SERVER", "http://127.0.0.1:8080"), "/")
	taskID := env("QMIGRATION_TASK_ID", "")
	if taskID == "" {
		return errors.New("QMIGRATION_TASK_ID is required")
	}
	direction := strings.ToLower(env("QMIGRATION_CDC_DIRECTION", "forward"))
	if direction != "forward" && direction != "reverse" {
		return errors.New("QMIGRATION_CDC_DIRECTION must be forward or reverse")
	}
	host := env("QMIGRATION_PG_HOST", "")
	user := env("QMIGRATION_PG_USER", "")
	slot := env("QMIGRATION_PG_SLOT", "")
	publication := env("QMIGRATION_PG_PUBLICATION", "")
	startLSN := env("QMIGRATION_PG_START_LSN", "")
	if host == "" || user == "" || slot == "" || publication == "" || startLSN == "" {
		return errors.New("QMIGRATION_PG_HOST, USER, SLOT, PUBLICATION and START_LSN are required")
	}
	port, _ := strconv.Atoi(env("QMIGRATION_PG_PORT", "5432"))
	if port <= 0 {
		port = 5432
	}
	sourceType := domain.DataSourceType(strings.ToLower(env("QMIGRATION_PG_SOURCE_TYPE", string(domain.DataSourcePostgreSQL))))
	switch sourceType {
	case domain.DataSourcePostgreSQL, domain.DataSourcePolarDBPostgreSQL:
	case domain.DataSourceKingbase:
		if !envEnabled("QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC") {
			return errors.New("KingbaseES CDC requires QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC=1")
		}
	default:
		return fmt.Errorf("qmigration-postgres-cdc does not support source type %s", sourceType)
	}
	ds := domain.DataSource{
		Type: sourceType, Host: host, Port: port, Username: user,
		Password: os.Getenv(env("QMIGRATION_PG_PASSWORD_ENV", "PGPASSWORD")), Database: env("QMIGRATION_PG_DATABASE", user),
		TLSMode: domain.TLSMode(env("QMIGRATION_PG_TLS_MODE", "PREFERRED")), TLSServerName: env("QMIGRATION_PG_TLS_SERVER_NAME", ""),
		TLSCACert:     os.Getenv(env("QMIGRATION_PG_TLS_CA_ENV", "QMIGRATION_PG_TLS_CA")),
		TLSClientCert: os.Getenv(env("QMIGRATION_PG_TLS_CLIENT_CERT_ENV", "QMIGRATION_PG_TLS_CLIENT_CERT")),
		TLSClientKey:  os.Getenv(env("QMIGRATION_PG_TLS_CLIENT_KEY_ENV", "QMIGRATION_PG_TLS_CLIENT_KEY")),
	}
	positionType, idPrefix := "LSN", "pg"
	if sourceType == domain.DataSourceKingbase {
		positionType, idPrefix = "KINGBASE_LSN", "kingbase"
	}
	endpoint := env("QMIGRATION_CDC_ENDPOINT", server+"/api/v1/migrations/"+taskID+"/cdc/events")
	readyEndpoint := env("QMIGRATION_CDC_READY_ENDPOINT", "")
	token := env("QMIGRATION_API_TOKEN", "")
	client := &http.Client{Timeout: 90 * time.Second}
	retryDelay := time.Second
	if d, err := time.ParseDuration(env("QMIGRATION_CDC_RETRY_DELAY", "1s")); err == nil && d > 0 {
		retryDelay = d
	}
	publicationTables := []string{}
	for _, item := range strings.Split(env("QMIGRATION_PG_PUBLICATION_TABLES", ""), ",") {
		if v := strings.TrimSpace(item); v != "" {
			publicationTables = append(publicationTables, v)
		}
	}
	currentLSN := startLSN
	total := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		raw, err := postgresconnector.NewFactory().New(ds)
		if err != nil {
			return err
		}
		if pg, ok := raw.(*postgresconnector.Connector); ok {
			expectedPlugin := "pgoutput"
			if sourceType == domain.DataSourceKingbase {
				expectedPlugin = "kboutput"
			}
			if err := pg.ValidateLogicalSlotPlugin(ctx, slot, expectedPlugin); err != nil {
				raw.Close()
				return fmt.Errorf("validate logical slot plugin: %w", err)
			}
			if len(publicationTables) > 0 {
				if err := pg.EnsurePublication(ctx, publication, publicationTables); err != nil {
					raw.Close()
					return fmt.Errorf("ensure publication: %w", err)
				}
			}
		}
		if err := waitCDCReady(ctx, client, readyEndpoint, token, 5*time.Second); err != nil {
			raw.Close()
			return err
		}
		source, ok := raw.(connector.PostgreSQLLogicalSource)
		if !ok {
			raw.Close()
			return errors.New("PostgreSQL connector does not expose logical replication")
		}
		stream, err := source.OpenLogicalReplicationStream(ctx, slot, currentLSN, publication)
		if err != nil {
			raw.Close()
			log.Printf("open logical stream failed at %s: %v", currentLSN, err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
				continue
			}
		}
		var reader cdcruntime.Reader = pgoutput.NewReaderWithDialect(stream, positionType, idPrefix)
		if sourceType == domain.DataSourceKingbase && envEnabled("QMIGRATION_EXPERIMENTAL_KINGBASE_DDL_CDC") {
			url := env("QMIGRATION_KINGBASE_DDL_SIDECAR_URL", "")
			if url == "" {
				stream.Close()
				raw.Close()
				return errors.New("Kingbase DDL CDC requires QMIGRATION_KINGBASE_DDL_SIDECAR_URL")
			}
			ddlClient, err := ddlsidecar.New(url, env("QMIGRATION_KINGBASE_DDL_SIDECAR_TOKEN", ""), env("QMIGRATION_KINGBASE_DDL_SIDECAR_SERVER_NAME", ""), os.Getenv(env("QMIGRATION_KINGBASE_DDL_SIDECAR_CA_ENV", "QMIGRATION_KINGBASE_DDL_SIDECAR_CA")))
			if err != nil {
				stream.Close()
				raw.Close()
				return err
			}
			reader = &ddlProofReader{inner: reader, client: ddlClient, product: "kingbase", tables: publicationTables}
		}
		streamErr := (cdcruntime.Runner{
			Reader: reader,
			Apply: func(applyCtx context.Context, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
				return postTransaction(applyCtx, client, endpoint, token, direction, events)
			},
			Observe: func(tx *cdcruntime.Transaction, result *domain.CDCApplyResult) {
				currentLSN = tx.Checkpoint.PositionValue
				if result != nil {
					total += result.Applied
					log.Printf("%s applied=%d total=%d checkpoint=%s", tx.Label, result.Applied, total, currentLSN)
				}
			},
		}).Run(ctx)
		_ = raw.Close()
		if streamErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("logical stream interrupted at acknowledged LSN %s: %v; reconnecting", currentLSN, streamErr)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
		}
	}
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
