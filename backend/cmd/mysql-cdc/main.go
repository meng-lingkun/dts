package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"qmigration/backend/internal/cdc/mysqlbinlog"
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/connector"
	mysqlconnector "qmigration/backend/internal/connector/mysql"
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

func addAuth(req *http.Request, token string) {
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if v := strings.TrimSpace(os.Getenv("QMIGRATION_WORKER_TOKEN")); v != "" {
		req.Header.Set("X-QMigration-Worker-Token", v)
	}
}

func waitReady(ctx context.Context, client *http.Client, endpoint, token string) error {
	if endpoint == "" {
		return nil
	}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
		if err != nil {
			return err
		}
		addAuth(req, token)
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
					log.Printf("native MySQL CDC released at task status %s", state.TaskStatus)
					return nil
				}
				log.Printf("native MySQL CDC waiting; task status=%s", state.TaskStatus)
			} else {
				log.Printf("CDC readiness returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
			}
		} else {
			log.Printf("CDC readiness failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func postEvents(ctx context.Context, client *http.Client, endpoint, token, direction string, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
	body, err := json.Marshal(domain.CDCApplyRequest{Direction: direction, Events: events})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	addAuth(req, token)
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

func selectedSet(raw string) map[string]bool {
	out := map[string]bool{}
	for _, v := range strings.Split(raw, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out[strings.ToLower(v)] = true
		}
	}
	return out
}

func ddlQuery(q *mysqlbinlog.Query) bool {
	if q == nil {
		return false
	}
	sql := strings.ToUpper(strings.TrimSpace(q.SQL))
	for _, p := range []string{"ALTER ", "CREATE ", "DROP ", "TRUNCATE ", "RENAME "} {
		if strings.HasPrefix(sql, p) {
			return true
		}
	}
	return false
}

func ddlTouchesSelected(q *mysqlbinlog.Query, selected map[string]bool) bool {
	if q == nil {
		return false
	}
	if len(selected) == 0 {
		return true
	}
	schema := strings.ToLower(strings.TrimSpace(q.Schema))
	sql := strings.ToLower(q.SQL)
	for key := range selected {
		i := strings.LastIndex(key, ".")
		if i <= 0 {
			continue
		}
		ks, table := key[:i], key[i+1:]
		if schema != "" && ks != schema {
			continue
		}
		// MySQL DDL commonly quotes identifiers with backticks. Checking both
		// quoted and bare forms is conservative: false positives stop/replay a DDL
		// rather than silently skipping a potentially relevant schema change.
		if strings.Contains(sql, "`"+table+"`") || strings.Contains(sql, table) {
			return true
		}
	}
	return false
}

func validateNativeFields(fields []domain.CDCField) error {
	for _, f := range fields {
		switch strings.ToLower(strings.TrimSpace(f.Encoding)) {
		case "", "text", "utf8", "base64", "json":
		default:
			return fmt.Errorf("column %s uses unsupported native CDC encoding %q", f.Column, f.Encoding)
		}
	}
	return nil
}

func decodeTransaction(tx *mysqlbinlog.Transaction, maps map[uint64]*mysqlbinlog.TableMap, metas map[uint64]*domain.TableMetadata, selected map[string]bool) ([]domain.CDCEvent, error) {
	out := []domain.CDCEvent{}
	for _, ev := range tx.Events {
		rows, err := mysqlbinlog.ParseRows(ev)
		if err != nil {
			return nil, err
		}
		tm := maps[rows.TableID]
		md := metas[rows.TableID]
		if tm == nil || md == nil {
			return nil, fmt.Errorf("missing TABLE_MAP/metadata for table id %d", rows.TableID)
		}
		key := strings.ToLower(tm.Schema + "." + tm.Table)
		if len(selected) > 0 && !selected[key] {
			continue
		}
		changes, err := mysqlbinlog.DecodeRows(tm, rows, md.Columns)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", key, err)
		}
		for _, ch := range changes {
			if err := validateNativeFields(ch.Before); err != nil {
				return nil, err
			}
			if err := validateNativeFields(ch.After); err != nil {
				return nil, err
			}
			op := domain.CDCInsert
			switch ev.Header.Type {
			case mysqlbinlog.UpdateRowsEventV1, mysqlbinlog.UpdateRowsEventV2, mysqlbinlog.PartialUpdateRowsEvent:
				op = domain.CDCUpdate
			case mysqlbinlog.DeleteRowsEventV1, mysqlbinlog.DeleteRowsEventV2:
				op = domain.CDCDelete
			}
			out = append(out, domain.CDCEvent{Operation: op, SourceSchema: tm.Schema, SourceTable: tm.Table, Before: ch.Before, After: ch.After, SourceTimestampMS: int64(ev.Header.Timestamp) * 1000})
		}
	}
	if len(out) > 0 {
		pos := tx.Position()
		last := &out[len(out)-1]
		last.PositionType = "BINLOG"
		last.PositionValue = pos
		last.Resource = tx.File
	}
	return out, nil
}

type replicationEndpoint struct {
	Host string
	Port int
}

func (e replicationEndpoint) String() string { return net.JoinHostPort(e.Host, strconv.Itoa(e.Port)) }

func replicationEndpoints(primaryHost string, primaryPort int, fallbacks string) ([]replicationEndpoint, error) {
	out := []replicationEndpoint{{Host: primaryHost, Port: primaryPort}}
	seen := map[string]bool{out[0].String(): true}
	for _, raw := range strings.Split(fallbacks, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		host, portRaw, err := net.SplitHostPort(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid replication failover endpoint %q: %w", raw, err)
		}
		port, err := strconv.Atoi(portRaw)
		if err != nil || port < 1 || port > 65535 || strings.TrimSpace(host) == "" {
			return nil, fmt.Errorf("invalid replication failover endpoint %q", raw)
		}
		ep := replicationEndpoint{Host: strings.TrimSpace(host), Port: port}
		if seen[ep.String()] {
			continue
		}
		seen[ep.String()] = true
		out = append(out, ep)
		if len(out) > 8 {
			return nil, errors.New("at most 8 replication endpoints are supported")
		}
	}
	return out, nil
}

func splitPosition(v string) (string, uint32, error) {
	i := strings.LastIndex(v, ":")
	if i <= 0 {
		return "", 0, fmt.Errorf("invalid binlog position %q", v)
	}
	p, err := strconv.ParseUint(v[i+1:], 10, 32)
	if err != nil {
		return "", 0, err
	}
	return v[:i], uint32(p), nil
}

func run(ctx context.Context) error {
	server := strings.TrimRight(env("QMIGRATION_SERVER", "http://127.0.0.1:8080"), "/")
	taskID := env("QMIGRATION_TASK_ID", "")
	if taskID == "" {
		return errors.New("QMIGRATION_TASK_ID is required")
	}
	direction := strings.ToLower(env("QMIGRATION_CDC_DIRECTION", "forward"))
	if direction != "forward" && direction != "reverse" {
		return errors.New("invalid CDC direction")
	}
	host, user := env("QMIGRATION_MYSQL_HOST", ""), env("QMIGRATION_MYSQL_USER", "")
	if host == "" || user == "" {
		return errors.New("MYSQL host/user are required")
	}
	port, _ := strconv.Atoi(env("QMIGRATION_MYSQL_PORT", "3306"))
	if port <= 0 {
		port = 3306
	}
	startGTID := env("QMIGRATION_MYSQL_START_GTID", "")
	startFile := env("QMIGRATION_MYSQL_START_FILE", "")
	startPos64 := uint64(4)
	var err error
	if startGTID == "" {
		startPos64, err = strconv.ParseUint(env("QMIGRATION_MYSQL_START_POS", "4"), 10, 32)
		if err != nil {
			return err
		}
		if startFile == "" {
			return errors.New("QMIGRATION_MYSQL_START_GTID or QMIGRATION_MYSQL_START_FILE is required")
		}
	}
	var executed *mysqlbinlog.GTIDSet
	if startGTID != "" {
		executed, err = mysqlbinlog.ParseGTIDSet(startGTID)
		if err != nil {
			return fmt.Errorf("parse QMIGRATION_MYSQL_START_GTID: %w", err)
		}
	}
	serverID64, _ := strconv.ParseUint(env("QMIGRATION_MYSQL_SERVER_ID", fmt.Sprint(700000+os.Getpid()%200000)), 10, 32)
	sourceType := domain.DataSourceType(env("QMIGRATION_MYSQL_SOURCE_TYPE", string(domain.DataSourceMySQL)))
	if !sourceType.IsMySQLFamily() {
		return fmt.Errorf("invalid MySQL CDC source type %q", sourceType)
	}
	ds := domain.DataSource{
		Type: sourceType, Host: host, Port: port, Username: user,
		Password:      os.Getenv(env("QMIGRATION_MYSQL_PASSWORD_ENV", "MYSQL_PWD")),
		Database:      env("QMIGRATION_MYSQL_DATABASE", ""),
		CDCURL:        env("QMIGRATION_MYSQL_CDC_URL", ""),
		TLSMode:       domain.TLSMode(strings.ToUpper(env("QMIGRATION_MYSQL_TLS_MODE", string(domain.TLSModeDisable)))),
		TLSServerName: env("QMIGRATION_MYSQL_TLS_SERVER_NAME", ""),
		TLSCACert:     env("QMIGRATION_MYSQL_TLS_CA", ""),
		TLSClientCert: env("QMIGRATION_MYSQL_TLS_CLIENT_CERT", ""),
		TLSClientKey:  env("QMIGRATION_MYSQL_TLS_CLIENT_KEY", ""),
	}
	replEndpoints, err := replicationEndpoints(host, port, env("QMIGRATION_MYSQL_FAILOVER_ENDPOINTS", ""))
	if err != nil {
		return err
	}
	endpointIndex := 0
	endpoint := env("QMIGRATION_CDC_ENDPOINT", server+"/api/v1/migrations/"+taskID+"/cdc/events")
	readyEndpoint := env("QMIGRATION_CDC_READY_ENDPOINT", "")
	token := env("QMIGRATION_API_TOKEN", "")
	selected := selectedSet(env("QMIGRATION_MYSQL_TABLES", ""))
	zstdBin := env("QMIGRATION_ZSTD_BIN", "zstd")
	client := &http.Client{Timeout: 90 * time.Second}
	retryDelay := time.Second
	currentFile, currentPos := startFile, uint32(startPos64)
	for {
		activeEndpoint := replEndpoints[endpointIndex%len(replEndpoints)]
		ds.Host, ds.Port = activeEndpoint.Host, activeEndpoint.Port
		if err := waitReady(ctx, client, readyEndpoint, token); err != nil {
			return err
		}
		raw, err := mysqlconnector.NewFactory().New(ds)
		if err != nil {
			return err
		}
		if inspector, ok := raw.(connector.MigrationPrecheckConnector); ok {
			for _, item := range inspector.MigrationPrechecks(ctx, true) {
				if item.Name == "mysql_binlog_transaction_compression" && item.Level != domain.PrecheckPass {
					if _, err := exec.LookPath(zstdBin); err != nil {
						raw.Close()
						return fmt.Errorf("source uses binlog transaction compression but zstd decoder %q is unavailable on this worker: %w", zstdBin, err)
					}
				}
			}
		}
		source, ok := raw.(connector.MySQLBinlogSource)
		if !ok {
			raw.Close()
			return errors.New("MySQL connector does not expose binlog streaming")
		}
		var stream connector.RawCDCStream
		if executed != nil {
			stream, err = source.OpenBinlogGTIDStream(ctx, executed.String(), uint32(serverID64))
		} else {
			stream, err = source.OpenBinlogStream(ctx, currentFile, currentPos, uint32(serverID64))
		}
		if err != nil {
			raw.Close()
			checkpoint := fmt.Sprintf("%s:%d", currentFile, currentPos)
			if executed != nil {
				checkpoint = "GTID " + executed.String()
			}
			log.Printf("open binlog from %s via %s failed: %v", checkpoint, activeEndpoint.String(), err)
			endpointIndex = (endpointIndex + 1) % len(replEndpoints)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
				continue
			}
		}
		nativeReader := mysqlbinlog.NewNativeReader(stream, raw, mysqlbinlog.ReaderState{File: currentFile, Position: currentPos, Executed: executed}, selected, zstdBin)
		streamErr := (cdcruntime.Runner{
			Reader: nativeReader,
			Apply: func(applyCtx context.Context, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
				return postEvents(applyCtx, client, endpoint, token, direction, events)
			},
			Observe: func(tx *cdcruntime.Transaction, result *domain.CDCApplyResult) {
				state := nativeReader.State()
				currentFile, currentPos, executed = state.File, state.Position, state.Executed
				applied := 0
				if result != nil {
					applied = result.Applied
				}
				log.Printf("%s applied=%d checkpoint=%s", tx.Label, applied, tx.Checkpoint.PositionValue)
			},
		}).Run(ctx)
		_ = raw.Close()
		if streamErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		checkpoint := fmt.Sprintf("%s:%d", currentFile, currentPos)
		if executed != nil {
			checkpoint = "GTID " + executed.String()
		}
		log.Printf("binlog stream via %s interrupted at acknowledged %s: %v; reconnecting", activeEndpoint.String(), checkpoint, streamErr)
		endpointIndex = (endpointIndex + 1) % len(replEndpoints)
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

var _ = splitPosition
