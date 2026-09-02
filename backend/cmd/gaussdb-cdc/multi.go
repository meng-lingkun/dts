package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"qmigration/backend/internal/cdc/orderedmerge"
	postgresconnector "qmigration/backend/internal/connector/postgres"
	"qmigration/backend/internal/domain"
	"sort"
	"strconv"
	"strings"
	"time"
)

type gaussPrimaryConfig struct {
	ID               string `json:"id"`
	Host             string `json:"host"`
	Port             int    `json:"port"`
	User             string `json:"user"`
	PasswordEnv      string `json:"password_env"`
	Database         string `json:"database"`
	Slot             string `json:"slot"`
	ResolvedCSNQuery string `json:"resolved_csn_query"`
	TLSMode          string `json:"tls_mode,omitempty"`
	TLSServerName    string `json:"tls_server_name,omitempty"`
	TLSCAEnv         string `json:"tls_ca_env,omitempty"`
}

type gaussVector struct {
	CSN uint64            `json:"csn"`
	LSN map[string]string `json:"lsn"`
}

func encodeGaussVector(v gaussVector) string { b, _ := json.Marshal(v); return string(b) }
func decodeGaussVector(raw string) (gaussVector, error) {
	var v gaussVector
	if strings.TrimSpace(raw) == "" {
		v.LSN = map[string]string{}
		return v, nil
	}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return v, fmt.Errorf("invalid GAUSSDB_CSN_VECTOR: %w", err)
	}
	if v.LSN == nil {
		v.LSN = map[string]string{}
	}
	return v, nil
}

type gaussPrimary struct {
	cfg gaussPrimaryConfig
	c   *postgresconnector.Connector
}

func loadGaussPrimaries(raw string) ([]gaussPrimaryConfig, error) {
	var cfg []gaussPrimaryConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, err
	}
	if len(cfg) < 2 {
		return nil, errors.New("GaussDB multi-primary requires at least two primaries")
	}
	seen := map[string]bool{}
	for i := range cfg {
		cfg[i].ID = strings.TrimSpace(cfg[i].ID)
		if cfg[i].ID == "" || seen[cfg[i].ID] {
			return nil, fmt.Errorf("invalid/duplicate primary id %q", cfg[i].ID)
		}
		seen[cfg[i].ID] = true
		if cfg[i].Host == "" || cfg[i].User == "" || cfg[i].Slot == "" || strings.TrimSpace(cfg[i].ResolvedCSNQuery) == "" {
			return nil, fmt.Errorf("primary %s requires host/user/slot/resolved_csn_query", cfg[i].ID)
		}
		if strings.Contains(cfg[i].ResolvedCSNQuery, ";") {
			return nil, fmt.Errorf("primary %s resolved_csn_query must be one read-only statement", cfg[i].ID)
		}
		if cfg[i].Port <= 0 {
			cfg[i].Port = 8000
		}
		if cfg[i].Database == "" {
			cfg[i].Database = cfg[i].User
		}
	}
	sort.Slice(cfg, func(i, j int) bool { return cfg[i].ID < cfg[j].ID })
	return cfg, nil
}
func openGaussPrimaries(ctx context.Context, cfgs []gaussPrimaryConfig) ([]gaussPrimary, error) {
	out := make([]gaussPrimary, 0, len(cfgs))
	for _, cfg := range cfgs {
		ds := domain.DataSource{Type: domain.DataSourceGaussDB, Host: cfg.Host, Port: cfg.Port, Username: cfg.User, Password: os.Getenv(func() string {
			if strings.TrimSpace(cfg.PasswordEnv) != "" {
				return cfg.PasswordEnv
			}
			return "GAUSSDB_PASSWORD"
		}()), Database: cfg.Database, TLSMode: domain.TLSMode(cfg.TLSMode), TLSServerName: cfg.TLSServerName}
		if cfg.TLSCAEnv != "" {
			ds.TLSCACert = os.Getenv(cfg.TLSCAEnv)
		}
		raw, err := postgresconnector.NewFactory().New(ds)
		if err != nil {
			return nil, err
		}
		c := raw.(*postgresconnector.Connector)
		if err := c.TestConnection(ctx); err != nil {
			c.Close()
			return nil, fmt.Errorf("primary %s: %w", cfg.ID, err)
		}
		out = append(out, gaussPrimary{cfg: cfg, c: c})
	}
	return out, nil
}
func resolvedCSN(ctx context.Context, p gaussPrimary) (uint64, error) {
	r, err := p.c.QuerySQL(ctx, p.cfg.ResolvedCSNQuery)
	if err != nil {
		return 0, err
	}
	if len(r.Rows) != 1 || len(r.Rows[0]) < 1 {
		return 0, errors.New("resolved CSN query must return exactly one row")
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(r.Rows[0][0])), 10, 64)
	if err != nil || v == 0 {
		return 0, fmt.Errorf("invalid resolved CSN %q", string(r.Rows[0][0]))
	}
	return v, nil
}

func mergeGaussPrimaryBatch(ctx context.Context, ps []gaussPrimary, tables []string, maxChanges int) ([]orderedmerge.Group[postgresconnector.GaussDBTransaction], error) {
	streams := make([]orderedmerge.Stream[postgresconnector.GaussDBTransaction], 0, len(ps))
	for _, p := range ps {
		resolved, err := resolvedCSN(ctx, p)
		if err != nil {
			return nil, fmt.Errorf("primary %s resolved CSN: %w", p.cfg.ID, err)
		}
		txs, err := p.c.PeekGaussDBTransactionsWithDDL(ctx, p.cfg.Slot, maxChanges, tables)
		if err != nil {
			return nil, fmt.Errorf("primary %s peek: %w", p.cfg.ID, err)
		}
		fs := make([]orderedmerge.Fragment[postgresconnector.GaussDBTransaction], 0, len(txs))
		for _, tx := range txs {
			if tx.CSN == 0 {
				return nil, fmt.Errorf("primary %s transaction %s has no global CSN proof", p.cfg.ID, tx.XID)
			}
			fs = append(fs, orderedmerge.Fragment[postgresconnector.GaussDBTransaction]{Stream: p.cfg.ID, Order: tx.CSN, Value: tx})
		}
		streams = append(streams, orderedmerge.Stream[postgresconnector.GaussDBTransaction]{ID: p.cfg.ID, Resolved: resolved, Fragments: fs})
	}
	return orderedmerge.Merge(streams)
}

func runMultiPrimary(ctx context.Context, rawConfig string) error {
	if !envEnabled("QMIGRATION_EXPERIMENTAL_GAUSSDB_MULTI_PRIMARY") {
		return errors.New("GaussDB multi-primary requires QMIGRATION_EXPERIMENTAL_GAUSSDB_MULTI_PRIMARY=1")
	}
	cfgs, err := loadGaussPrimaries(rawConfig)
	if err != nil {
		return err
	}
	tables, err := parseTables(env("QMIGRATION_GAUSSDB_TABLES", ""))
	if err != nil {
		return err
	}
	ps, err := openGaussPrimaries(ctx, cfgs)
	if err != nil {
		return err
	}
	defer func() {
		for _, p := range ps {
			_ = p.c.Close()
		}
	}()
	server := strings.TrimRight(env("QMIGRATION_SERVER", "http://127.0.0.1:8080"), "/")
	taskID := env("QMIGRATION_TASK_ID", "")
	if taskID == "" {
		return errors.New("QMIGRATION_TASK_ID is required")
	}
	direction := strings.ToLower(env("QMIGRATION_CDC_DIRECTION", "forward"))
	endpoint := env("QMIGRATION_CDC_ENDPOINT", server+"/api/v1/migrations/"+taskID+"/cdc/events")
	token := env("QMIGRATION_API_TOKEN", "")
	client := &http.Client{Timeout: 90 * time.Second}
	if err := waitCDCReady(ctx, client, env("QMIGRATION_CDC_READY_ENDPOINT", ""), token); err != nil {
		return err
	}
	maxChanges, _ := strconv.Atoi(env("QMIGRATION_GAUSSDB_CDC_MAX_CHANGES", "4096"))
	if maxChanges <= 0 {
		maxChanges = 4096
	}
	poll, _ := time.ParseDuration(env("QMIGRATION_GAUSSDB_CDC_POLL_INTERVAL", "1s"))
	if poll <= 0 {
		poll = time.Second
	}
	vector, err := decodeGaussVector(env("QMIGRATION_GAUSSDB_START_VECTOR", ""))
	if err != nil {
		return err
	}
	byID := map[string]gaussPrimary{}
	for _, p := range ps {
		byID[p.cfg.ID] = p
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		groups, err := mergeGaussPrimaryBatch(ctx, ps, tables, maxChanges)
		if err != nil {
			return err
		}
		if len(groups) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(poll):
				continue
			}
		}
		for _, g := range groups {
			next := gaussVector{CSN: g.Order, LSN: map[string]string{}}
			for k, v := range vector.LSN {
				next.LSN[k] = v
			}
			events := []domain.CDCEvent{}
			for _, f := range g.Fragments {
				next.LSN[f.Stream] = f.Value.CommitLSN
				events = append(events, f.Value.Events...)
			}
			pos := encodeGaussVector(next)
			for i := range events {
				events[i].PositionType = "GAUSSDB_CSN_VECTOR"
				events[i].PositionValue = pos
				events[i].Resource = "multi-primary"
			}
			if _, err := postTransaction(ctx, client, endpoint, token, direction, events); err != nil {
				return fmt.Errorf("apply GaussDB CSN %d: %w", g.Order, err)
			}
			// ACK every contributing primary only after the atomic target apply and durable vector checkpoint.
			for _, f := range g.Fragments {
				p := byID[f.Stream]
				if err := p.c.AcknowledgeGaussDBDecodedTransaction(ctx, p.cfg.Slot, f.Value, tables); err != nil {
					return fmt.Errorf("ack primary %s CSN %d: %w", f.Stream, g.Order, err)
				}
			}
			vector = next
		}
	}
}
