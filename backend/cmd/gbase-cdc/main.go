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
	"strings"
	"time"

	"qmigration/backend/internal/cdc/gbase8acdc"
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/domain"
)

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
func on(k string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(k))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
func post(ctx context.Context, c *http.Client, url, token, direction string, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
	b, e := json.Marshal(domain.CDCApplyRequest{Direction: direction, Events: events})
	if e != nil {
		return nil, e
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if wt := strings.TrimSpace(os.Getenv("QMIGRATION_WORKER_TOKEN")); wt != "" {
		req.Header.Set("X-QMigration-Worker-Token", wt)
	}
	resp, e := c.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("QMigration returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	var out domain.CDCApplyResult
	if len(data) > 0 {
		if e = json.Unmarshal(data, &out); e != nil {
			return nil, e
		}
	}
	return &out, nil
}
func run(ctx context.Context) error {
	if !on("QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE") || !on("QMIGRATION_EXPERIMENTAL_GBASE8A_SOURCE_CDC") {
		return errors.New("GBase 8a CDC requires QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE=1 and QMIGRATION_EXPERIMENTAL_GBASE8A_SOURCE_CDC=1")
	}
	raw, start, database := env("QMIGRATION_GBASE8A_CDC_URL", ""), env("QMIGRATION_GBASE8A_CDC_START_POSITION", ""), env("QMIGRATION_GBASE8A_CDC_DATABASE", "")
	if raw == "" || start == "" {
		return errors.New("GBase 8a CDC URL and start position are required")
	}
	var selections []gbase8acdc.TableSelection
	if e := json.Unmarshal([]byte(env("QMIGRATION_GBASE8A_CDC_SELECTIONS_JSON", "")), &selections); e != nil || len(selections) == 0 {
		return fmt.Errorf("invalid GBase 8a CDC selections: %v", e)
	}
	agent, e := gbase8acdc.NewClient(raw, os.Getenv("QMIGRATION_GBASE8A_CDC_CA_PEM"), os.Getenv("QMIGRATION_GBASE8A_CDC_SERVER_NAME"), os.Getenv("QMIGRATION_GBASE8A_CDC_TOKEN"))
	if e != nil {
		return e
	}
	if e = agent.Health(ctx); e != nil {
		return fmt.Errorf("GBase 8a CDC provider health: %w", e)
	}
	server := strings.TrimRight(env("QMIGRATION_SERVER", "http://127.0.0.1:8080"), "/")
	task := env("QMIGRATION_TASK_ID", "")
	if task == "" {
		return errors.New("QMIGRATION_TASK_ID is required")
	}
	direction := strings.ToLower(env("QMIGRATION_CDC_DIRECTION", "forward"))
	if direction != "forward" && direction != "reverse" {
		return errors.New("invalid CDC direction")
	}
	endpoint := env("QMIGRATION_CDC_ENDPOINT", server+"/api/v1/migrations/"+task+"/cdc/events")
	token := env("QMIGRATION_API_TOKEN", "")
	hc := &http.Client{Timeout: 90 * time.Second}
	r, e := gbase8acdc.NewReader(agent, database, start, selections, raw)
	if e != nil {
		return e
	}
	total := 0
	return (cdcruntime.Runner{Reader: r, Apply: func(c context.Context, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
		return post(c, hc, endpoint, token, direction, events)
	}, Observe: func(tx *cdcruntime.Transaction, res *domain.CDCApplyResult) {
		if res != nil {
			total += res.Applied
			log.Printf("%s applied=%d total=%d checkpoint=%s", tx.Label, res.Applied, total, tx.Checkpoint.PositionValue)
		}
	}}).Run(ctx)
}
func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
