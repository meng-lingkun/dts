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

	"qmigration/backend/internal/cdc/gbase8scdc"
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/domain"
)

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
func enabled(k string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(k))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

type readyResponse struct {
	Ready      bool                   `json:"ready"`
	TaskStatus domain.MigrationStatus `json:"task_status"`
}

func waitReady(ctx context.Context, c *http.Client, url, token string) error {
	if strings.TrimSpace(url) == "" {
		return nil
	}
	for {
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
		if e != nil {
			return e
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if wt := strings.TrimSpace(os.Getenv("QMIGRATION_WORKER_TOKEN")); wt != "" {
			req.Header.Set("X-QMigration-Worker-Token", wt)
		}
		resp, e := c.Do(req)
		if e == nil {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				var r readyResponse
				if e = json.Unmarshal(b, &r); e != nil {
					return e
				}
				if r.Ready {
					return nil
				}
			} else {
				e = fmt.Errorf("CDC readiness returned %s: %s", resp.Status, strings.TrimSpace(string(b)))
			}
		}
		if e != nil {
			log.Printf("GBase 8s CDC readiness: %v", e)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
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
	if !enabled("QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE") || !enabled("QMIGRATION_EXPERIMENTAL_GBASE8S_CDC") {
		return errors.New("GBase 8s CDC requires QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE=1 and QMIGRATION_EXPERIMENTAL_GBASE8S_CDC=1")
	}
	rawURL := env("QMIGRATION_GBASE8S_CDC_URL", "")
	start := env("QMIGRATION_GBASE8S_CDC_START_POSITION", "")
	database := env("QMIGRATION_GBASE8S_CDC_DATABASE", "")
	if rawURL == "" || start == "" {
		return errors.New("GBase 8s CDC URL and start position are required")
	}
	var selections []gbase8scdc.TableSelection
	if e := json.Unmarshal([]byte(env("QMIGRATION_GBASE8S_CDC_SELECTIONS_JSON", "")), &selections); e != nil || len(selections) == 0 {
		return fmt.Errorf("invalid GBase 8s CDC selections: %v", e)
	}
	agent, e := gbase8scdc.NewClient(rawURL, os.Getenv("QMIGRATION_GBASE8S_CDC_CA_PEM"), os.Getenv("QMIGRATION_GBASE8S_CDC_SERVER_NAME"), os.Getenv("QMIGRATION_GBASE8S_CDC_TOKEN"))
	if e != nil {
		return e
	}
	if e = agent.Health(ctx); e != nil {
		return fmt.Errorf("GBase 8s CDC provider health: %w", e)
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
	hc := &http.Client{Timeout: 90 * time.Second}
	if e = waitReady(ctx, hc, ready, token); e != nil {
		return e
	}
	r, e := gbase8scdc.NewReader(agent, database, start, selections, rawURL)
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
