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

	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/cdc/ticdc"
	"qmigration/backend/internal/domain"
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
					log.Printf("native TiDB CDC released at task status %s", state.TaskStatus)
					return nil
				}
				log.Printf("native TiDB CDC waiting; task status=%s", state.TaskStatus)
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
func tableList(raw string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, v := range strings.Split(raw, ",") {
		v = strings.TrimSpace(v)
		if v != "" && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
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
	ep, err := ticdc.ParseEndpoint(env("QMIGRATION_TICDC_URL", ""))
	if err != nil {
		return err
	}
	start, err := ticdc.ParsePosition(env("QMIGRATION_TICDC_START_POSITION", ""))
	if err != nil {
		return err
	}
	cfID, topic := ticdc.DeterministicNames(taskID, direction)
	if v := env("QMIGRATION_TICDC_CHANGEFEED_ID", ""); v != "" {
		cfID = v
	}
	if v := env("QMIGRATION_TICDC_TOPIC", ""); v != "" {
		topic = v
	}
	tables := tableList(env("QMIGRATION_TICDC_TABLES", ""))
	if len(tables) == 0 {
		return errors.New("QMIGRATION_TICDC_TABLES is required")
	}
	control := ticdc.NewControlClient(ep, nil)
	if start.HasDurableKafkaOffset() {
		existing, _, e := control.GetChangefeed(ctx, cfID)
		if e != nil {
			return e
		}
		if existing == nil {
			return fmt.Errorf("TiCDC changefeed %s is missing while durable Kafka checkpoint %s exists; refusing to recreate a new topic behind an existing checkpoint", cfID, start.String())
		}
	}
	// This occurs before the full-load readiness gate. TiCDC therefore protects
	// the start TSO and accumulates changes in Kafka while the snapshot runs.
	if err := control.EnsureChangefeed(ctx, ticdc.ChangefeedPlan{ID: cfID, Topic: topic, StartTS: start.TSO, Tables: tables}); err != nil {
		return err
	}
	kafka, err := ticdc.NewKafkaClientForEndpoint(ep, "qmigration-"+taskID)
	if err != nil {
		return err
	}
	meta, err := kafka.Metadata(ctx, topic)
	if err != nil {
		return fmt.Errorf("TiCDC Kafka topic readiness: %w", err)
	}
	if meta.Count != ep.KafkaPartitions {
		return fmt.Errorf("TiCDC topic partition count %d does not match configured kafka_partitions=%d; exact partition topology is required for durable per-partition checkpoints", meta.Count, ep.KafkaPartitions)
	}

	endpoint := env("QMIGRATION_CDC_ENDPOINT", server+"/api/v1/migrations/"+taskID+"/cdc/events")
	readyEndpoint := env("QMIGRATION_CDC_READY_ENDPOINT", "")
	token := env("QMIGRATION_API_TOKEN", "")
	client := &http.Client{Timeout: 90 * time.Second}
	if err := waitReady(ctx, client, readyEndpoint, token); err != nil {
		return err
	}
	selected := selectedSet(strings.Join(tables, ","))
	current := start
	for {
		reader, err := ticdc.NewReader(kafka, topic, cfID, current, selected)
		if err != nil {
			return err
		}
		streamErr := (cdcruntime.Runner{Reader: reader, Apply: func(applyCtx context.Context, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
			return postEvents(applyCtx, client, endpoint, token, direction, events)
		}, Observe: func(tx *cdcruntime.Transaction, result *domain.CDCApplyResult) {
			current = reader.Acknowledged()
			applied := 0
			if result != nil {
				applied = result.Applied
			}
			log.Printf("%s applied=%d checkpoint=%s", tx.Label, applied, current.String())
		}}).Run(ctx)
		current = reader.Acknowledged()
		if streamErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("TiCDC stream interrupted at acknowledged %s: %v; reconnecting", current.String(), streamErr)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
