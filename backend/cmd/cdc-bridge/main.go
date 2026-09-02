package main

import (
	"bufio"
	"bytes"
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

	"qmigration/backend/internal/domain"
)

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}

func postBatch(client *http.Client, endpoint, token string, req domain.CDCApplyRequest) (*domain.CDCApplyResult, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(httpReq)
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

func main() {
	server := strings.TrimRight(env("QMIGRATION_SERVER", "http://127.0.0.1:8080"), "/")
	taskID := env("QMIGRATION_TASK_ID", "")
	if taskID == "" {
		log.Fatal("QMIGRATION_TASK_ID is required")
	}
	direction := strings.ToLower(env("QMIGRATION_CDC_DIRECTION", "forward"))
	if direction != "forward" && direction != "reverse" {
		log.Fatal("QMIGRATION_CDC_DIRECTION must be forward or reverse")
	}
	token := env("QMIGRATION_API_TOKEN", "")
	batchSize := 100
	if raw := env("QMIGRATION_CDC_BATCH_SIZE", "100"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 10000 {
			batchSize = n
		}
	}
	endpoint := server + "/api/v1/migrations/" + taskID + "/cdc/events"
	client := &http.Client{Timeout: 60 * time.Second}
	scanner := bufio.NewScanner(os.Stdin)
	// CDC rows can contain large JSON/LOB values. Keep a bounded but practical
	// scanner buffer; very large LOB pipelines should use a dedicated streaming adapter.
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	batch := make([]domain.CDCEvent, 0, batchSize)
	total := 0
	flush := func(force bool) error {
		if len(batch) == 0 {
			return nil
		}
		// A durable position on the final event is a hard invariant of the server
		// apply API. If the source only emits positions periodically, keep buffering
		// until one arrives instead of applying rows without a recoverable checkpoint.
		if batch[len(batch)-1].PositionValue == "" {
			if force {
				return errors.New("input ended before the final buffered event carried a durable source position")
			}
			return nil
		}
		result, err := postBatch(client, endpoint, token, domain.CDCApplyRequest{Direction: direction, Events: batch})
		if err != nil {
			return err
		}
		total += result.Applied
		log.Printf("applied=%d total=%d checkpoint=%s:%s", result.Applied, total, result.PositionType, result.PositionValue)
		batch = batch[:0]
		return nil
	}
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var event domain.CDCEvent
		if err := json.Unmarshal(line, &event); err != nil {
			log.Fatalf("decode NDJSON event: %v", err)
		}
		batch = append(batch, event)
		if len(batch) >= batchSize && event.PositionValue != "" {
			if err := flush(false); err != nil {
				log.Fatal(err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	if err := flush(true); err != nil {
		log.Fatal(err)
	}
	log.Printf("CDC bridge finished; total applied=%d", total)
}
