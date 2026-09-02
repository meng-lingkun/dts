package ticdc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ControlClient struct {
	ep     Endpoint
	client *http.Client
}

func NewControlClient(ep Endpoint, client *http.Client) *ControlClient {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &ControlClient{ep: ep, client: client}
}

type Changefeed struct {
	ID            string `json:"id"`
	State         string `json:"state"`
	SinkURI       string `json:"sink_uri"`
	StartTS       uint64 `json:"start_ts"`
	CheckpointTSO uint64 `json:"checkpoint_tso"`
	Error         any    `json:"error"`
}

type ChangefeedPlan struct {
	ID, Topic string
	StartTS   uint64
	Tables    []string
}

func DeterministicNames(taskID, direction string) (string, string) {
	raw := strings.TrimSpace(taskID) + "\x00" + strings.ToLower(strings.TrimSpace(direction))
	h := sha256.Sum256([]byte(raw))
	suffix := hex.EncodeToString(h[:8])
	return "qmigration-" + suffix, "qmigration-" + suffix
}

func (c *ControlClient) Health(ctx context.Context) error {
	_, status, err := c.do(ctx, http.MethodGet, "/api/v2/health", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("TiCDC health returned HTTP %d", status)
	}
	return nil
}
func (c *ControlClient) GetChangefeed(ctx context.Context, id string) (*Changefeed, int, error) {
	body, status, err := c.do(ctx, http.MethodGet, "/api/v2/changefeeds/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, status, err
	}
	if status == http.StatusNotFound {
		return nil, status, nil
	}
	if status < 200 || status >= 300 {
		return nil, status, fmt.Errorf("TiCDC query changefeed returned HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}
	var out Changefeed
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, status, fmt.Errorf("decode TiCDC changefeed: %w", err)
	}
	return &out, status, nil
}
func (c *ControlClient) EnsureChangefeed(ctx context.Context, plan ChangefeedPlan) error {
	if plan.ID == "" || plan.Topic == "" || plan.StartTS == 0 || len(plan.Tables) == 0 {
		return errors.New("TiCDC changefeed plan requires id/topic/start TSO/tables")
	}
	if err := c.Health(ctx); err != nil {
		return err
	}
	existing, _, err := c.GetChangefeed(ctx, plan.ID)
	if err != nil {
		return err
	}
	if existing == nil {
		if err := c.createChangefeed(ctx, plan); err != nil {
			return err
		}
	} else {
		if existing.StartTS != 0 && existing.StartTS > plan.StartTS {
			return fmt.Errorf("existing TiCDC changefeed %s starts at TSO %d after required start %d", plan.ID, existing.StartTS, plan.StartTS)
		}
		if existing.SinkURI != "" && !sinkMatchesEndpoint(existing.SinkURI, plan.Topic, c.ep) {
			return fmt.Errorf("existing TiCDC changefeed %s uses a different sink/topic", plan.ID)
		}
		if strings.EqualFold(existing.State, "stopped") {
			if err := c.resume(ctx, plan.ID); err != nil {
				return err
			}
		}
	}
	return c.waitNormal(ctx, plan.ID)
}
func (c *ControlClient) createChangefeed(ctx context.Context, plan ChangefeedPlan) error {
	rules := append([]string(nil), plan.Tables...)
	sort.Strings(rules)
	payload := map[string]any{"changefeed_id": plan.ID, "sink_uri": buildKafkaSinkURIEndpoint(c.ep, plan.Topic), "start_ts": plan.StartTS, "replica_config": map[string]any{"case_sensitive": false, "check_gc_safe_point": true, "filter": map[string]any{"rules": rules}, "sink": map[string]any{"protocol": "canal-json", "transaction_atomicity": "none"}}}
	body, status, err := c.do(ctx, http.MethodPost, "/api/v2/changefeeds", payload)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("TiCDC create changefeed returned HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}
	return nil
}
func buildKafkaSinkURI(brokers []string, topic string) string {
	return buildKafkaSinkURIEndpoint(Endpoint{Brokers: brokers, KafkaPartitions: 1}, topic)
}

func buildKafkaSinkURIEndpoint(ep Endpoint, topic string) string {
	q := url.Values{}
	q.Set("protocol", "canal-json")
	q.Set("enable-tidb-extension", "true")
	partitions := ep.KafkaPartitions
	if partitions <= 0 {
		partitions = 1
	}
	q.Set("partition-num", strconv.Itoa(partitions))
	q.Set("compression", "none")
	q.Set("auto-create-topic", "true")
	if ep.KafkaTLS {
		q.Set("enable-tls", "true")
		if ep.KafkaCA != "" {
			q.Set("ca", ep.KafkaCA)
		}
		if ep.KafkaCert != "" {
			q.Set("cert", ep.KafkaCert)
		}
		if ep.KafkaKey != "" {
			q.Set("key", ep.KafkaKey)
		}
	}
	if ep.KafkaSASLMechanism != "" {
		q.Set("sasl-mechanism", strings.ToLower(ep.KafkaSASLMechanism))
		if user := os.Getenv("QMIGRATION_TIDB_KAFKA_SASL_USERNAME"); user != "" {
			q.Set("sasl-user", user)
		}
		if password := os.Getenv("QMIGRATION_TIDB_KAFKA_SASL_PASSWORD"); password != "" {
			q.Set("sasl-password", password)
		}
	}
	return "kafka://" + strings.Join(ep.Brokers, ",") + "/" + url.PathEscape(topic) + "?" + q.Encode()
}

func sinkMatches(raw, topic string) bool {
	return sinkMatchesEndpoint(raw, topic, Endpoint{KafkaPartitions: 1})
}

func sinkMatchesEndpoint(raw, topic string, ep Endpoint) bool {
	u, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(u.Scheme, "kafka") {
		return false
	}
	partitions := ep.KafkaPartitions
	if partitions <= 0 {
		partitions = 1
	}
	q := u.Query()
	if strings.TrimPrefix(u.Path, "/") != topic || !strings.EqualFold(q.Get("protocol"), "canal-json") || !strings.EqualFold(q.Get("enable-tidb-extension"), "true") || q.Get("partition-num") != strconv.Itoa(partitions) {
		return false
	}
	if ep.KafkaTLS != strings.EqualFold(q.Get("enable-tls"), "true") {
		return false
	}
	if ep.KafkaSASLMechanism != "" && !strings.EqualFold(q.Get("sasl-mechanism"), ep.KafkaSASLMechanism) {
		return false
	}
	return true
}

func RedactKafkaSinkURI(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid-kafka-sink-uri>"
	}
	q := u.Query()
	for _, key := range []string{"sasl-password", "sasl-oauth-client-secret", "password"} {
		if q.Has(key) {
			q.Set(key, "REDACTED")
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (c *ControlClient) DeleteChangefeed(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("TiCDC changefeed id is empty")
	}
	body, status, err := c.do(ctx, http.MethodDelete, "/api/v2/changefeeds/"+url.PathEscape(id), nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("TiCDC delete changefeed returned HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}
	return nil
}

func (c *ControlClient) resume(ctx context.Context, id string) error {
	body, status, err := c.do(ctx, http.MethodPost, "/api/v2/changefeeds/"+url.PathEscape(id)+"/resume", map[string]any{})
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("TiCDC resume changefeed returned HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}
	return nil
}
func (c *ControlClient) waitNormal(ctx context.Context, id string) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		cf, _, err := c.GetChangefeed(ctx, id)
		if err != nil {
			return err
		}
		if cf != nil {
			switch strings.ToLower(strings.TrimSpace(cf.State)) {
			case "normal":
				return nil
			case "failed", "error", "finished":
				return fmt.Errorf("TiCDC changefeed %s entered state %s", id, cf.State)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (c *ControlClient) do(ctx context.Context, method, path string, payload any) ([]byte, int, error) {
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.ep.ControlURL, "/")+path, body)
	if err != nil {
		return nil, 0, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("TiCDC %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return b, resp.StatusCode, nil
}
