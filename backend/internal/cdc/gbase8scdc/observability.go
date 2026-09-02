package gbase8scdc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

type AgentStatus struct {
	Status                  string       `json:"status"`
	APIVersion              string       `json:"api_version"`
	Provider                ProviderInfo `json:"provider,omitempty"`
	StartedAtUTC            string       `json:"started_at_utc"`
	UptimeSeconds           int64        `json:"uptime_seconds"`
	Busy                    bool         `json:"busy"`
	CurrentOperation        string       `json:"current_operation,omitempty"`
	HealthCalls             uint64       `json:"health_calls"`
	HealthErrors            uint64       `json:"health_errors"`
	CheckpointCalls         uint64       `json:"checkpoint_calls"`
	CheckpointErrors        uint64       `json:"checkpoint_errors"`
	ReadCalls               uint64       `json:"read_calls"`
	ReadErrors              uint64       `json:"read_errors"`
	RecordsReturned         uint64       `json:"records_returned"`
	BytesReturned           uint64       `json:"bytes_returned"`
	LastCheckpointSequence  string       `json:"last_checkpoint_sequence,omitempty"`
	LastCaptureLineage      string       `json:"last_capture_lineage,omitempty"`
	LastReadStartSequence   string       `json:"last_read_start_sequence,omitempty"`
	LastReadNextSequence    string       `json:"last_read_next_sequence,omitempty"`
	LastReadRecords         int          `json:"last_read_records"`
	LastReadBytes           int          `json:"last_read_bytes"`
	LastReadToCurrent       bool         `json:"last_read_to_current"`
	LastOperationDurationMS int64        `json:"last_operation_duration_ms"`
	LastSuccessAtUTC        string       `json:"last_success_at_utc,omitempty"`
	LastErrorAtUTC          string       `json:"last_error_at_utc,omitempty"`
	LastError               string       `json:"last_error,omitempty"`
}

type StatusDescriber interface{ AgentStatus() AgentStatus }
type MetricsRenderer interface{ PrometheusMetrics() string }

type observedAgent struct {
	inner   Agent
	mu      sync.Mutex
	started time.Time
	status  AgentStatus
}

func ObserveAgent(a Agent) Agent {
	if a == nil {
		return nil
	}
	if _, ok := a.(*observedAgent); ok {
		return a
	}
	now := time.Now().UTC()
	o := &observedAgent{inner: a, started: now}
	o.status = AgentStatus{Status: "ok", APIVersion: AgentAPIVersion, Provider: providerInfoOf(a), StartedAtUTC: now.Format(time.RFC3339Nano)}
	return o
}

func providerInfoOf(a Agent) ProviderInfo {
	if d, ok := a.(ProviderDescriber); ok {
		return d.ProviderInfo()
	}
	return ProviderInfo{}
}

func (o *observedAgent) begin(op string) time.Time {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.status.Busy = true
	o.status.CurrentOperation = op
	switch op {
	case "health":
		o.status.HealthCalls++
	case "checkpoint":
		o.status.CheckpointCalls++
	case "read":
		o.status.ReadCalls++
	}
	return time.Now()
}
func (o *observedAgent) end(op string, started time.Time, err error) {
	now := time.Now().UTC()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.status.Busy = false
	o.status.CurrentOperation = ""
	o.status.LastOperationDurationMS = time.Since(started).Milliseconds()
	if err != nil {
		switch op {
		case "health":
			o.status.HealthErrors++
		case "checkpoint":
			o.status.CheckpointErrors++
		case "read":
			o.status.ReadErrors++
		}
		o.status.LastErrorAtUTC = now.Format(time.RFC3339Nano)
		o.status.LastError = err.Error()
		o.status.Status = "degraded"
	} else {
		o.status.LastSuccessAtUTC = now.Format(time.RFC3339Nano)
		o.status.Status = "ok"
	}
}
func (o *observedAgent) Health(ctx context.Context) (err error) {
	st := o.begin("health")
	defer func() { o.end("health", st, err) }()
	return o.inner.Health(ctx)
}
func (o *observedAgent) Checkpoint(ctx context.Context, req CheckpointRequest) (out *CheckpointResponse, err error) {
	st := o.begin("checkpoint")
	defer func() { o.end("checkpoint", st, err) }()
	out, err = o.inner.Checkpoint(ctx, req)
	if err == nil && out != nil {
		o.mu.Lock()
		o.status.LastCheckpointSequence = out.Sequence
		o.status.LastCaptureLineage = out.CaptureLineage
		o.mu.Unlock()
	}
	return out, err
}
func (o *observedAgent) Read(ctx context.Context, req ReadRequest) (out *ReadResponse, err error) {
	st := o.begin("read")
	defer func() { o.end("read", st, err) }()
	out, err = o.inner.Read(ctx, req)
	if err == nil && out != nil {
		b, _ := json.Marshal(out)
		o.mu.Lock()
		o.status.LastReadStartSequence = req.StartSequence
		o.status.LastReadNextSequence = out.NextSequence
		o.status.LastCaptureLineage = out.CaptureLineage
		o.status.LastReadRecords = len(out.Records)
		o.status.LastReadBytes = len(b)
		o.status.LastReadToCurrent = out.ReadToCurrent
		o.status.RecordsReturned += uint64(len(out.Records))
		o.status.BytesReturned += uint64(len(b))
		o.mu.Unlock()
	}
	return out, err
}
func (o *observedAgent) ProviderInfo() ProviderInfo { return providerInfoOf(o.inner) }
func (o *observedAgent) Close() error {
	if c, ok := o.inner.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}
func (o *observedAgent) AgentStatus() AgentStatus {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := o.status
	s.Provider = providerInfoOf(o.inner)
	s.UptimeSeconds = int64(time.Since(o.started).Seconds())
	return s
}
func (o *observedAgent) PrometheusMetrics() string {
	s := o.AgentStatus()
	busy := 0
	if s.Busy {
		busy = 1
	}
	healthy := 1
	if s.Status != "ok" {
		healthy = 0
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# HELP qmigration_gbase8s_cdc_agent_up Agent/provider health state.\n# TYPE qmigration_gbase8s_cdc_agent_up gauge\nqmigration_gbase8s_cdc_agent_up %d\n", healthy)
	fmt.Fprintf(&b, "# HELP qmigration_gbase8s_cdc_agent_busy Whether a provider call is active.\n# TYPE qmigration_gbase8s_cdc_agent_busy gauge\nqmigration_gbase8s_cdc_agent_busy %d\n", busy)
	fmt.Fprintf(&b, "# TYPE qmigration_gbase8s_cdc_health_calls_total counter\nqmigration_gbase8s_cdc_health_calls_total %d\n", s.HealthCalls)
	fmt.Fprintf(&b, "# TYPE qmigration_gbase8s_cdc_health_errors_total counter\nqmigration_gbase8s_cdc_health_errors_total %d\n", s.HealthErrors)
	fmt.Fprintf(&b, "# TYPE qmigration_gbase8s_cdc_checkpoint_calls_total counter\nqmigration_gbase8s_cdc_checkpoint_calls_total %d\n", s.CheckpointCalls)
	fmt.Fprintf(&b, "# TYPE qmigration_gbase8s_cdc_checkpoint_errors_total counter\nqmigration_gbase8s_cdc_checkpoint_errors_total %d\n", s.CheckpointErrors)
	fmt.Fprintf(&b, "# TYPE qmigration_gbase8s_cdc_read_calls_total counter\nqmigration_gbase8s_cdc_read_calls_total %d\n", s.ReadCalls)
	fmt.Fprintf(&b, "# TYPE qmigration_gbase8s_cdc_read_errors_total counter\nqmigration_gbase8s_cdc_read_errors_total %d\n", s.ReadErrors)
	fmt.Fprintf(&b, "# TYPE qmigration_gbase8s_cdc_records_returned_total counter\nqmigration_gbase8s_cdc_records_returned_total %d\n", s.RecordsReturned)
	fmt.Fprintf(&b, "# TYPE qmigration_gbase8s_cdc_bytes_returned_total counter\nqmigration_gbase8s_cdc_bytes_returned_total %d\n", s.BytesReturned)
	fmt.Fprintf(&b, "# TYPE qmigration_gbase8s_cdc_last_operation_duration_milliseconds gauge\nqmigration_gbase8s_cdc_last_operation_duration_milliseconds %d\n", s.LastOperationDurationMS)
	return b.String()
}
