package gbase8scdc

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type observedFakeAgent struct{ failRead bool }

func (a *observedFakeAgent) Health(context.Context) error { return nil }
func (a *observedFakeAgent) Checkpoint(context.Context, CheckpointRequest) (*CheckpointResponse, error) {
	return &CheckpointResponse{Sequence: "42", CaptureLineage: strings.Repeat("a", CaptureLineageHexLength)}, nil
}
func (a *observedFakeAgent) Read(context.Context, ReadRequest) (*ReadResponse, error) {
	if a.failRead {
		return nil, errors.New("read boom")
	}
	return &ReadResponse{CaptureLineage: strings.Repeat("a", CaptureLineageHexLength), Records: []RecordEnvelope{{Kind: KindBegin, Sequence: "43", TransactionID: 7}}, NextSequence: "44", ReadToCurrent: true}, nil
}
func (a *observedFakeAgent) ProviderInfo() ProviderInfo {
	return ProviderInfo{Kind: "native-c-abi", ABIVersion: "4", SHA256Pinned: true}
}

func TestObserveAgentStatusAndMetrics(t *testing.T) {
	inner := &observedFakeAgent{}
	a := ObserveAgent(inner)
	if err := a.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Checkpoint(context.Background(), CheckpointRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Read(context.Background(), ReadRequest{StartSequence: "43"}); err != nil {
		t.Fatal(err)
	}
	d, ok := a.(StatusDescriber)
	if !ok {
		t.Fatal("missing status describer")
	}
	s := d.AgentStatus()
	if s.HealthCalls != 1 || s.CheckpointCalls != 1 || s.ReadCalls != 1 {
		t.Fatalf("calls=%+v", s)
	}
	if s.LastCheckpointSequence != "42" || s.LastReadStartSequence != "43" || s.LastReadNextSequence != "44" {
		t.Fatalf("sequences=%+v", s)
	}
	lineage := strings.Repeat("a", CaptureLineageHexLength)
	if s.LastCaptureLineage != lineage {
		t.Fatalf("capture lineage=%q", s.LastCaptureLineage)
	}
	if s.RecordsReturned != 1 || s.LastReadRecords != 1 || !s.LastReadToCurrent {
		t.Fatalf("read status=%+v", s)
	}
	if s.Provider.Kind != "native-c-abi" || !s.Provider.SHA256Pinned {
		t.Fatalf("provider=%+v", s.Provider)
	}
	m := a.(MetricsRenderer).PrometheusMetrics()
	for _, want := range []string{"qmigration_gbase8s_cdc_agent_up 1", "qmigration_gbase8s_cdc_health_calls_total 1", "qmigration_gbase8s_cdc_read_calls_total 1", "qmigration_gbase8s_cdc_records_returned_total 1"} {
		if !strings.Contains(m, want) {
			t.Fatalf("metrics missing %q:\n%s", want, m)
		}
	}
	if strings.Contains(m, "43") || strings.Contains(m, "44") || strings.Contains(m, lineage) {
		t.Fatalf("metrics must not expose exact sequence/lineage values as labels/gauges: %s", m)
	}
}

func TestObserveAgentRecordsErrors(t *testing.T) {
	inner := &observedFakeAgent{failRead: true}
	a := ObserveAgent(inner)
	_, err := a.Read(context.Background(), ReadRequest{StartSequence: "10"})
	if err == nil {
		t.Fatal("expected error")
	}
	s := a.(StatusDescriber).AgentStatus()
	if s.Status != "degraded" || s.ReadErrors != 1 || s.LastError == "" || s.LastErrorAtUTC == "" {
		t.Fatalf("status=%+v", s)
	}
}
