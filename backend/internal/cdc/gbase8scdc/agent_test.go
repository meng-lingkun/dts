package gbase8scdc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"qmigration/backend/internal/domain"
)

type concurrentAgent struct {
	active int32
	max    int32
}

func (a *concurrentAgent) enter() {
	n := atomic.AddInt32(&a.active, 1)
	for {
		m := atomic.LoadInt32(&a.max)
		if n <= m || atomic.CompareAndSwapInt32(&a.max, m, n) {
			break
		}
	}
	time.Sleep(2 * time.Millisecond)
	atomic.AddInt32(&a.active, -1)
}
func (a *concurrentAgent) Health(context.Context) error { a.enter(); return nil }
func (a *concurrentAgent) Checkpoint(context.Context, CheckpointRequest) (*CheckpointResponse, error) {
	a.enter()
	return &CheckpointResponse{Sequence: "1"}, nil
}
func (a *concurrentAgent) Read(context.Context, ReadRequest) (*ReadResponse, error) {
	a.enter()
	return &ReadResponse{}, nil
}

func TestSerializeAgentSerializesProviderCalls(t *testing.T) {
	inner := &concurrentAgent{}
	a := SerializeAgent(inner)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 3 {
			case 0:
				_ = a.Health(context.Background())
			case 1:
				_, _ = a.Checkpoint(context.Background(), CheckpointRequest{})
			default:
				_, _ = a.Read(context.Background(), ReadRequest{})
			}
		}(i)
	}
	wg.Wait()
	if got := atomic.LoadInt32(&inner.max); got != 1 {
		t.Fatalf("provider concurrency=%d want 1", got)
	}
}

func TestNewClientRejectsRemotePlaintextAndCredentials(t *testing.T) {
	if _, err := NewClient("gbase8scdc://10.0.0.2:9188", "", "", ""); err == nil {
		t.Fatal("expected remote plaintext rejection")
	}
	if _, err := NewClient("gbase8scdc://user:pass@127.0.0.1:9188", "", "", ""); err == nil {
		t.Fatal("expected URL credential rejection")
	}
	if _, err := NewClient("gbase8scdc://127.0.0.1:9188", "", "", ""); err != nil {
		t.Fatal(err)
	}
}

func TestClientHealthInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		write := `{"status":"ok","api_version":"v4","provider":{"kind":"native-c-abi","abi_version":"4","sha256_pinned":true}}`
		_, _ = w.Write([]byte(write))
	}))
	defer srv.Close()
	c, err := NewClient(srv.URL, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	info, err := c.HealthInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Provider.Kind != "native-c-abi" || info.Provider.ABIVersion != "4" || !info.Provider.SHA256Pinned {
		t.Fatalf("info=%+v", info)
	}
}

func TestClientRejectsLegacyAgentAPI(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok","api_version":"v1"}`))
	}))
	defer srv.Close()
	c, err := NewClient(srv.URL, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.HealthInfo(context.Background()); err == nil || !strings.Contains(err.Error(), "requires v4") {
		t.Fatalf("expected legacy API rejection, got %v", err)
	}
}

func TestSchemaFingerprintDeterministicAndSensitive(t *testing.T) {
	cols := []domain.ColumnInfo{{Name: "ID", ColumnType: "INTEGER", Nullable: false, PrimaryKey: true}, {Name: "V", ColumnType: "VARCHAR(64)", Nullable: true}}
	a, err := BuildTableSelection("APP", "Orders", cols, []string{"ID"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := BuildTableSelection("app", "orders", []domain.ColumnInfo{{Name: "id", ColumnType: "integer", Nullable: false, PrimaryKey: true}, {Name: "v", ColumnType: "varchar(64)", Nullable: true}}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if a.SchemaFingerprint != b.SchemaFingerprint {
		t.Fatalf("case-normalized fingerprint differs: %s %s", a.SchemaFingerprint, b.SchemaFingerprint)
	}
	c, _ := BuildTableSelection("app", "orders", []domain.ColumnInfo{{Name: "id", ColumnType: "integer", Nullable: false, PrimaryKey: true}, {Name: "v", ColumnType: "varchar(128)", Nullable: true}}, []string{"id"})
	if c.SchemaFingerprint == b.SchemaFingerprint {
		t.Fatal("type change did not change schema fingerprint")
	}
	d, _ := BuildTableSelection("app", "orders", []domain.ColumnInfo{{Name: "id", ColumnType: "integer", Nullable: false, PrimaryKey: true}, {Name: "v", ColumnType: "varchar(64)", Nullable: false}}, []string{"id"})
	if d.SchemaFingerprint == b.SchemaFingerprint {
		t.Fatal("nullability change did not change schema fingerprint")
	}
}

func TestValidateSchemaFences(t *testing.T) {
	s := sel()
	if err := ValidateSchemaFences(s, SchemaFencesForSelections(s)); err != nil {
		t.Fatal(err)
	}
	bad := SchemaFencesForSelections(s)
	bad[0].Fingerprint = strings.Repeat("0", 64)
	if err := ValidateSchemaFences(s, bad); err == nil || !strings.Contains(strings.ToLower(err.Error()), "drift") {
		t.Fatalf("expected drift, got %v", err)
	}
}

func TestCaptureLineageValidation(t *testing.T) {
	want := strings.Repeat("a", CaptureLineageHexLength)
	other := strings.Repeat("b", CaptureLineageHexLength)
	selections := sel()
	cp := &CheckpointResponse{Sequence: "10", CaptureLineage: want, SchemaFences: SchemaFencesForSelections(selections)}
	if err := ValidateCheckpointResponse(CheckpointRequest{Tables: selections}, cp); err != nil {
		t.Fatal(err)
	}
	rr := &ReadResponse{CaptureLineage: other, SchemaFences: SchemaFencesForSelections(selections)}
	err := ValidateReadResponse(ReadRequest{ExpectedCaptureLineage: want, Tables: selections}, rr)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "lineage changed") {
		t.Fatalf("expected lineage mismatch, got %v", err)
	}
}
