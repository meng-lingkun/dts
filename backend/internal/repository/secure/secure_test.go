package secure

import (
	"context"
	"os"
	"path/filepath"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/repository/memory"
	"qmigration/backend/internal/security"
	"strings"
	"testing"
	"time"
)

func TestEncryptedPersistentDatasource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	base, err := memory.NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := security.New("master")
	s := New(base, c)
	d := &domain.DataSource{ID: "ds1", Name: "test", Type: domain.DataSourceMySQL, Host: "127.0.0.1", Port: 3306, Username: "root", Password: "top-secret", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := s.CreateDataSource(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "top-secret") {
		t.Fatal("plaintext password persisted")
	}
	got, err := s.GetDataSource(context.Background(), "ds1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Password != "top-secret" {
		t.Fatalf("got password %q", got.Password)
	}
	listed, _ := s.ListDataSources(context.Background())
	if listed[0].Password != "" {
		t.Fatal("password leaked in listing")
	}
}

func TestEncryptedPersistentCDCDeadLetterPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	base, err := memory.NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := security.New("master")
	s := New(base, c)
	now := time.Now()
	dlq := &domain.CDCDeadLetter{
		ID: "dlq1", TaskID: "task1", Direction: "forward", PositionType: "GTID", PositionValue: "uuid:1-3",
		Events:    []domain.CDCEvent{{Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "customers", After: []domain.CDCField{{Column: "name", Value: "sensitive-customer-name"}}}},
		LastError: "test", RetryCount: 1, Status: domain.CDCDeadLetterOpen, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.CreateCDCDeadLetter(context.Background(), dlq); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "sensitive-customer-name") {
		t.Fatal("plaintext CDC payload persisted")
	}
	got, err := s.GetCDCDeadLetter(context.Background(), "dlq1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 1 || got.Events[0].After[0].Value != "sensitive-customer-name" {
		t.Fatalf("payload did not decrypt: %+v", got)
	}
	if got.EventsCiphertext != "" {
		t.Fatal("ciphertext leaked through secure repository")
	}
}

func TestEncryptedPersistentCDCSpoolPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	base, err := memory.NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := security.New("master")
	s := New(base, c)
	now := time.Now()
	rec := &domain.CDCSpoolRecord{ID: "sp1", TaskID: "task1", Direction: "forward", PositionType: "BINLOG", PositionValue: "mysql-bin.000001:120", Events: []domain.CDCEvent{{Operation: domain.CDCInsert, SourceSchema: "app", SourceTable: "customers", After: []domain.CDCField{{Column: "name", Value: "spool-secret-value"}}}}, Status: domain.CDCSpoolPending, CreatedAt: now}
	if err := s.CreateCDCSpool(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "spool-secret-value") {
		t.Fatal("plaintext CDC spool payload persisted")
	}
	reopened, err := memory.NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	secure2 := New(reopened, c)
	items, err := secure2.ListCDCSpool(context.Background(), "task1", "forward", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || len(items[0].Events) != 1 || items[0].Events[0].After[0].Value != "spool-secret-value" {
		t.Fatalf("spool payload did not decrypt: %+v", items)
	}
	if items[0].EventsCiphertext != "" {
		t.Fatal("ciphertext leaked through secure repository")
	}
}
