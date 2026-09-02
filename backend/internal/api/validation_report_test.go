package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/engine"
	"qmigration/backend/internal/repository/memory"
	"qmigration/backend/internal/validationreport"
)

func newValidationReportTestServer(t *testing.T) *Server {
	t.Helper()
	t.Setenv("QMIGRATION_AUTH_REQUIRED", "false")
	t.Setenv("QMIGRATION_RBAC_TOKENS", "")
	t.Setenv("QMIGRATION_API_TOKEN", "")
	t.Setenv("QMIGRATION_VALIDATION_REPORT_HMAC_KEY", "0123456789abcdef0123456789abcdef")
	t.Setenv("QMIGRATION_VALIDATION_REPORT_HMAC_KEY_ID", "test-key")
	t.Setenv("QMIGRATION_VALIDATION_REPORT_ED25519_PRIVATE_KEY", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{5}, ed25519.SeedSize)))
	t.Setenv("QMIGRATION_VALIDATION_REPORT_ED25519_KEY_ID", "public-test-key")
	repo := memory.New()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	task := &domain.MigrationTask{ID: "m-report", Name: "acceptance", Mode: domain.ModeFullAndIncremental, Status: domain.StatusFinished, SourceID: "src", TargetID: "dst", RowsMigrated: 10, BytesMigrated: 100, CreatedAt: now.Add(-time.Hour), UpdatedAt: now}
	if err := repo.CreateMigration(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	archive := &domain.ValidationArchive{TaskID: task.ID, TerminalStatus: task.Status, ValidationMode: "ROW_COUNT_CHECKSUM", TotalTables: 1, TotalChunks: 1, CoveredChunks: 1, SuccessChunks: 1, EvidenceDigest: strings.Repeat("a", 64), ArchivedAt: now, Tables: []domain.ValidationTableArchive{{TableID: "t1", SourceSchema: "app", SourceTable: "t", TargetSchema: "app", TargetTable: "t", EvidenceScope: "CHUNK_SET", ChecksumKind: "CHUNK_SET_SHA256", TotalChunks: 1, CoveredChunks: 1, SuccessChunks: 1, SourceRows: 10, TargetRows: 10, EvidenceDigest: strings.Repeat("b", 64)}}}
	if created, err := repo.CreateValidationArchive(context.Background(), archive); err != nil || !created {
		t.Fatalf("archive create=%v err=%v", created, err)
	}
	return New(repo, connector.NewRegistry(), engine.NewRegistry())
}

func TestValidationReportEndpoints(t *testing.T) {
	s := newValidationReportTestServer(t)
	h := s.Handler()
	for _, tc := range []struct{ path, ct, prefix string }{
		{"/api/v1/migrations/m-report/validation-report?format=json", "application/json", "{"},
		{"/api/v1/migrations/m-report/validation-report?format=html", "text/html", "<!doctype html>"},
		{"/api/v1/migrations/m-report/validation-report?format=pdf", "application/pdf", "%PDF-1.4"},
		{"/api/v1/migrations/m-report/validation-report/manifest", "application/json", "{"},
	} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		h.ServeHTTP(rr, req)
		if rr.Code != 200 {
			t.Fatalf("%s status=%d body=%s", tc.path, rr.Code, rr.Body.String())
		}
		if !strings.HasPrefix(rr.Header().Get("Content-Type"), tc.ct) {
			t.Fatalf("%s content-type=%s", tc.path, rr.Header().Get("Content-Type"))
		}
		if !strings.HasPrefix(rr.Body.String(), tc.prefix) {
			t.Fatalf("%s prefix=%q", tc.path, rr.Body.String()[:minInt(len(rr.Body.String()), 20)])
		}
		if rr.Header().Get("X-QMigration-Content-SHA256") == "" {
			t.Fatalf("%s missing SHA header", tc.path)
		}
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rr.Body.String(), "qmigration_validation_report_exports_total 4") {
		t.Fatalf("missing report export metric: %s", rr.Body.String())
	}
}

func TestRC47ValidationReportPublicKeyEndpoint(t *testing.T) {
	s := newValidationReportTestServer(t)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/validation-report/public-key", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	for _, required := range []string{"Ed25519", "public-test-key", "fingerprint_sha256"} {
		if !strings.Contains(rr.Body.String(), required) {
			t.Fatalf("public key response missing %q: %s", required, rr.Body.String())
		}
	}
	if rr.Header().Get("X-QMigration-Public-Key-Fingerprint-SHA256") == "" {
		t.Fatal("missing public key fingerprint header")
	}

	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/migrations/m-report/validation-report?format=json", nil))
	if rr.Header().Get("X-QMigration-Content-SHA256") == "" {
		t.Fatal("missing content sha")
	}
}

func TestValidationReportArchiveRequiresConfiguredS3(t *testing.T) {
	t.Setenv("QMIGRATION_VALIDATION_REPORT_S3_ENDPOINT", "")
	t.Setenv("QMIGRATION_VALIDATION_REPORT_S3_BUCKET", "")
	s := newValidationReportTestServer(t)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/migrations/m-report/validation-report/archive", nil))
	if rr.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type rc47APIFakeS3 struct {
	mu      sync.Mutex
	data    map[string][]byte
	headers map[string]http.Header
}

func (f *rc47APIFakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.Method {
	case http.MethodHead:
		b, ok := f.data[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(b)))
		if h := f.headers[r.URL.Path]; h != nil {
			if v := h.Get("x-amz-meta-sha256"); v != "" {
				w.Header().Set("x-amz-meta-sha256", v)
			}
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		b, _ := io.ReadAll(r.Body)
		f.data[r.URL.Path] = b
		f.headers[r.URL.Path] = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		b, ok := f.data[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func TestRC47ValidationReportArchiveRegistersImmutableExternalProof(t *testing.T) {
	fake := &rc47APIFakeS3{data: map[string][]byte{}, headers: map[string]http.Header{}}
	ts := httptest.NewServer(fake)
	defer ts.Close()
	t.Setenv("QMIGRATION_VALIDATION_REPORT_S3_ENDPOINT", ts.URL)
	t.Setenv("QMIGRATION_VALIDATION_REPORT_S3_BUCKET", "reports")
	t.Setenv("QMIGRATION_VALIDATION_REPORT_S3_PREFIX", "acceptance")
	t.Setenv("QMIGRATION_VALIDATION_REPORT_S3_REGION", "us-east-1")
	t.Setenv("QMIGRATION_VALIDATION_REPORT_S3_ACCESS_KEY", "ak")
	t.Setenv("QMIGRATION_VALIDATION_REPORT_S3_SECRET_KEY", "sk")
	t.Setenv("QMIGRATION_VALIDATION_REPORT_S3_PATH_STYLE", "true")
	t.Setenv("QMIGRATION_VALIDATION_REPORT_OBJECT_LOCK_MODE", "OFF")
	srv := newValidationReportTestServer(t)
	h := srv.Handler()
	for i := 0; i < 2; i++ {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/migrations/m-report/validation-report/archive", nil))
		if rr.Code != http.StatusCreated {
			t.Fatalf("archive attempt %d status=%d body=%s", i, rr.Code, rr.Body.String())
		}
		for _, required := range []string{"\"committed\":true", "manifest_sha256", "public_key_fingerprint_sha256"} {
			if !strings.Contains(rr.Body.String(), required) {
				t.Fatalf("archive response missing %q: %s", required, rr.Body.String())
			}
		}
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/migrations/m-report/validation-report/archive", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("registry GET status=%d body=%s", rr.Code, rr.Body.String())
	}
	for _, required := range []string{"s3://reports/", strings.Repeat("a", 64), "Ed25519", "public-test-key", "manifest_sha256"} {
		if !strings.Contains(rr.Body.String(), required) {
			t.Fatalf("registry response missing %q: %s", required, rr.Body.String())
		}
	}
}

func TestRC48KeyTransitionAndRevocationCertificateEndpoints(t *testing.T) {
	s := newValidationReportTestServer(t)
	h := s.Handler()
	seed := bytes.Repeat([]byte{8}, ed25519.SeedSize)
	newSigner := validationreport.Signer{Ed25519PrivateKey: ed25519.NewKeyFromSeed(seed), Ed25519KeyID: "public-next-key"}
	newDoc, err := validationreport.PublicKeyDocumentForSigner(newSigner)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]any{"new_public_key": newDoc, "reason": "scheduled rotation"})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/validation-report/key-transition", bytes.NewReader(body)))
	if rr.Code != http.StatusCreated {
		t.Fatalf("transition status=%d body=%s", rr.Code, rr.Body.String())
	}
	cert, err := validationreport.ParseKeyTransitionCertificate(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if cert.From.KeyID != "public-test-key" || cert.To.KeyID != "public-next-key" {
		t.Fatalf("unexpected transition: %+v", cert)
	}

	// A revocation certificate is signed by the currently configured server key.
	body, _ = json.Marshal(map[string]any{"target_public_key": newDoc, "reason": "new key withdrawn before activation"})
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/validation-report/key-revocation", bytes.NewReader(body)))
	if rr.Code != http.StatusCreated {
		t.Fatalf("revocation status=%d body=%s", rr.Code, rr.Body.String())
	}
	revoke, err := validationreport.ParseKeyRevocationCertificate(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if revoke.Issuer.KeyID != "public-test-key" || revoke.Target.KeyID != "public-next-key" {
		t.Fatalf("unexpected revocation: %+v", revoke)
	}
}
