package validationreport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeReportS3 struct {
	mu       sync.Mutex
	data     map[string][]byte
	headers  map[string]http.Header
	omitLock bool
}

func (f *fakeReportS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := r.URL.Path
	switch r.Method {
	case http.MethodHead:
		b, ok := f.data[key]
		if !ok {
			w.WriteHeader(404)
			return
		}
		h := f.headers[key]
		w.Header().Set("Content-Length", stringInt(len(b)))
		for _, k := range []string{"x-amz-meta-sha256", "x-amz-object-lock-mode", "x-amz-object-lock-retain-until-date", "x-amz-object-lock-legal-hold"} {
			if f.omitLock && strings.HasPrefix(k, "x-amz-object-lock-") {
				continue
			}
			if v := h.Get(k); v != "" {
				w.Header().Set(k, v)
			}
		}
		w.WriteHeader(200)
	case http.MethodPut:
		b, _ := io.ReadAll(r.Body)
		f.data[key] = b
		f.headers[key] = r.Header.Clone()
		w.WriteHeader(200)
	case http.MethodGet:
		b, ok := f.data[key]
		if !ok {
			w.WriteHeader(404)
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write(b)
	default:
		w.WriteHeader(405)
	}
}
func stringInt(v int) string {
	if v == 0 {
		return "0"
	}
	b := make([]byte, 0, 20)
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}

func TestArchiveBundleS3ObjectLockAndIdempotency(t *testing.T) {
	fake := &fakeReportS3{data: map[string][]byte{}, headers: map[string]http.Header{}}
	ts := httptest.NewServer(fake)
	defer ts.Close()
	b, err := BuildBundle(testReport(t), Signer{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := S3Config{Endpoint: ts.URL, Bucket: "reports", Prefix: "acceptance", Region: "ap-southeast-1", AccessKey: "a", SecretKey: "b", PathStyle: true, ObjectLockMode: "COMPLIANCE", RetentionDays: 30, LegalHold: true, HTTPClient: ts.Client()}
	res, err := ArchiveBundleS3(context.Background(), cfg, b)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Committed || len(res.Objects) < 6 {
		t.Fatalf("bad archive result %+v", res)
	}
	if res.ObjectLockMode != "COMPLIANCE" || !res.LegalHold || res.RetainUntil == "" || res.ManifestSHA256 != SHA256Hex(b.ManifestJSON) {
		t.Fatalf("archive WORM/manifest summary missing: %+v", res)
	}
	readyKey := "/reports/" + res.Prefix + "/READY.json"
	if _, ok := fake.data[readyKey]; !ok {
		t.Fatalf("READY commit marker missing: %s", readyKey)
	}
	for k, h := range fake.headers {
		if !strings.HasPrefix(k, "/reports/"+res.Prefix+"/") {
			continue
		}
		auth := h.Get("Authorization")
		for _, signed := range []string{"x-amz-meta-sha256", "x-amz-object-lock-mode", "x-amz-object-lock-retain-until-date", "x-amz-object-lock-legal-hold"} {
			if !strings.Contains(auth, signed) {
				t.Fatalf("Object Lock/integrity header %s was not signed for %s: %s", signed, k, auth)
			}
		}
		if h.Get("x-amz-object-lock-mode") != "COMPLIANCE" || h.Get("x-amz-object-lock-legal-hold") != "ON" {
			t.Fatalf("missing WORM headers for %s: %v", k, h)
		}
		if until, err := time.Parse(time.RFC3339, h.Get("x-amz-object-lock-retain-until-date")); err != nil || !until.After(time.Now()) {
			t.Fatalf("bad retain-until for %s", k)
		}
	}
	res2, err := ArchiveBundleS3(context.Background(), cfg, b)
	if err != nil {
		t.Fatal(err)
	}
	if !res2.Committed {
		t.Fatal("idempotent archive not committed")
	}
	for _, o := range res2.Objects {
		if !o.Existing {
			t.Fatalf("expected existing object on second archive: %+v", o)
		}
	}
}

func TestArchiveBundleS3FailsClosedWhenObjectLockNotObservable(t *testing.T) {
	fake := &fakeReportS3{data: map[string][]byte{}, headers: map[string]http.Header{}, omitLock: true}
	ts := httptest.NewServer(fake)
	defer ts.Close()
	b, _ := BuildBundle(testReport(t), Signer{})
	cfg := S3Config{Endpoint: ts.URL, Bucket: "reports", Region: "us-east-1", AccessKey: "a", SecretKey: "b", PathStyle: true, ObjectLockMode: "GOVERNANCE", RetentionDays: 1, HTTPClient: ts.Client()}
	if _, err := ArchiveBundleS3(context.Background(), cfg, b); err == nil || !strings.Contains(err.Error(), "Object Lock") {
		t.Fatalf("expected fail-closed Object Lock error, got %v", err)
	}
}

func TestArchiveBundleS3RejectsExistingDifferentContent(t *testing.T) {
	fake := &fakeReportS3{data: map[string][]byte{}, headers: map[string]http.Header{}}
	ts := httptest.NewServer(fake)
	defer ts.Close()
	b, _ := BuildBundle(testReport(t), Signer{})
	cfg := S3Config{Endpoint: ts.URL, Bucket: "reports", Prefix: "p", Region: "us-east-1", AccessKey: "a", SecretKey: "b", PathStyle: true, HTTPClient: ts.Client()}
	res, err := ArchiveBundleS3(context.Background(), cfg, b)
	if err != nil {
		t.Fatal(err)
	}
	key := "/reports/" + res.Prefix + "/" + b.Artifacts[0].Name
	fake.mu.Lock()
	fake.headers[key].Set("x-amz-meta-sha256", strings.Repeat("0", 64))
	fake.mu.Unlock()
	if _, err := ArchiveBundleS3(context.Background(), cfg, b); err == nil || !strings.Contains(err.Error(), "different SHA-256") {
		t.Fatalf("expected immutable mismatch, got %v", err)
	}
}

func TestRC47ArchiveBundleS3IncludesPublicSignaturesAndManifestDigest(t *testing.T) {
	fake := &fakeReportS3{data: map[string][]byte{}, headers: map[string]http.Header{}}
	ts := httptest.NewServer(fake)
	defer ts.Close()
	priv := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{9}, ed25519.SeedSize))
	b, err := BuildBundle(testReport(t), Signer{Ed25519PrivateKey: priv, Ed25519KeyID: "public-proof"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := S3Config{Endpoint: ts.URL, Bucket: "reports", Prefix: "rc47", Region: "us-east-1", AccessKey: "a", SecretKey: "b", PathStyle: true, HTTPClient: ts.Client()}
	res, err := ArchiveBundleS3(context.Background(), cfg, b)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Committed || res.ManifestSHA256 != SHA256Hex(b.ManifestJSON) {
		t.Fatalf("bad archive result: %+v", res)
	}
	key := "/reports/" + res.Prefix + "/ED25519SIGNATURES"
	if got := fake.data[key]; len(got) == 0 {
		t.Fatalf("missing Ed25519 signature index %s", key)
	}
}
