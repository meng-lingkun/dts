package spools3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/repository"
	"qmigration/backend/internal/repository/memory"
	securerepo "qmigration/backend/internal/repository/secure"
	"qmigration/backend/internal/security"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeS3 struct {
	mu         sync.Mutex
	objects    map[string][]byte
	modified   map[string]time.Time
	sawAuth    bool
	uploads    map[string]map[int][]byte
	nextUpload int
	abortCount int
	initiated  map[string]time.Time
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
		f.sawAuth = true
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Has("uploads") {
		prefix := r.URL.Query().Get("prefix")
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>")
		for id := range f.uploads {
			key := ""
			// Fake uploads are keyed by id, while the object key is reconstructed
			// from a small side-channel stored in initiated map as id|key below.
			for marker := range f.initiated {
				parts := strings.SplitN(marker, "|", 2)
				if len(parts) == 2 && parts[0] == id {
					key = parts[1]
					break
				}
			}
			if key == "" || !strings.HasPrefix(key, prefix) {
				continue
			}
			when := f.initiated[id+"|"+key]
			_, _ = io.WriteString(w, fmt.Sprintf("<Upload><Key>%s</Key><UploadId>%s</UploadId><Initiated>%s</Initiated></Upload>", key, id, when.UTC().Format(time.RFC3339)))
		}
		_, _ = io.WriteString(w, "</ListMultipartUploadsResult>")
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		prefix := r.URL.Query().Get("prefix")
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, "<ListBucketResult><IsTruncated>false</IsTruncated>")
		for k, b := range f.objects {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			mod := f.modified[k]
			if mod.IsZero() {
				mod = time.Now().UTC()
			}
			_, _ = io.WriteString(w, fmt.Sprintf("<Contents><Key>%s</Key><LastModified>%s</LastModified><Size>%d</Size></Contents>", k, mod.UTC().Format(time.RFC3339), len(b)))
		}
		_, _ = io.WriteString(w, "</ListBucketResult>")
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	if r.Method == http.MethodPost && r.URL.Query().Has("uploads") {
		f.nextUpload++
		id := fmt.Sprintf("upload-%d", f.nextUpload)
		if f.uploads == nil {
			f.uploads = map[string]map[int][]byte{}
		}
		f.uploads[id] = map[int][]byte{}
		if f.initiated == nil {
			f.initiated = map[string]time.Time{}
		}
		f.initiated[id+"|"+key] = time.Now().UTC()
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "<InitiateMultipartUploadResult><UploadId>"+id+"</UploadId></InitiateMultipartUploadResult>")
		return
	}
	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		if r.Method == http.MethodPut && r.URL.Query().Get("partNumber") != "" {
			part, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
			b, _ := io.ReadAll(r.Body)
			if f.uploads == nil || f.uploads[uploadID] == nil {
				http.Error(w, "missing upload", 404)
				return
			}
			f.uploads[uploadID][part] = append([]byte(nil), b...)
			w.Header().Set("ETag", fmt.Sprintf("\"etag-%d\"", part))
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodPost {
			parts := f.uploads[uploadID]
			if parts == nil {
				http.Error(w, "missing upload", 404)
				return
			}
			var all []byte
			for i := 1; i <= len(parts); i++ {
				all = append(all, parts[i]...)
			}
			f.objects[key] = all
			f.modified[key] = time.Now().UTC()
			delete(f.uploads, uploadID)
			for marker := range f.initiated {
				if strings.HasPrefix(marker, uploadID+"|") {
					delete(f.initiated, marker)
				}
			}
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "<CompleteMultipartUploadResult/>")
			return
		}
		if r.Method == http.MethodDelete {
			delete(f.uploads, uploadID)
			for marker := range f.initiated {
				if strings.HasPrefix(marker, uploadID+"|") {
					delete(f.initiated, marker)
				}
			}
			f.abortCount++
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	if r.Method == http.MethodPut && r.Header.Get("x-amz-copy-source") != "" {
		src := strings.TrimPrefix(r.Header.Get("x-amz-copy-source"), "/bucket/")
		src = strings.ReplaceAll(src, "%2F", "/")
		b, ok := f.objects[src]
		if !ok {
			http.Error(w, "missing source", 404)
			return
		}
		f.objects[key] = append([]byte(nil), b...)
		f.modified[key] = time.Now().UTC()
		w.WriteHeader(200)
		_, _ = w.Write([]byte("<CopyObjectResult/>"))
		return
	}
	switch r.Method {
	case http.MethodPut:
		b, _ := io.ReadAll(r.Body)
		f.objects[key] = b
		f.modified[key] = time.Now().UTC()
		w.WriteHeader(200)
	case http.MethodGet:
		b, ok := f.objects[key]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write(b)
	case http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(204)
	default:
		http.Error(w, "bad method", 405)
	}
}

func newTestStore(t *testing.T) (*fakeS3, *Store, *securerepo.Store) {
	t.Helper()
	f := &fakeS3{objects: map[string][]byte{}, modified: map[string]time.Time{}, uploads: map[string]map[int][]byte{}, initiated: map[string]time.Time{}}
	ts := httptest.NewServer(f)
	t.Cleanup(ts.Close)
	base := memory.New()
	st, err := New(base, Config{Endpoint: ts.URL, Bucket: "bucket", Prefix: "qmigration/test", Region: "us-east-1", AccessKey: "AKID", SecretKey: "SECRET", PathStyle: true, AppliedRetention: time.Hour, HTTPClient: ts.Client(), MaxPendingBytes: 1024 * 1024, WarnUsedPct: 80, CriticalUsedPct: 90})
	if err != nil {
		t.Fatal(err)
	}
	cipher, err := security.New("spools3-test-master")
	if err != nil {
		t.Fatal(err)
	}
	return f, st, securerepo.New(st, cipher)
}

func TestS3SpoolEncryptsOutsideMetadataAndHydrates(t *testing.T) {
	f, st, repo := newTestStore(t)
	ctx := context.Background()
	if err := st.CDCSpoolStorageReady(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	_ = repo.CreateMigration(ctx, &domain.MigrationTask{ID: "task", Name: "task", CreatedAt: now, UpdatedAt: now})
	rec := &domain.CDCSpoolRecord{ID: "r1", TaskID: "task", Direction: "forward", PositionType: "GTID", PositionValue: "g:1", Events: []domain.CDCEvent{{Operation: domain.CDCInsert, After: []domain.CDCField{{Column: "secret", Value: "do-not-leak"}}}}, Status: domain.CDCSpoolPending, CreatedAt: now}
	if err := repo.CreateCDCSpool(ctx, rec); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	if !f.sawAuth {
		t.Fatal("SigV4 Authorization was not sent")
	}
	if len(f.objects) != 1 {
		t.Fatalf("objects=%d", len(f.objects))
	}
	for _, b := range f.objects {
		if strings.Contains(string(b), "do-not-leak") {
			t.Fatal("plaintext leaked to S3")
		}
	}
	f.mu.Unlock()
	rows, err := repo.ListCDCSpool(ctx, "task", "forward", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || len(rows[0].Events) != 1 || rows[0].Events[0].After[0].Value != "do-not-leak" {
		t.Fatalf("rows=%+v", rows)
	}
	stats, err := repo.CDCSpoolStats(ctx, "task", "forward")
	if err != nil {
		t.Fatal(err)
	}
	if stats.StorageBackend != "s3" || stats.StorageCapacityBytes <= 0 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestS3MarkAppliedCopiesThenDeletesPending(t *testing.T) {
	f, _, repo := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	_ = repo.CreateMigration(ctx, &domain.MigrationTask{ID: "task", Name: "task", CreatedAt: now, UpdatedAt: now})
	rec := &domain.CDCSpoolRecord{ID: "r2", TaskID: "task", Direction: "forward", PositionType: "LSN", PositionValue: "0/10", Events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "LSN", PositionValue: "0/10"}}, Status: domain.CDCSpoolPending, CreatedAt: now}
	if err := repo.CreateCDCSpool(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkCDCSpoolApplied(ctx, rec.ID, now); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	pending, applied := 0, 0
	for k := range f.objects {
		if strings.Contains(k, "/pending/") {
			pending++
		}
		if strings.Contains(k, "/applied/") {
			applied++
		}
	}
	if pending != 0 || applied != 1 {
		t.Fatalf("pending=%d applied=%d objects=%v", pending, applied, f.objects)
	}
}

func TestS3SignatureUsesConfiguredSessionToken(t *testing.T) {
	var token, auth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token = r.Header.Get("x-amz-security-token")
		auth = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	defer ts.Close()
	c, err := newS3Client(Config{Endpoint: ts.URL, Bucket: "bucket", Region: "ap-southeast-1", AccessKey: "a", SecretKey: "b", SessionToken: "token123", PathStyle: true, HTTPClient: ts.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.HeadBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	if token != "token123" || !strings.Contains(auth, "Credential=a/") {
		t.Fatalf("token=%q auth=%q", token, auth)
	}
}

func TestS3AppliedGCAndOrphanReconcile(t *testing.T) {
	f, st, repo := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	_ = repo.CreateMigration(ctx, &domain.MigrationTask{ID: "task", Name: "task", CreatedAt: now, UpdatedAt: now})
	rec := &domain.CDCSpoolRecord{ID: "live", TaskID: "task", Direction: "forward", PositionType: "GTID", PositionValue: "g:2", Events: []domain.CDCEvent{{Operation: domain.CDCCheckpoint, PositionType: "GTID", PositionValue: "g:2"}}, Status: domain.CDCSpoolPending, CreatedAt: now}
	if err := repo.CreateCDCSpool(ctx, rec); err != nil {
		t.Fatal(err)
	}
	// Simulate an object persisted before a crash, but without a metadata commit.
	orphanKey := st.key("pending", "ff", "orphan.blob")
	f.mu.Lock()
	f.objects[orphanKey] = []byte("ciphertext")
	f.modified[orphanKey] = now
	f.mu.Unlock()
	if err := st.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	orphanPending := false
	orphanRecovered := false
	for k := range f.objects {
		if k == orphanKey {
			orphanPending = true
		}
		if strings.Contains(k, "recovered-orphans") && strings.HasSuffix(k, "orphan.blob") {
			orphanRecovered = true
		}
	}
	f.mu.Unlock()
	if orphanPending || !orphanRecovered {
		t.Fatalf("orphan reconcile failed pending=%v recovered=%v", orphanPending, orphanRecovered)
	}
	if err := repo.MarkCDCSpoolApplied(ctx, rec.ID, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// Fake S3 COPY timestamps are 'now'; mark the applied object old so retention GC is deterministic.
	f.mu.Lock()
	for k := range f.objects {
		if strings.Contains(k, "/applied/") && !strings.Contains(k, "recovered-orphans") {
			f.modified[k] = now.Add(-2 * time.Hour)
		}
	}
	f.mu.Unlock()
	st.cfg.AppliedRetention = time.Hour
	if err := st.PurgeApplied(ctx, now); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := range f.objects {
		if strings.Contains(k, "/applied/") && !strings.Contains(k, "recovered-orphans") {
			t.Fatalf("old applied object not purged: %s", k)
		}
	}
}

func TestS3MultipartUploadAndIntegrityReference(t *testing.T) {
	f, st, repo := newTestStore(t)
	st.cfg.MultipartThreshold = 1024
	st.cfg.MultipartPartSize = 5 << 20
	ctx := context.Background()
	now := time.Now()
	_ = repo.CreateMigration(ctx, &domain.MigrationTask{ID: "task", Name: "task", CreatedAt: now, UpdatedAt: now})
	// Use high-entropy input so secure gzip does not shrink below the multipart threshold.
	value := make([]byte, 128<<10)
	alphabet := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	var x uint32 = 0x12345678
	for i := range value {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		value[i] = alphabet[int(x)%len(alphabet)]
	}
	rec := &domain.CDCSpoolRecord{ID: "multipart", TaskID: "task", Direction: "forward", PositionType: "LSN", PositionValue: "10", Events: []domain.CDCEvent{{Operation: domain.CDCInsert, After: []domain.CDCField{{Column: "payload", Value: string(value)}}}}, Status: domain.CDCSpoolPending, CreatedAt: now}
	if err := repo.CreateCDCSpool(ctx, rec); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	if f.nextUpload == 0 {
		f.mu.Unlock()
		t.Fatal("expected multipart upload")
	}
	var objectKey string
	for k := range f.objects {
		if strings.Contains(k, "/pending/") {
			objectKey = k
		}
	}
	f.mu.Unlock()
	if objectKey == "" {
		t.Fatal("multipart object missing")
	}
	baseRows, err := st.Repository.ListCDCSpool(ctx, "task", "forward", 10)
	if err != nil || len(baseRows) != 1 {
		t.Fatalf("metadata rows=%v err=%v", baseRows, err)
	}
	if !strings.HasPrefix(baseRows[0].EventsCiphertext, refPrefixV2) {
		t.Fatalf("reference=%q", baseRows[0].EventsCiphertext)
	}
	rows, err := repo.ListCDCSpool(ctx, "task", "forward", 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("hydrate err=%v rows=%d", err, len(rows))
	}
	if rows[0].Events[0].After[0].Value != string(value) {
		t.Fatal("multipart hydrate payload mismatch")
	}

	// Corrupt the encrypted object: v2 reference must detect storage corruption before decrypt/apply.
	f.mu.Lock()
	f.objects[objectKey][0] ^= 0xff
	f.mu.Unlock()
	_, err = repo.ListCDCSpool(ctx, "task", "forward", 10)
	if err == nil || !strings.Contains(err.Error(), "integrity check failed") {
		t.Fatalf("expected integrity failure, got %v", err)
	}
}

func TestS3MultipartFailureAbortsUpload(t *testing.T) {
	var mu sync.Mutex
	uploadID := "u1"
	abort := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.Method == http.MethodPost && q.Has("uploads") {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "<InitiateMultipartUploadResult><UploadId>"+uploadID+"</UploadId></InitiateMultipartUploadResult>")
			return
		}
		if r.Method == http.MethodPut && q.Get("partNumber") == "1" {
			http.Error(w, "injected", http.StatusInternalServerError)
			return
		}
		if r.Method == http.MethodDelete && q.Get("uploadId") == uploadID {
			mu.Lock()
			abort++
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "unexpected", http.StatusBadRequest)
	}))
	defer ts.Close()
	c, err := newS3Client(Config{Endpoint: ts.URL, Bucket: "bucket", Region: "us-east-1", AccessKey: "a", SecretKey: "b", PathStyle: true, HTTPClient: ts.Client()})
	if err != nil {
		t.Fatal(err)
	}
	err = c.PutMultipart(context.Background(), "pending/x.blob", make([]byte, 6<<20), 5<<20)
	if err == nil {
		t.Fatal("expected multipart failure")
	}
	mu.Lock()
	got := abort
	mu.Unlock()
	if got != 1 {
		t.Fatalf("abort count=%d", got)
	}
}

func TestS3TLSConfigFailsClosedOnInvalidMaterial(t *testing.T) {
	_, err := newS3Client(Config{Endpoint: "https://s3.example.invalid", Bucket: "b", Region: "us-east-1", AccessKey: "a", SecretKey: "b", PathStyle: true, CACert: "not-pem"})
	if err == nil || !strings.Contains(err.Error(), "CA PEM") {
		t.Fatalf("expected CA validation error, got %v", err)
	}
	_, err = newS3Client(Config{Endpoint: "https://s3.example.invalid", Bucket: "b", Region: "us-east-1", AccessKey: "a", SecretKey: "b", PathStyle: true, TLSClientCert: "cert-only"})
	if err == nil || !strings.Contains(err.Error(), "mTLS requires both") {
		t.Fatalf("expected mTLS pair validation error, got %v", err)
	}
}

func TestConfigFromEnvWiresTLSAndMultipart(t *testing.T) {
	t.Setenv("QMIGRATION_CDC_SPOOL_S3_ENDPOINT", "https://minio.internal")
	t.Setenv("QMIGRATION_CDC_SPOOL_S3_BUCKET", "cdc")
	t.Setenv("QMIGRATION_CDC_SPOOL_S3_CA_CERT", "ca-pem")
	t.Setenv("QMIGRATION_CDC_SPOOL_S3_TLS_SERVER_NAME", "minio.service.local")
	t.Setenv("QMIGRATION_CDC_SPOOL_S3_TLS_CLIENT_CERT", "cert-pem")
	t.Setenv("QMIGRATION_CDC_SPOOL_S3_TLS_CLIENT_KEY", "key-pem")
	t.Setenv("QMIGRATION_CDC_SPOOL_S3_MULTIPART_THRESHOLD_BYTES", "10485760")
	t.Setenv("QMIGRATION_CDC_SPOOL_S3_MULTIPART_PART_BYTES", "6291456")
	t.Setenv("QMIGRATION_CDC_SPOOL_S3_MULTIPART_ABORT_AFTER_HOURS", "9")
	cfg := ConfigFromEnv()
	if cfg.CACert != "ca-pem" || cfg.TLSServerName != "minio.service.local" || cfg.TLSClientCert != "cert-pem" || cfg.TLSClientKey != "key-pem" {
		t.Fatalf("TLS env not wired: %+v", cfg)
	}
	if cfg.MultipartThreshold != 10485760 || cfg.MultipartPartSize != 6291456 || cfg.MultipartAbortAfter != 9*time.Hour {
		t.Fatalf("multipart env not wired: threshold=%d part=%d abort=%s", cfg.MultipartThreshold, cfg.MultipartPartSize, cfg.MultipartAbortAfter)
	}
}

func TestS3ReconcileAbortsOnlyStaleMultipartUploads(t *testing.T) {
	f, st, _ := newTestStore(t)
	ctx := context.Background()
	st.cfg.MultipartAbortAfter = time.Hour
	// Create two in-progress multipart uploads directly through the client.
	create := func(key string) string {
		resp, err := st.s3.do(ctx, http.MethodPost, key, mapToValues(map[string]string{"uploads": ""}), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		text := string(b)
		start := strings.Index(text, "<UploadId>") + len("<UploadId>")
		end := strings.Index(text, "</UploadId>")
		if start < len("<UploadId>") || end <= start {
			t.Fatalf("bad upload response %q", text)
		}
		return text[start:end]
	}
	oldID := create(st.key("pending", "aa", "old.blob"))
	freshID := create(st.key("pending", "aa", "fresh.blob"))
	f.mu.Lock()
	for marker := range f.initiated {
		if strings.HasPrefix(marker, oldID+"|") {
			f.initiated[marker] = time.Now().Add(-2 * time.Hour)
		}
		if strings.HasPrefix(marker, freshID+"|") {
			f.initiated[marker] = time.Now()
		}
	}
	f.mu.Unlock()
	if err := st.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.uploads[oldID]; ok {
		t.Fatal("stale multipart upload was not aborted")
	}
	if _, ok := f.uploads[freshID]; !ok {
		t.Fatal("fresh multipart upload was incorrectly aborted")
	}
}

func mapToValues(m map[string]string) url.Values {
	v := url.Values{}
	for k, x := range m {
		v.Set(k, x)
	}
	return v
}

func TestRC44MetadataMaintenanceSurvivesSecureAndS3Decorators(t *testing.T) {
	_, _, repo := newTestStore(t)
	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour)
	if err := repo.CreateTaskLog(ctx, &domain.TaskLog{ID: "old-log", TaskID: "task", Level: "INFO", Message: "old", CreatedAt: old}); err != nil {
		t.Fatal(err)
	}
	m, ok := any(repo).(repository.MetadataMaintenance)
	if !ok {
		t.Fatal("metadata maintenance capability lost through secure/S3 decorators")
	}
	res, err := m.PruneMetadata(ctx, repository.MetadataRetentionPolicy{TaskLogMaxAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if res.TaskLogsDeleted != 1 {
		t.Fatalf("expected old log to be pruned through S3 wrapper, got %+v", res)
	}
	logs, err := repo.ListTaskLogs(ctx, "task", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Fatalf("old log survived maintenance: %+v", logs)
	}
}
