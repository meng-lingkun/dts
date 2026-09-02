package validationreport

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

type S3Config struct {
	Endpoint, Bucket, Prefix, Region                   string
	AccessKey, SecretKey, SessionToken                 string
	PathStyle                                          bool
	CACert, TLSServerName, TLSClientCert, TLSClientKey string
	ObjectLockMode                                     string
	RetentionDays                                      int
	LegalHold                                          bool
	HTTPClient                                         *http.Client
}

type ArchivedObject struct {
	Key            string `json:"key"`
	SHA256         string `json:"sha256"`
	SizeBytes      int64  `json:"size_bytes"`
	Existing       bool   `json:"existing"`
	ObjectLockMode string `json:"object_lock_mode,omitempty"`
	RetainUntil    string `json:"retain_until,omitempty"`
	LegalHold      bool   `json:"legal_hold,omitempty"`
}

type ArchiveResult struct {
	URI            string           `json:"uri"`
	Bucket         string           `json:"bucket"`
	Prefix         string           `json:"prefix"`
	EvidenceDigest string           `json:"evidence_digest"`
	ManifestSHA256 string           `json:"manifest_sha256"`
	ObjectLockMode string           `json:"object_lock_mode,omitempty"`
	RetainUntil    string           `json:"retain_until,omitempty"`
	LegalHold      bool             `json:"legal_hold,omitempty"`
	Objects        []ArchivedObject `json:"objects"`
	Committed      bool             `json:"committed"`
}

func envBool(k string, d bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(k)))
	if v == "" {
		return d
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return d
}
func envInt(k string, d int) int {
	v, e := strconv.Atoi(strings.TrimSpace(os.Getenv(k)))
	if e != nil || v < 0 {
		return d
	}
	return v
}

func S3ConfigFromEnv() S3Config {
	return S3Config{
		Endpoint:  strings.TrimRight(strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_S3_ENDPOINT")), "/"),
		Bucket:    strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_S3_BUCKET")),
		Prefix:    strings.Trim(strings.TrimSpace(envString("QMIGRATION_VALIDATION_REPORT_S3_PREFIX", "qmigration/validation-reports")), "/"),
		Region:    strings.TrimSpace(envString("QMIGRATION_VALIDATION_REPORT_S3_REGION", "us-east-1")),
		AccessKey: os.Getenv("QMIGRATION_VALIDATION_REPORT_S3_ACCESS_KEY"), SecretKey: os.Getenv("QMIGRATION_VALIDATION_REPORT_S3_SECRET_KEY"), SessionToken: os.Getenv("QMIGRATION_VALIDATION_REPORT_S3_SESSION_TOKEN"),
		PathStyle: envBool("QMIGRATION_VALIDATION_REPORT_S3_PATH_STYLE", true),
		CACert:    os.Getenv("QMIGRATION_VALIDATION_REPORT_S3_CA_CERT"), TLSServerName: strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_S3_TLS_SERVER_NAME")), TLSClientCert: os.Getenv("QMIGRATION_VALIDATION_REPORT_S3_TLS_CLIENT_CERT"), TLSClientKey: os.Getenv("QMIGRATION_VALIDATION_REPORT_S3_TLS_CLIENT_KEY"),
		ObjectLockMode: strings.ToUpper(strings.TrimSpace(envString("QMIGRATION_VALIDATION_REPORT_OBJECT_LOCK_MODE", "OFF"))),
		RetentionDays:  envInt("QMIGRATION_VALIDATION_REPORT_OBJECT_LOCK_RETENTION_DAYS", 365),
		LegalHold:      envBool("QMIGRATION_VALIDATION_REPORT_OBJECT_LOCK_LEGAL_HOLD", false),
	}
}
func envString(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}

func (c S3Config) Configured() bool {
	return strings.TrimSpace(c.Endpoint) != "" || strings.TrimSpace(c.Bucket) != ""
}

func (c S3Config) validate() error {
	if !c.Configured() {
		return errors.New("validation report S3 archive is not configured")
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("invalid validation report S3 endpoint %q", c.Endpoint)
	}
	if c.Bucket == "" {
		return errors.New("validation report S3 bucket is required")
	}
	if strings.TrimSpace(c.AccessKey) == "" || strings.TrimSpace(c.SecretKey) == "" {
		return errors.New("validation report S3 access key and secret key are required")
	}
	switch c.ObjectLockMode {
	case "", "OFF":
	case "GOVERNANCE", "COMPLIANCE":
		if c.RetentionDays <= 0 {
			return errors.New("Object Lock retention days must be > 0")
		}
	default:
		return fmt.Errorf("unsupported validation report Object Lock mode %q", c.ObjectLockMode)
	}
	return nil
}

type reportS3Client struct {
	cfg      S3Config
	endpoint *url.URL
	http     *http.Client
}

func newReportS3Client(cfg S3Config) (*reportS3Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	u, _ := url.Parse(cfg.Endpoint)
	hc := cfg.HTTPClient
	if hc == nil {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: strings.TrimSpace(cfg.TLSServerName)}
		if strings.TrimSpace(cfg.CACert) != "" {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(cfg.CACert)) {
				return nil, errors.New("invalid validation report S3 CA PEM")
			}
			tlsCfg.RootCAs = pool
		}
		certPEM, keyPEM := strings.TrimSpace(cfg.TLSClientCert), strings.TrimSpace(cfg.TLSClientKey)
		if certPEM != "" || keyPEM != "" {
			if certPEM == "" || keyPEM == "" {
				return nil, errors.New("validation report S3 mTLS requires both certificate and key")
			}
			cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
			if err != nil {
				return nil, fmt.Errorf("load validation report S3 client certificate: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = tlsCfg
		hc = &http.Client{Timeout: 20 * time.Second, Transport: tr}
	}
	return &reportS3Client{cfg: cfg, endpoint: u, http: hc}, nil
}
func s3SHA(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func s3HMAC(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(data))
	return h.Sum(nil)
}
func awsEsc(v string) string {
	x := url.QueryEscape(v)
	x = strings.ReplaceAll(x, "+", "%20")
	x = strings.ReplaceAll(x, "%7E", "~")
	return x
}
func canonicalQ(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := []string{}
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		if len(vals) == 0 {
			vals = []string{""}
		}
		for _, v := range vals {
			parts = append(parts, awsEsc(k)+"="+awsEsc(v))
		}
	}
	return strings.Join(parts, "&")
}
func uriPath(v string) string {
	parts := strings.Split(v, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	out := strings.Join(parts, "/")
	out = strings.ReplaceAll(out, "+", "%20")
	if !strings.HasPrefix(out, "/") {
		out = "/" + out
	}
	return out
}

func (c *reportS3Client) objectURL(key string) (*url.URL, string) {
	u := *c.endpoint
	prefix := strings.TrimSuffix(u.Path, "/")
	clean := strings.TrimPrefix(path.Clean("/"+key), "/")
	if c.cfg.PathStyle {
		u.Path = prefix + "/" + c.cfg.Bucket
		if clean != "" && clean != "." {
			u.Path += "/" + clean
		}
	} else {
		u.Host = c.cfg.Bucket + "." + u.Host
		u.Path = prefix
		if clean != "" && clean != "." {
			u.Path += "/" + clean
		}
		if u.Path == "" {
			u.Path = "/"
		}
	}
	return &u, u.EscapedPath()
}
func (c *reportS3Client) do(ctx context.Context, method, key string, body []byte, extra http.Header) (*http.Response, error) {
	u, canonicalURI := c.objectURL(key)
	payloadHash := s3SHA(body)
	now := time.Now().UTC()
	amz := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-amz-date", amz)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if c.cfg.SessionToken != "" {
		req.Header.Set("x-amz-security-token", c.cfg.SessionToken)
	}
	for k, vals := range extra {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	headers := map[string]string{"host": u.Host, "x-amz-content-sha256": payloadHash, "x-amz-date": amz}
	if c.cfg.SessionToken != "" {
		headers["x-amz-security-token"] = c.cfg.SessionToken
	}
	for k, vals := range extra {
		lk := strings.ToLower(strings.TrimSpace(k))
		if lk == "authorization" {
			continue
		}
		vv := []string{}
		for _, v := range vals {
			vv = append(vv, strings.Join(strings.Fields(v), " "))
		}
		headers[lk] = strings.Join(vv, ",")
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	var ch strings.Builder
	for _, k := range names {
		ch.WriteString(k)
		ch.WriteByte(':')
		ch.WriteString(headers[k])
		ch.WriteByte('\n')
	}
	signed := strings.Join(names, ";")
	canonical := method + "\n" + canonicalURI + "\n\n" + ch.String() + "\n" + signed + "\n" + payloadHash
	scope := date + "/" + c.cfg.Region + "/s3/aws4_request"
	sts := "AWS4-HMAC-SHA256\n" + amz + "\n" + scope + "\n" + s3SHA([]byte(canonical))
	kd := s3HMAC([]byte("AWS4"+c.cfg.SecretKey), date)
	kr := s3HMAC(kd, c.cfg.Region)
	ks := s3HMAC(kr, "s3")
	ksign := s3HMAC(ks, "aws4_request")
	sig := hex.EncodeToString(s3HMAC(ksign, sts))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.cfg.AccessKey+"/"+scope+", SignedHeaders="+signed+", Signature="+sig)
	return c.http.Do(req)
}

type headInfo struct {
	Exists                                   bool
	SHA256, LockMode, RetainUntil, LegalHold string
	Size                                     int64
}

func (c *reportS3Client) head(ctx context.Context, key string) (headInfo, error) {
	resp, err := c.do(ctx, http.MethodHead, key, nil, nil)
	if err != nil {
		return headInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return headInfo{}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return headInfo{}, fmt.Errorf("validation report S3 HEAD failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return headInfo{Exists: true, SHA256: strings.TrimSpace(resp.Header.Get("x-amz-meta-sha256")), LockMode: strings.ToUpper(strings.TrimSpace(resp.Header.Get("x-amz-object-lock-mode"))), RetainUntil: strings.TrimSpace(resp.Header.Get("x-amz-object-lock-retain-until-date")), LegalHold: strings.ToUpper(strings.TrimSpace(resp.Header.Get("x-amz-object-lock-legal-hold"))), Size: resp.ContentLength}, nil
}

func (c *reportS3Client) get(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.do(ctx, http.MethodGet, key, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("validation report S3 GET failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return io.ReadAll(io.LimitReader(resp.Body, 32<<20))
}

func (c *reportS3Client) putImmutable(ctx context.Context, key, contentType string, data []byte) (ArchivedObject, error) {
	want := s3SHA(data)
	existing, err := c.head(ctx, key)
	if err != nil {
		return ArchivedObject{}, err
	}
	verify := func(h headInfo) error {
		if h.SHA256 == "" {
			return fmt.Errorf("immutable report object %s is missing x-amz-meta-sha256", key)
		}
		if !strings.EqualFold(h.SHA256, want) {
			return fmt.Errorf("immutable report object %s already exists with different SHA-256", key)
		}
		if c.cfg.ObjectLockMode != "" && c.cfg.ObjectLockMode != "OFF" {
			if h.LockMode != c.cfg.ObjectLockMode {
				return fmt.Errorf("report object %s Object Lock mode=%q expected=%q", key, h.LockMode, c.cfg.ObjectLockMode)
			}
			if h.RetainUntil == "" {
				return fmt.Errorf("report object %s is missing Object Lock retain-until", key)
			}
			if t, e := time.Parse(time.RFC3339, h.RetainUntil); e != nil || !t.After(time.Now().UTC()) {
				if !c.cfg.LegalHold || h.LegalHold != "ON" {
					return fmt.Errorf("report object %s Object Lock retention is not active", key)
				}
			}
		}
		if c.cfg.LegalHold && h.LegalHold != "ON" {
			return fmt.Errorf("report object %s legal hold is not ON", key)
		}
		actual, err := c.get(ctx, key)
		if err != nil {
			return err
		}
		if s3SHA(actual) != want {
			return fmt.Errorf("immutable report object %s content SHA-256 does not match metadata", key)
		}
		return nil
	}
	if existing.Exists {
		if err := verify(existing); err != nil {
			return ArchivedObject{}, err
		}
		return ArchivedObject{Key: key, SHA256: want, SizeBytes: int64(len(data)), Existing: true, ObjectLockMode: existing.LockMode, RetainUntil: existing.RetainUntil, LegalHold: existing.LegalHold == "ON"}, nil
	}
	h := make(http.Header)
	h.Set("Content-Type", contentType)
	h.Set("x-amz-meta-sha256", want)
	retainUntil := ""
	if c.cfg.ObjectLockMode != "" && c.cfg.ObjectLockMode != "OFF" {
		retainUntil = time.Now().UTC().Add(time.Duration(c.cfg.RetentionDays) * 24 * time.Hour).Truncate(time.Second).Format(time.RFC3339)
		h.Set("x-amz-object-lock-mode", c.cfg.ObjectLockMode)
		h.Set("x-amz-object-lock-retain-until-date", retainUntil)
	}
	if c.cfg.LegalHold {
		h.Set("x-amz-object-lock-legal-hold", "ON")
	}
	resp, err := c.do(ctx, http.MethodPut, key, data, h)
	if err != nil {
		return ArchivedObject{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return ArchivedObject{}, fmt.Errorf("validation report S3 PUT failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	got, err := c.head(ctx, key)
	if err != nil {
		return ArchivedObject{}, err
	}
	if !got.Exists {
		return ArchivedObject{}, fmt.Errorf("validation report S3 object %s missing after PUT", key)
	}
	if err := verify(got); err != nil {
		return ArchivedObject{}, err
	}
	return ArchivedObject{Key: key, SHA256: want, SizeBytes: int64(len(data)), ObjectLockMode: got.LockMode, RetainUntil: got.RetainUntil, LegalHold: got.LegalHold == "ON"}, nil
}

func ArchiveBundleS3(ctx context.Context, cfg S3Config, b Bundle) (ArchiveResult, error) {
	c, err := newReportS3Client(cfg)
	if err != nil {
		return ArchiveResult{}, err
	}
	digest := strings.ToLower(strings.TrimSpace(b.Report.Validation.EvidenceDigest))
	if digest == "" {
		return ArchiveResult{}, errors.New("validation report evidence digest is empty")
	}
	prefix := path.Join(strings.Trim(cfg.Prefix, "/"), safeBase(b.Report.Task.ID), digest)
	result := ArchiveResult{URI: "s3://" + cfg.Bucket + "/" + prefix, Bucket: cfg.Bucket, Prefix: prefix, EvidenceDigest: digest, ManifestSHA256: s3SHA(b.ManifestJSON)}
	put := func(name, ct string, data []byte) error {
		o, e := c.putImmutable(ctx, path.Join(prefix, name), ct, data)
		if e != nil {
			return e
		}
		result.Objects = append(result.Objects, o)
		return nil
	}
	for _, a := range b.Artifacts {
		if err := put(a.Name, a.ContentType, a.Data); err != nil {
			return result, err
		}
	}
	if err := put("SHA256SUMS", "text/plain; charset=utf-8", b.SHA256SUMS); err != nil {
		return result, err
	}
	if len(b.HMACSHA256SUMS) > 0 {
		if err := put("HMACSHA256SUMS", "text/plain; charset=utf-8", b.HMACSHA256SUMS); err != nil {
			return result, err
		}
	}
	if len(b.ED25519SIGNATURES) > 0 {
		if err := put("ED25519SIGNATURES", "text/plain; charset=utf-8", b.ED25519SIGNATURES); err != nil {
			return result, err
		}
	}
	if len(b.TimestampToken) > 0 {
		if err := put("manifest.tsr", "application/timestamp-reply", b.TimestampToken); err != nil {
			return result, err
		}
	}
	if len(b.TimestampJSON) > 0 {
		if err := put("timestamp.json", "application/json; charset=utf-8", b.TimestampJSON); err != nil {
			return result, err
		}
	}
	if err := put("manifest.json", "application/json; charset=utf-8", b.ManifestJSON); err != nil {
		return result, err
	}
	readyBytes, _ := json.MarshalIndent(map[string]any{"schema_version": SchemaVersion + "/archive-ready", "task_id": b.Report.Task.ID, "archive_evidence_digest": digest, "manifest_sha256": s3SHA(b.ManifestJSON), "generated_at": b.Report.GeneratedAt}, "", "  ")
	readyBytes = append(readyBytes, '\n')
	if err := put("READY.json", "application/json; charset=utf-8", readyBytes); err != nil {
		return result, err
	}
	if len(result.Objects) > 0 {
		result.ObjectLockMode = result.Objects[0].ObjectLockMode
		result.LegalHold = true
		var minRetain time.Time
		for _, o := range result.Objects {
			if result.ObjectLockMode != o.ObjectLockMode {
				result.ObjectLockMode = "MIXED"
			}
			if !o.LegalHold {
				result.LegalHold = false
			}
			if o.RetainUntil != "" {
				if t, err := time.Parse(time.RFC3339, o.RetainUntil); err == nil && (minRetain.IsZero() || t.Before(minRetain)) {
					minRetain = t
				}
			}
		}
		if !minRetain.IsZero() {
			result.RetainUntil = minRetain.UTC().Format(time.RFC3339)
		}
	}
	result.Committed = true
	return result, nil
}

func uriEncodeForTest(v string) string { return uriPath(v) }
