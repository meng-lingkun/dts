package gbase8scdc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"qmigration/backend/internal/domain"
)

// TableSelection is the stable contract between QMigration and a local
// GBase Client-SDK CDC provider. The provider owns only vendor CSDK/syscdcv1
// calls and smart-LOB transport; QMigration owns table selection, transaction
// assembly, checkpointing and target apply.
type SchemaColumn struct {
	Name       string `json:"name"`
	ColumnType string `json:"column_type"`
	Nullable   bool   `json:"nullable"`
	PrimaryKey bool   `json:"primary_key"`
	SmartLOB   string `json:"smart_lob,omitempty"` // blob or clob; derived from the live column type
}

type TableSelection struct {
	ID                int            `json:"id"`
	Schema            string         `json:"schema"`
	Table             string         `json:"table"`
	Columns           []string       `json:"columns"`
	SchemaColumns     []SchemaColumn `json:"schema_columns"`
	PrimaryKeys       []string       `json:"primary_keys"`
	SchemaFingerprint string         `json:"schema_fingerprint"`
}

const SmartLOBImageContract = "cdc-event-owned-lob-v1"

func smartLOBKind(columnType string) string {
	t := canonicalSchemaType(columnType)
	// TEXT/BYTE are simple large objects in the GBase/Informix family. RC28
	// only widens the contract for smart BLOB/CLOB, which can be read through
	// an event-owned locator by a qualified datasource-local CSDK provider.
	if strings.Contains(t, "blob") {
		return "blob"
	}
	if strings.Contains(t, "clob") {
		return "clob"
	}
	return ""
}

func SelectionRequiresSmartLOB(s TableSelection) bool {
	for _, c := range s.SchemaColumns {
		if c.SmartLOB != "" || smartLOBKind(c.ColumnType) != "" {
			return true
		}
	}
	return false
}

func selectionsRequireSmartLOB(in []TableSelection) bool {
	for _, s := range in {
		if SelectionRequiresSmartLOB(s) {
			return true
		}
	}
	return false
}

type SchemaFence struct {
	TableID     int    `json:"table_id"`
	Fingerprint string `json:"fingerprint"`
}

func canonicalSchemaType(v string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(v))), " ")
}

func schemaFingerprint(schema, table string, cols []SchemaColumn, primaryKeys []string) string {
	var b strings.Builder
	b.WriteString("gbase8s-schema-fence-v1\n")
	b.WriteString(strings.ToLower(strings.TrimSpace(schema)))
	b.WriteByte('\n')
	b.WriteString(strings.ToLower(strings.TrimSpace(table)))
	b.WriteByte('\n')
	for _, c := range cols {
		b.WriteString(strings.ToLower(strings.TrimSpace(c.Name)))
		b.WriteByte('\t')
		b.WriteString(canonicalSchemaType(c.ColumnType))
		if c.Nullable {
			b.WriteString("\t1")
		} else {
			b.WriteString("\t0")
		}
		if c.PrimaryKey {
			b.WriteString("\t1\n")
		} else {
			b.WriteString("\t0\n")
		}
	}
	b.WriteString("pk")
	for _, k := range primaryKeys {
		b.WriteByte('\t')
		b.WriteString(strings.ToLower(strings.TrimSpace(k)))
	}
	b.WriteByte('\n')
	return fmt.Sprintf("%x", sha256.Sum256([]byte(b.String())))
}

func BuildTableSelection(schema, table string, columns []domain.ColumnInfo, primaryKeys []string) (TableSelection, error) {
	s := TableSelection{ID: StableTableID(schema, table), Schema: strings.TrimSpace(schema), Table: strings.TrimSpace(table), PrimaryKeys: append([]string(nil), primaryKeys...)}
	if s.ID <= 0 || s.Schema == "" || s.Table == "" || len(columns) == 0 {
		return s, errors.New("invalid GBase 8s CDC table selection")
	}
	s.Columns = make([]string, 0, len(columns))
	s.SchemaColumns = make([]SchemaColumn, 0, len(columns))
	pkSet := make(map[string]bool, len(primaryKeys))
	for _, k := range primaryKeys {
		pkSet[strings.ToLower(strings.TrimSpace(k))] = true
	}
	for _, c := range columns {
		name := strings.TrimSpace(c.Name)
		ct := strings.TrimSpace(c.ColumnType)
		if name == "" || ct == "" {
			return s, fmt.Errorf("GBase 8s CDC schema fingerprint requires column name/type for %s.%s", s.Schema, s.Table)
		}
		s.Columns = append(s.Columns, name)
		s.SchemaColumns = append(s.SchemaColumns, SchemaColumn{Name: name, ColumnType: ct, Nullable: c.Nullable, PrimaryKey: pkSet[strings.ToLower(name)], SmartLOB: smartLOBKind(ct)})
	}
	s.SchemaFingerprint = schemaFingerprint(s.Schema, s.Table, s.SchemaColumns, s.PrimaryKeys)
	return s, nil
}

func ValidateTableSelection(s TableSelection) error {
	if s.ID <= 0 || strings.TrimSpace(s.Schema) == "" || strings.TrimSpace(s.Table) == "" || len(s.Columns) == 0 || len(s.SchemaColumns) != len(s.Columns) {
		return fmt.Errorf("invalid GBase 8s CDC selection: %+v", s)
	}
	for i := range s.Columns {
		if !strings.EqualFold(strings.TrimSpace(s.Columns[i]), strings.TrimSpace(s.SchemaColumns[i].Name)) {
			return fmt.Errorf("GBase 8s CDC selection column/schema mismatch at %s.%s[%d]", s.Schema, s.Table, i)
		}
		wantLOB := smartLOBKind(s.SchemaColumns[i].ColumnType)
		gotLOB := strings.ToLower(strings.TrimSpace(s.SchemaColumns[i].SmartLOB))
		if gotLOB != "" && gotLOB != wantLOB {
			return fmt.Errorf("GBase 8s CDC selection smart-LOB marker mismatch at %s.%s.%s: marker=%q type=%q", s.Schema, s.Table, s.Columns[i], gotLOB, s.SchemaColumns[i].ColumnType)
		}
		s.SchemaColumns[i].SmartLOB = wantLOB
	}
	want := schemaFingerprint(s.Schema, s.Table, s.SchemaColumns, s.PrimaryKeys)
	if !strings.EqualFold(strings.TrimSpace(s.SchemaFingerprint), want) {
		return fmt.Errorf("GBase 8s CDC selection schema fingerprint mismatch for %s.%s", s.Schema, s.Table)
	}
	return nil
}

func SchemaFencesForSelections(selections []TableSelection) []SchemaFence {
	out := make([]SchemaFence, 0, len(selections))
	for _, s := range selections {
		out = append(out, SchemaFence{TableID: s.ID, Fingerprint: s.SchemaFingerprint})
	}
	return out
}

func ValidateSchemaFences(selections []TableSelection, fences []SchemaFence) error {
	if len(fences) != len(selections) {
		return fmt.Errorf("GBase 8s CDC provider returned %d schema fences; expected %d", len(fences), len(selections))
	}
	byID := make(map[int]string, len(fences))
	for _, f := range fences {
		fp := strings.ToLower(strings.TrimSpace(f.Fingerprint))
		if f.TableID <= 0 || len(fp) != 64 {
			return fmt.Errorf("invalid GBase 8s CDC schema fence for table id %d", f.TableID)
		}
		if _, err := hex.DecodeString(fp); err != nil {
			return fmt.Errorf("invalid GBase 8s CDC schema fingerprint %q: %w", f.Fingerprint, err)
		}
		if _, exists := byID[f.TableID]; exists {
			return fmt.Errorf("duplicate GBase 8s CDC schema fence for table id %d", f.TableID)
		}
		byID[f.TableID] = fp
	}
	for _, s := range selections {
		if err := ValidateTableSelection(s); err != nil {
			return err
		}
		got, ok := byID[s.ID]
		if !ok {
			return fmt.Errorf("GBase 8s CDC provider omitted schema fence for %s.%s", s.Schema, s.Table)
		}
		if got != strings.ToLower(strings.TrimSpace(s.SchemaFingerprint)) {
			return fmt.Errorf("GBase 8s CDC schema drift detected for %s.%s: provider=%s planned=%s", s.Schema, s.Table, got, s.SchemaFingerprint)
		}
	}
	return nil
}

const CaptureLineageHexLength = 64

// NormalizeCaptureLineage validates the opaque provider capture generation.
// A lineage is intentionally a fixed SHA-256-sized hex token so it can be
// embedded safely in the durable GBASE8S_CDC_SEQ checkpoint. Providers must
// keep it stable for one logical capture lineage and change it whenever that
// lineage is recreated or replaced.
func NormalizeCaptureLineage(v string) (string, error) {
	v = strings.ToLower(strings.TrimSpace(v))
	if len(v) != CaptureLineageHexLength {
		return "", fmt.Errorf("GBase 8s CDC capture lineage must be %d hex characters", CaptureLineageHexLength)
	}
	if _, err := hex.DecodeString(v); err != nil {
		return "", fmt.Errorf("invalid GBase 8s CDC capture lineage %q: %w", v, err)
	}
	return v, nil
}

type CheckpointRequest struct {
	Database string           `json:"database"`
	Tables   []TableSelection `json:"tables"`
}

type CheckpointResponse struct {
	Sequence              string        `json:"sequence"`
	SourceTimestampMS     int64         `json:"source_timestamp_ms,omitempty"`
	Resource              string        `json:"resource,omitempty"`
	APIVersion            string        `json:"api_version,omitempty"`
	CaptureLineage        string        `json:"capture_lineage"`
	SchemaFences          []SchemaFence `json:"schema_fences"`
	SmartLOBImageContract string        `json:"smart_lob_image_contract,omitempty"`
}

// SmartLOBImageProof proves transport integrity for a provider-supplied smart
// BLOB/CLOB image. The acquisition value is deliberately fixed: QMigration
// never accepts a later SELECT-current-row fallback as a historical CDC image.
type SmartLOBImageProof struct {
	Column      string `json:"column"`
	Kind        string `json:"kind"` // blob or clob
	ByteLength  int64  `json:"byte_length"`
	SHA256      string `json:"sha256"`
	Acquisition string `json:"acquisition"`
}

// RecordEnvelope is provider-normalized CDC data. The provider must preserve
// the exact syscdcv1 record sequence/transaction/table identity and convert
// column values to CDCField without lossy stringification. Binary values use
// Encoding=base64. This keeps vendor CSDK representation out of the control
// plane while leaving transaction semantics in QMigration.
type RecordEnvelope struct {
	Kind              string               `json:"kind"`
	Sequence          string               `json:"sequence,omitempty"`
	TransactionID     uint64               `json:"transaction_id,omitempty"`
	TableID           int                  `json:"table_id,omitempty"`
	Fields            []domain.CDCField    `json:"fields,omitempty"`
	SourceTimestampMS int64                `json:"source_timestamp_ms,omitempty"`
	ErrorCode         int                  `json:"error_code,omitempty"`
	ErrorText         string               `json:"error_text,omitempty"`
	SchemaFingerprint string               `json:"schema_fingerprint,omitempty"`
	SmartLOBProofs    []SmartLOBImageProof `json:"smart_lob_proofs,omitempty"`
}

type ReadRequest struct {
	Database               string           `json:"database"`
	StartSequence          string           `json:"start_sequence"`
	ExpectedCaptureLineage string           `json:"expected_capture_lineage"`
	Tables                 []TableSelection `json:"tables"`
	MaxRecords             int              `json:"max_records,omitempty"`
	MaxBytes               int              `json:"max_bytes,omitempty"`
}

type ReadResponse struct {
	Records []RecordEnvelope `json:"records"`
	// NextSequence is an opaque syscdcv1 restart position suitable for the
	// next provider read in the same live Worker. It is distinct from QMigration's
	// durable restart/commit checkpoint, which may need to rewind to an open
	// transaction BEGIN after a process crash.
	NextSequence          string        `json:"next_sequence,omitempty"`
	ReadToCurrent         bool          `json:"read_to_current,omitempty"`
	CaptureLineage        string        `json:"capture_lineage"`
	SchemaFences          []SchemaFence `json:"schema_fences"`
	SmartLOBImageContract string        `json:"smart_lob_image_contract,omitempty"`
}

func ValidateCheckpointResponse(req CheckpointRequest, out *CheckpointResponse) error {
	if out == nil {
		return errors.New("GBase 8s CDC provider returned nil checkpoint")
	}
	if strings.TrimSpace(out.Sequence) == "" {
		return errors.New("GBase 8s CDC provider returned empty checkpoint sequence")
	}
	if _, err := parseSequence(out.Sequence); err != nil {
		return fmt.Errorf("GBase 8s CDC provider returned invalid checkpoint sequence %q: %w", out.Sequence, err)
	}
	lineage, err := NormalizeCaptureLineage(out.CaptureLineage)
	if err != nil {
		return err
	}
	out.CaptureLineage = lineage
	if err := ValidateSchemaFences(req.Tables, out.SchemaFences); err != nil {
		return fmt.Errorf("GBase 8s CDC checkpoint schema fence: %w", err)
	}
	if selectionsRequireSmartLOB(req.Tables) && strings.TrimSpace(out.SmartLOBImageContract) != SmartLOBImageContract {
		return fmt.Errorf("GBase 8s CDC smart-LOB selection requires provider image contract %q; got %q", SmartLOBImageContract, out.SmartLOBImageContract)
	}
	return nil
}

func ValidateReadResponse(req ReadRequest, out *ReadResponse) error {
	if out == nil {
		return errors.New("GBase 8s CDC provider returned nil read response")
	}
	want, err := NormalizeCaptureLineage(req.ExpectedCaptureLineage)
	if err != nil {
		return fmt.Errorf("GBase 8s CDC read requires expected capture lineage: %w", err)
	}
	got, err := NormalizeCaptureLineage(out.CaptureLineage)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("GBase 8s CDC capture lineage changed: provider=%s expected=%s; refusing to continue from an old checkpoint", got, want)
	}
	out.CaptureLineage = got
	if err := ValidateSchemaFences(req.Tables, out.SchemaFences); err != nil {
		return fmt.Errorf("GBase 8s CDC read schema fence: %w", err)
	}
	if selectionsRequireSmartLOB(req.Tables) && strings.TrimSpace(out.SmartLOBImageContract) != SmartLOBImageContract {
		return fmt.Errorf("GBase 8s CDC smart-LOB read requires provider image contract %q; got %q", SmartLOBImageContract, out.SmartLOBImageContract)
	}
	return nil
}

// StableTableID is passed to cdc_startcapture as user_data so provider table
// identity survives process restarts independently of mapping/list order.
func StableTableID(schema, table string) int {
	h := uint32(2166136261)
	for _, b := range []byte(strings.ToLower(strings.TrimSpace(schema) + "\x00" + strings.TrimSpace(table))) {
		h ^= uint32(b)
		h *= 16777619
	}
	id := int(h & 0x7fffffff)
	if id == 0 {
		id = 1
	}
	return id
}

type Agent interface {
	Health(context.Context) error
	Checkpoint(context.Context, CheckpointRequest) (*CheckpointResponse, error)
	Read(context.Context, ReadRequest) (*ReadResponse, error)
}

type ProviderInfo struct {
	Kind         string `json:"kind,omitempty"`
	ABIVersion   string `json:"abi_version,omitempty"`
	SHA256Pinned bool   `json:"sha256_pinned,omitempty"`
}

type ProviderDescriber interface {
	ProviderInfo() ProviderInfo
}

const AgentAPIVersion = "v4"

type HealthResponse struct {
	Status     string       `json:"status"`
	APIVersion string       `json:"api_version"`
	Provider   ProviderInfo `json:"provider,omitempty"`
}

type describedAgent struct {
	Agent
	info ProviderInfo
}

func WithProviderInfo(a Agent, info ProviderInfo) Agent {
	if a == nil {
		return nil
	}
	return &describedAgent{Agent: a, info: info}
}
func (d *describedAgent) ProviderInfo() ProviderInfo { return d.info }
func (d *describedAgent) Close() error {
	if c, ok := d.Agent.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// SerializeAgent protects datasource-local CSDK sessions from concurrent HTTP
// health/checkpoint/read calls. The native CDC API is session-oriented and the
// same smart-LOB session must not be read concurrently.
type serializedAgent struct {
	inner Agent
	mu    sync.Mutex
}

func SerializeAgent(a Agent) Agent {
	if a == nil {
		return nil
	}
	if _, ok := a.(*serializedAgent); ok {
		return a
	}
	return &serializedAgent{inner: a}
}

func (s *serializedAgent) Health(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.Health(ctx)
}
func (s *serializedAgent) Checkpoint(ctx context.Context, req CheckpointRequest) (*CheckpointResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.Checkpoint(ctx, req)
}
func (s *serializedAgent) Read(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.Read(ctx, req)
}
func (s *serializedAgent) ProviderInfo() ProviderInfo {
	if d, ok := s.inner.(ProviderDescriber); ok {
		return d.ProviderInfo()
	}
	return ProviderInfo{}
}
func (s *serializedAgent) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.inner.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

type Client struct {
	base  string
	http  *http.Client
	token string
}

func providerHostIsLoopback(host string) bool {
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// NewClient accepts gbase8scdc:// for HTTP and gbase8scdcs:// for HTTPS.
// Production deployments should prefer HTTPS when the provider is not strictly
// loopback-local. The agent is datasource-specific and does not receive source
// database credentials from QMigration.
func NewClient(rawURL, caPEM, serverName, token string) (*Client, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, errors.New("GBase 8s CDC provider URL is required")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(u.Scheme) {
	case "gbase8scdc":
		u.Scheme = "http"
	case "gbase8scdcs":
		u.Scheme = "https"
	case "http", "https":
	default:
		return nil, fmt.Errorf("GBase 8s CDC provider URL must use gbase8scdc:// or gbase8scdcs://, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("GBase 8s CDC provider URL has no host")
	}
	if u.User != nil {
		return nil, errors.New("GBase 8s CDC provider URL must not contain userinfo/credentials")
	}
	if u.Scheme == "http" && !providerHostIsLoopback(u.Hostname()) {
		return nil, errors.New("non-loopback GBase 8s CDC provider URL requires gbase8scdcs:// or https://")
	}
	q := u.Query()
	if v := strings.TrimSpace(q.Get("server_name")); v != "" && strings.TrimSpace(serverName) == "" {
		serverName = v
	}
	q.Del("server_name")
	u.RawQuery = q.Encode()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if u.Scheme == "https" {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: strings.TrimSpace(serverName)}
		if strings.TrimSpace(caPEM) != "" {
			pool, e := x509.SystemCertPool()
			if e != nil || pool == nil {
				pool = x509.NewCertPool()
			}
			if !pool.AppendCertsFromPEM([]byte(caPEM)) {
				return nil, errors.New("GBase 8s CDC provider CA PEM contains no certificate")
			}
			tc.RootCAs = pool
		}
		tr.TLSClientConfig = tc
	}
	return &Client{
		base:  strings.TrimRight(u.String(), "/"),
		token: strings.TrimSpace(token),
		http:  &http.Client{Transport: tr, Timeout: 90 * time.Second},
	}, nil
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GBase 8s CDC provider %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) Health(ctx context.Context) error {
	_, err := c.HealthInfo(ctx)
	return err
}
func (c *Client) HealthInfo(ctx context.Context) (*HealthResponse, error) {
	var out HealthResponse
	if err := c.do(ctx, http.MethodGet, "/v1/health", nil, &out); err != nil {
		return nil, err
	}
	if strings.TrimSpace(out.Status) != "ok" {
		return nil, fmt.Errorf("GBase 8s CDC provider health status %q", out.Status)
	}
	if strings.TrimSpace(out.APIVersion) != AgentAPIVersion {
		return nil, fmt.Errorf("GBase 8s CDC agent API version %q is incompatible; RC28 requires %s schema-fence + capture-lineage + smart-LOB image protocol", out.APIVersion, AgentAPIVersion)
	}
	return &out, nil
}

func (c *Client) Checkpoint(ctx context.Context, req CheckpointRequest) (*CheckpointResponse, error) {
	var out CheckpointResponse
	if err := c.do(ctx, http.MethodPost, "/v1/checkpoint", req, &out); err != nil {
		return nil, err
	}
	if err := ValidateCheckpointResponse(req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) Read(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
	if req.MaxRecords <= 0 {
		req.MaxRecords = 4096
	}
	if req.MaxBytes <= 0 {
		req.MaxBytes = 32 << 20
	}
	var out ReadResponse
	if err := c.do(ctx, http.MethodPost, "/v1/records", req, &out); err != nil {
		return nil, err
	}
	if err := ValidateReadResponse(req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) StatusInfo(ctx context.Context) (*AgentStatus, error) {
	var out AgentStatus
	if err := c.do(ctx, http.MethodGet, "/v1/status", nil, &out); err != nil {
		return nil, err
	}
	if strings.TrimSpace(out.APIVersion) != AgentAPIVersion {
		return nil, fmt.Errorf("GBase 8s CDC provider status api_version %q is incompatible; expected %s", out.APIVersion, AgentAPIVersion)
	}
	return &out, nil
}
