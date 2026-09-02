package gbase8acdc

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
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"qmigration/backend/internal/domain"
)

// GBase 8a does not expose a single portable public SQL log API across every
// deployment.  QMigration therefore uses a datasource-local provider contract:
// vendor SDK/C/C++ code supplies complete committed transactions plus explicit
// lineage/schema/order proofs, while QMigration owns transaction apply,
// checkpoint durability and source ACK ordering.

type SchemaColumn struct {
	Name       string `json:"name"`
	ColumnType string `json:"column_type"`
	Nullable   bool   `json:"nullable"`
	PrimaryKey bool   `json:"primary_key"`
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

type SchemaFence struct {
	TableID     int    `json:"table_id"`
	Fingerprint string `json:"fingerprint"`
}

type CheckpointRequest struct {
	Database string           `json:"database"`
	Tables   []TableSelection `json:"tables"`
}
type CheckpointResponse struct {
	Sequence             string        `json:"sequence"`
	CaptureLineage       string        `json:"capture_lineage"`
	SourceTimestampMS    int64         `json:"source_timestamp_ms,omitempty"`
	Resource             string        `json:"resource,omitempty"`
	SchemaFences         []SchemaFence `json:"schema_fences"`
	ProviderVersion      string        `json:"provider_version,omitempty"`
	TransactionAtomicity string        `json:"transaction_atomicity"` // must be COMMITTED_TXN_V1
}

type ReadRequest struct {
	Database               string           `json:"database"`
	AfterSequence          string           `json:"after_sequence"`
	ExpectedCaptureLineage string           `json:"expected_capture_lineage"`
	Tables                 []TableSelection `json:"tables"`
	MaxTransactions        int              `json:"max_transactions,omitempty"`
	MaxBytes               int              `json:"max_bytes,omitempty"`
}

type TransactionEnvelope struct {
	Sequence          string            `json:"sequence"`
	TransactionID     string            `json:"transaction_id"`
	CaptureLineage    string            `json:"capture_lineage"`
	SourceTimestampMS int64             `json:"source_timestamp_ms,omitempty"`
	SchemaFences      []SchemaFence     `json:"schema_fences"`
	Events            []domain.CDCEvent `json:"events"`
	Atomicity         string            `json:"atomicity"` // COMMITTED_TXN_V1
}

type ReadResponse struct {
	Transactions     []TransactionEnvelope `json:"transactions"`
	ResolvedSequence string                `json:"resolved_sequence"`
}
type AckRequest struct {
	Database       string `json:"database"`
	Sequence       string `json:"sequence"`
	CaptureLineage string `json:"capture_lineage"`
}

type Agent interface {
	Health(context.Context) error
	Checkpoint(context.Context, CheckpointRequest) (*CheckpointResponse, error)
	Read(context.Context, ReadRequest) (*ReadResponse, error)
	Ack(context.Context, AckRequest) error
}

func stableTableID(schema, table string) int {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(schema)) + "\x00" + strings.ToLower(strings.TrimSpace(table))))
	n := int(uint32(sum[0])<<24 | uint32(sum[1])<<16 | uint32(sum[2])<<8 | uint32(sum[3]))
	if n < 0 {
		n = -n
	}
	if n == 0 {
		n = 1
	}
	return n
}

func schemaFingerprint(schema, table string, cols []SchemaColumn, pks []string) string {
	var b strings.Builder
	b.WriteString("gbase8a-schema-fence-v1\n")
	b.WriteString(strings.ToLower(strings.TrimSpace(schema)))
	b.WriteByte('\n')
	b.WriteString(strings.ToLower(strings.TrimSpace(table)))
	b.WriteByte('\n')
	for _, c := range cols {
		fmt.Fprintf(&b, "%s\t%s\t%t\t%t\n", strings.ToLower(strings.TrimSpace(c.Name)), strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(c.ColumnType))), " "), c.Nullable, c.PrimaryKey)
	}
	b.WriteString("pk")
	for _, k := range pks {
		b.WriteByte('\t')
		b.WriteString(strings.ToLower(strings.TrimSpace(k)))
	}
	b.WriteByte('\n')
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func BuildTableSelection(schema, table string, columns []domain.ColumnInfo, primaryKeys []string) (TableSelection, error) {
	var s TableSelection
	schema, table = strings.TrimSpace(schema), strings.TrimSpace(table)
	if schema == "" || table == "" || len(columns) == 0 || len(primaryKeys) == 0 {
		return s, errors.New("GBase 8a CDC requires schema/table/columns/primary key")
	}
	s.ID, s.Schema, s.Table = stableTableID(schema, table), schema, table
	s.PrimaryKeys = append([]string(nil), primaryKeys...)
	pk := map[string]bool{}
	for _, k := range primaryKeys {
		pk[strings.ToLower(strings.TrimSpace(k))] = true
	}
	for _, c := range columns {
		if strings.TrimSpace(c.Name) == "" || strings.TrimSpace(c.ColumnType) == "" {
			return TableSelection{}, fmt.Errorf("GBase 8a CDC schema proof requires complete column metadata for %s.%s", schema, table)
		}
		s.Columns = append(s.Columns, c.Name)
		s.SchemaColumns = append(s.SchemaColumns, SchemaColumn{Name: c.Name, ColumnType: c.ColumnType, Nullable: c.Nullable, PrimaryKey: pk[strings.ToLower(strings.TrimSpace(c.Name))]})
	}
	s.SchemaFingerprint = schemaFingerprint(schema, table, s.SchemaColumns, s.PrimaryKeys)
	return s, nil
}

func ValidateSchemaFences(selections []TableSelection, fences []SchemaFence) error {
	got := map[int]string{}
	for _, f := range fences {
		fp := strings.ToLower(strings.TrimSpace(f.Fingerprint))
		if f.TableID <= 0 || len(fp) != 64 {
			return fmt.Errorf("invalid GBase 8a schema fence for table id %d", f.TableID)
		}
		if _, err := hex.DecodeString(fp); err != nil {
			return fmt.Errorf("invalid GBase 8a schema fingerprint: %w", err)
		}
		if _, ok := got[f.TableID]; ok {
			return fmt.Errorf("duplicate GBase 8a schema fence for table id %d", f.TableID)
		}
		got[f.TableID] = fp
	}
	if len(got) != len(selections) {
		return fmt.Errorf("GBase 8a provider returned %d schema fences; expected %d", len(got), len(selections))
	}
	for _, s := range selections {
		want := strings.ToLower(strings.TrimSpace(s.SchemaFingerprint))
		if got[s.ID] != want {
			return fmt.Errorf("GBase 8a schema drift detected for %s.%s", s.Schema, s.Table)
		}
	}
	return nil
}

func NormalizeLineage(v string) (string, error) {
	v = strings.ToLower(strings.TrimSpace(v))
	if len(v) != 64 {
		return "", errors.New("GBase 8a capture lineage must be 64 hex characters")
	}
	if _, err := hex.DecodeString(v); err != nil {
		return "", fmt.Errorf("invalid GBase 8a capture lineage: %w", err)
	}
	return v, nil
}
func ParseSequence(v string) (uint64, error) { return strconv.ParseUint(strings.TrimSpace(v), 10, 64) }
func FormatPosition(seq uint64, lineage string) string {
	return fmt.Sprintf("seq=%d;capture=%s", seq, lineage)
}
func ParsePosition(v string) (uint64, string, error) {
	var seq uint64
	var lineage string
	var hs, hc bool
	for _, p := range strings.Split(strings.TrimSpace(v), ";") {
		kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch strings.ToLower(kv[0]) {
		case "seq":
			n, e := ParseSequence(kv[1])
			if e != nil {
				return 0, "", e
			}
			seq = n
			hs = true
		case "capture":
			x, e := NormalizeLineage(kv[1])
			if e != nil {
				return 0, "", e
			}
			lineage = x
			hc = true
		}
	}
	if !hs || !hc {
		return 0, "", fmt.Errorf("invalid GBase 8a CDC position %q", v)
	}
	return seq, lineage, nil
}

func validateTransactions(resp *ReadResponse, after uint64, lineage string, selections []TableSelection) error {
	allowed := map[string]TableSelection{}
	for _, s := range selections {
		allowed[strings.ToLower(s.Schema+"\x00"+s.Table)] = s
	}
	prev := after
	for i, tx := range resp.Transactions {
		seq, err := ParseSequence(tx.Sequence)
		if err != nil {
			return fmt.Errorf("transaction %d sequence: %w", i, err)
		}
		if seq <= prev {
			return fmt.Errorf("GBase 8a CDC sequence is not strictly monotonic: %d after %d", seq, prev)
		}
		prev = seq
		l, err := NormalizeLineage(tx.CaptureLineage)
		if err != nil {
			return err
		}
		if l != lineage {
			return errors.New("GBase 8a CDC capture lineage changed during read")
		}
		if tx.Atomicity != "COMMITTED_TXN_V1" {
			return fmt.Errorf("GBase 8a transaction %s lacks committed-transaction atomicity proof", tx.TransactionID)
		}
		if strings.TrimSpace(tx.TransactionID) == "" || len(tx.Events) == 0 {
			return errors.New("GBase 8a provider emitted empty/unidentified transaction")
		}
		if err := ValidateSchemaFences(selections, tx.SchemaFences); err != nil {
			return err
		}
		for j, e := range tx.Events {
			s, ok := allowed[strings.ToLower(strings.TrimSpace(e.SourceSchema)+"\x00"+strings.TrimSpace(e.SourceTable))]
			if !ok {
				return fmt.Errorf("GBase 8a provider emitted unselected table %s.%s", e.SourceSchema, e.SourceTable)
			}
			if e.Operation != domain.CDCInsert && e.Operation != domain.CDCUpdate && e.Operation != domain.CDCDelete && e.Operation != domain.CDCTruncate && e.Operation != domain.CDCDDL {
				return fmt.Errorf("GBase 8a provider emitted unsupported operation %s", e.Operation)
			}
			if e.Operation == domain.CDCInsert || e.Operation == domain.CDCUpdate {
				if len(e.After) != len(s.Columns) {
					return fmt.Errorf("GBase 8a event %d does not contain a complete after image for %s.%s", j, s.Schema, s.Table)
				}
			}
			if e.Operation == domain.CDCDelete || e.Operation == domain.CDCUpdate {
				if len(e.Before) != len(s.Columns) {
					return fmt.Errorf("GBase 8a event %d does not contain a complete before image for %s.%s", j, s.Schema, s.Table)
				}
			}
			e.PositionType = "GBASE8A_CDC_SEQ"
			e.PositionValue = FormatPosition(seq, lineage)
		}
	}
	if strings.TrimSpace(resp.ResolvedSequence) != "" {
		r, err := ParseSequence(resp.ResolvedSequence)
		if err != nil {
			return err
		}
		if r < prev {
			return fmt.Errorf("GBase 8a resolved sequence %d is behind emitted sequence %d", r, prev)
		}
	}
	return nil
}

type Client struct {
	base  string
	token string
	hc    *http.Client
}

func NewClient(raw, caPEM, serverName, token string) (*Client, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("empty GBase 8a CDC provider URL")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(u.Scheme) {
	case "gbase8acdc":
		u.Scheme = "http"
	case "gbase8acdcs":
		u.Scheme = "https"
	case "http", "https":
	default:
		return nil, fmt.Errorf("unsupported GBase 8a CDC provider scheme %q", u.Scheme)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if u.Scheme == "https" {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: strings.TrimSpace(serverName)}
		if strings.TrimSpace(caPEM) != "" {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(caPEM)) {
				return nil, errors.New("invalid GBase 8a CDC CA PEM")
			}
			cfg.RootCAs = pool
		}
		tr.TLSClientConfig = cfg
	}
	return &Client{base: strings.TrimRight(u.String(), "/"), token: token, hc: &http.Client{Transport: tr, Timeout: 90 * time.Second}}, nil
}
func (c *Client) call(ctx context.Context, method, path string, in, out any) error {
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
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GBase 8a provider %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	if out != nil && len(b) > 0 {
		return json.Unmarshal(b, out)
	}
	return nil
}
func (c *Client) Health(ctx context.Context) error {
	return c.call(ctx, http.MethodGet, "/v1/health", nil, nil)
}
func (c *Client) Checkpoint(ctx context.Context, r CheckpointRequest) (*CheckpointResponse, error) {
	var out CheckpointResponse
	err := c.call(ctx, http.MethodPost, "/v1/checkpoint", r, &out)
	return &out, err
}
func (c *Client) Read(ctx context.Context, r ReadRequest) (*ReadResponse, error) {
	var out ReadResponse
	err := c.call(ctx, http.MethodPost, "/v1/read", r, &out)
	return &out, err
}
func (c *Client) Ack(ctx context.Context, r AckRequest) error {
	return c.call(ctx, http.MethodPost, "/v1/ack", r, nil)
}

// ValidateReadResponseForAgent validates a native/local provider response before
// it is exposed by the HTTP agent. It is intentionally the same strict proof
// validation used by the remote QMigration reader.
func ValidateReadResponseForAgent(resp *ReadResponse, after uint64, lineage string, selections []TableSelection) error {
	return validateTransactions(resp, after, lineage, selections)
}
