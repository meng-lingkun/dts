package ddlsidecar

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"qmigration/backend/internal/domain"
)

// Item is one position in the source transaction order. Kind is DML or DDL.
// DML entries are placeholders: QMigration substitutes the next event from the
// native DML decoder. DDL entries carry the exact vendor-observed statement.
type Item struct {
	Kind   string `json:"kind"`
	SQL    string `json:"sql,omitempty"`
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
}

type Request struct {
	Product       string   `json:"product"`
	PositionType  string   `json:"position_type"`
	PositionValue string   `json:"position_value"`
	XID           string   `json:"xid,omitempty"`
	DMLCount      int      `json:"dml_count"`
	Tables        []string `json:"tables,omitempty"`
}

type Response struct {
	PositionType  string `json:"position_type"`
	PositionValue string `json:"position_value"`
	XID           string `json:"xid,omitempty"`
	Atomicity     string `json:"atomicity"`
	Sequence      []Item `json:"sequence"`
}

type Client struct {
	endpoint string
	token    string
	http     *http.Client
}

func New(endpoint, token, serverName, caPEM string) (*Client, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, errors.New("DDL sidecar endpoint is required")
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid DDL sidecar endpoint %q", endpoint)
	}
	tr := &http.Transport{}
	if u.Scheme == "https" {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: strings.TrimSpace(serverName)}
		if strings.TrimSpace(caPEM) != "" {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(caPEM)) {
				return nil, errors.New("DDL sidecar CA PEM contains no certificates")
			}
			cfg.RootCAs = pool
		}
		tr.TLSClientConfig = cfg
	}
	return &Client{endpoint: strings.TrimRight(endpoint, "/"), token: strings.TrimSpace(token), http: &http.Client{Timeout: 30 * time.Second, Transport: tr}}, nil
}

func (c *Client) Sequence(ctx context.Context, req Request) (*Response, error) {
	if c == nil || c.http == nil {
		return nil, errors.New("DDL sidecar client is not configured")
	}
	if strings.TrimSpace(req.PositionValue) == "" || req.DMLCount < 0 {
		return nil, errors.New("DDL sidecar request requires position and non-negative DML count")
	}
	b, _ := json.Marshal(req)
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/v1/transaction-sequence", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		hreq.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("DDL sidecar returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	var out Response
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode DDL sidecar response: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(out.Atomicity), "COMMITTED_TXN_V1") {
		return nil, fmt.Errorf("DDL sidecar did not attest COMMITTED_TXN_V1 atomicity")
	}
	if !strings.EqualFold(strings.TrimSpace(out.PositionType), strings.TrimSpace(req.PositionType)) || strings.TrimSpace(out.PositionValue) != strings.TrimSpace(req.PositionValue) {
		return nil, fmt.Errorf("DDL sidecar position %s/%s does not match native CDC %s/%s", out.PositionType, out.PositionValue, req.PositionType, req.PositionValue)
	}
	if strings.TrimSpace(req.XID) != "" && strings.TrimSpace(out.XID) != "" && strings.TrimSpace(out.XID) != strings.TrimSpace(req.XID) {
		return nil, fmt.Errorf("DDL sidecar xid %s does not match native CDC xid %s", out.XID, req.XID)
	}
	return &out, nil
}

var (
	alterTableRE  = regexp.MustCompile(`(?is)^\s*ALTER\s+TABLE\s+((?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)(?:\.(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*))?)\s+.+$`)
	truncateRE    = regexp.MustCompile(`(?is)^\s*TRUNCATE\s+(?:TABLE\s+)?((?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)(?:\.(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*))?)(?:\s+.*)?$`)
	createIndexRE = regexp.MustCompile(`(?is)^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)\s+ON\s+((?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)(?:\.(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*))?)\s*\(.+\).*$`)
)

func normalizeIdent(s string) string {
	s = strings.TrimSpace(s)
	parts := strings.Split(s, ".")
	for i := range parts {
		parts[i] = strings.ToLower(strings.Trim(parts[i], `"`))
	}
	return strings.Join(parts, ".")
}

func ddlTable(sql string) (string, bool) {
	for _, re := range []*regexp.Regexp{alterTableRE, truncateRE, createIndexRE} {
		m := re.FindStringSubmatch(sql)
		if len(m) > 1 {
			return normalizeIdent(m[1]), true
		}
	}
	return "", false
}

// Reconstruct inserts trusted DDL entries into the exact native DML order.
// Only the conservative selected-table ALTER/TRUNCATE/CREATE INDEX subset is
// accepted. Anything else fails closed rather than being replayed speculatively.
func Reconstruct(native []domain.CDCEvent, proof *Response, selected []string) ([]domain.CDCEvent, error) {
	if proof == nil {
		return nil, errors.New("nil DDL sequence proof")
	}
	selectedSet := map[string]bool{}
	for _, t := range selected {
		selectedSet[normalizeIdent(t)] = true
	}
	out := make([]domain.CDCEvent, 0, len(proof.Sequence)+len(native))
	dml := 0
	for _, item := range proof.Sequence {
		switch strings.ToUpper(strings.TrimSpace(item.Kind)) {
		case "DML":
			if dml >= len(native) {
				return nil, errors.New("DDL sidecar contains more DML placeholders than native decoder events")
			}
			out = append(out, native[dml])
			dml++
		case "DDL":
			table, ok := ddlTable(item.SQL)
			if !ok {
				return nil, fmt.Errorf("DDL sidecar statement is outside safe DDL subset: %q", item.SQL)
			}
			if len(selectedSet) > 0 && !selectedSet[table] {
				// Permit unqualified table only when it uniquely names one selected table.
				matched := ""
				if !strings.Contains(table, ".") {
					for s := range selectedSet {
						parts := strings.Split(s, ".")
						if parts[len(parts)-1] == table {
							if matched != "" {
								return nil, fmt.Errorf("ambiguous unqualified DDL table %s", table)
							}
							matched = s
						}
					}
				}
				if matched == "" {
					return nil, fmt.Errorf("DDL sidecar statement targets unselected table %s", table)
				}
				table = matched
			}
			parts := strings.Split(table, ".")
			schema, name := "", parts[len(parts)-1]
			if len(parts) == 2 {
				schema = parts[0]
			}
			out = append(out, domain.CDCEvent{Operation: domain.CDCDDL, SourceSchema: schema, SourceTable: name, SQL: strings.TrimSpace(item.SQL), PositionType: proof.PositionType, PositionValue: proof.PositionValue})
		default:
			return nil, fmt.Errorf("DDL sidecar sequence has unsupported kind %q", item.Kind)
		}
	}
	if dml != len(native) {
		return nil, fmt.Errorf("DDL sidecar DML placeholders=%d native events=%d", dml, len(native))
	}
	if len(proof.Sequence) == 0 && len(native) > 0 {
		return nil, errors.New("DDL sidecar returned empty sequence for non-empty native transaction")
	}
	return out, nil
}
