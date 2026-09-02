package db2log

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
	"strings"
	"time"
)

type PositionResponse struct {
	InitialLRI    LRI    `json:"initial_lri"`
	NextStartLRI  LRI    `json:"next_start_lri"`
	CurrentEndLRI LRI    `json:"current_end_lri"`
	ByteOrder     string `json:"byte_order"`
	Recoverable   bool   `json:"recoverable"`
	Database      string `json:"database,omitempty"`
}

type RecordEnvelope struct {
	LRI               LRI    `json:"lri"`
	NextLRI           LRI    `json:"next_lri"`
	LogType           uint16 `json:"log_type"`
	Flags             uint16 `json:"flags"`
	TID               string `json:"tid"`
	ByteOrder         string `json:"byte_order"`
	RawBase64         string `json:"raw_base64"`
	SourceTimestampMS int64  `json:"source_timestamp_ms,omitempty"`
}

type ReadResponse struct {
	Records       []RecordEnvelope `json:"records"`
	NextStartLRI  LRI              `json:"next_start_lri"`
	CurrentEndLRI LRI              `json:"current_end_lri"`
	ReadToCurrent bool             `json:"read_to_current"`
}

type BootstrapRequest struct {
	EndLRI LRI             `json:"end_lri"`
	Tables []TableIdentity `json:"tables"`
}
type BootstrapResponse struct {
	Records []RecordEnvelope `json:"records"`
}

type TableIdentity struct {
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	TablespaceID uint16 `json:"tablespace_id"`
	TableID      uint16 `json:"table_id"`
}

type Client struct {
	base  string
	http  *http.Client
	token string
}

// NewClient accepts db2log:// (HTTP) and db2logs:// (HTTPS). HTTPS can use a
// PEM CA and an explicit TLS server name. A bearer token is optional because
// mTLS-only deployments are supported as well.
func NewClient(rawURL, caPEM, serverName, token string) (*Client, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, errors.New("DB2 log agent URL is required")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(u.Scheme) {
	case "db2log":
		u.Scheme = "http"
	case "db2logs":
		u.Scheme = "https"
	case "http", "https":
	default:
		return nil, fmt.Errorf("DB2 log agent URL must use db2log:// or db2logs://, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("DB2 log agent URL has no host")
	}
	if q := strings.TrimSpace(u.Query().Get("server_name")); q != "" && serverName == "" {
		serverName = q
	}
	q := u.Query()
	q.Del("server_name")
	u.RawQuery = q.Encode()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if u.Scheme == "https" {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
		if strings.TrimSpace(caPEM) != "" {
			pool, err := x509.SystemCertPool()
			if err != nil || pool == nil {
				pool = x509.NewCertPool()
			}
			if !pool.AppendCertsFromPEM([]byte(caPEM)) {
				return nil, errors.New("DB2 log agent CA PEM contains no certificate")
			}
			tc.RootCAs = pool
		}
		tr.TLSClientConfig = tc
	}
	return &Client{base: strings.TrimRight(u.String(), "/"), token: strings.TrimSpace(token), http: &http.Client{Transport: tr, Timeout: 90 * time.Second}}, nil
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
		return fmt.Errorf("DB2 log agent %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return err
		}
	}
	return nil
}
func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/v1/health", nil, nil)
}
func (c *Client) Position(ctx context.Context) (*PositionResponse, error) {
	var out PositionResponse
	if err := c.do(ctx, http.MethodGet, "/v1/position", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
func (c *Client) Bootstrap(ctx context.Context, req BootstrapRequest) (*BootstrapResponse, error) {
	var out BootstrapResponse
	if err := c.do(ctx, http.MethodPost, "/v1/bootstrap", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
func (c *Client) Read(ctx context.Context, start LRI, maxRecords, maxBytes int) (*ReadResponse, error) {
	if maxRecords <= 0 {
		maxRecords = 4096
	}
	if maxBytes <= 0 {
		maxBytes = 32 << 20
	}
	path := fmt.Sprintf("/v1/records?start_lri=%s&max_records=%d&max_bytes=%d", url.QueryEscape(start.String()), maxRecords, maxBytes)
	var out ReadResponse
	if err := c.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
