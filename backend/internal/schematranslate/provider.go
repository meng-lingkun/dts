package schematranslate

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
	"os"
	"strings"
	"time"
)

type Request struct {
	SourceFamily string `json:"source_family"`
	TargetFamily string `json:"target_family"`
	ObjectType   string `json:"object_type"`
	SourceSchema string `json:"source_schema"`
	TargetSchema string `json:"target_schema"`
	Name         string `json:"name"`
	SourceDDL    string `json:"source_ddl"`
}
type Response struct {
	TargetDDL        string `json:"target_ddl"`
	TargetName       string `json:"target_name,omitempty"`
	SemanticReview   bool   `json:"semantic_review"`
	SideEffectReview bool   `json:"side_effect_review"`
	SafeAutoApply    bool   `json:"safe_auto_apply"`
	TargetDDLSHA256  string `json:"target_ddl_sha256"`
	Notes            string `json:"notes,omitempty"`
}
type Provider struct {
	url, token string
	client     *http.Client
}

func FromEnv() (*Provider, error) {
	u := strings.TrimRight(strings.TrimSpace(os.Getenv("QMIGRATION_SCHEMA_TRANSLATION_URL")), "/")
	if u == "" {
		return nil, nil
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if strings.HasPrefix(strings.ToLower(u), "https://") {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: strings.TrimSpace(os.Getenv("QMIGRATION_SCHEMA_TRANSLATION_SERVER_NAME"))}
		if ca := strings.TrimSpace(os.Getenv("QMIGRATION_SCHEMA_TRANSLATION_CA")); ca != "" {
			pool, _ := x509.SystemCertPool()
			if pool == nil {
				pool = x509.NewCertPool()
			}
			if !pool.AppendCertsFromPEM([]byte(ca)) {
				return nil, errors.New("schema translation CA contains no certificate")
			}
			tc.RootCAs = pool
		}
		tr.TLSClientConfig = tc
	}
	return &Provider{url: u, token: strings.TrimSpace(os.Getenv("QMIGRATION_SCHEMA_TRANSLATION_TOKEN")), client: &http.Client{Transport: tr, Timeout: 60 * time.Second}}, nil
}
func digest(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func (p *Provider) Translate(ctx context.Context, in Request) (Response, error) {
	var out Response
	if p == nil {
		return out, errors.New("schema translation provider is not configured")
	}
	if strings.TrimSpace(in.SourceDDL) == "" {
		return out, errors.New("schema translation requires source DDL")
	}
	b, _ := json.Marshal(in)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url+"/v1/translate", bytes.NewReader(b))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode/100 != 2 {
		return out, fmt.Errorf("schema translation provider %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, err
	}
	out.TargetDDL = strings.TrimSpace(out.TargetDDL)
	if out.TargetDDL == "" {
		return out, errors.New("schema translation provider returned empty target DDL")
	}
	if !out.SemanticReview || !out.SideEffectReview || !out.SafeAutoApply {
		return out, errors.New("schema translation provider did not attest semantic/side-effect/safe-auto-apply review")
	}
	if !strings.EqualFold(strings.TrimSpace(out.TargetDDLSHA256), digest(out.TargetDDL)) {
		return out, errors.New("schema translation provider target DDL SHA-256 proof mismatch")
	}
	return out, nil
}
