package validationreport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type TimestampProof struct {
	SchemaVersion  string `json:"schema_version"`
	ManifestSHA256 string `json:"manifest_sha256"`
	TokenSHA256    string `json:"token_sha256"`
	PolicyOID      string `json:"policy_oid,omitempty"`
	SerialNumber   string `json:"serial_number,omitempty"`
	GenTime        string `json:"gen_time,omitempty"`
	TSA            string `json:"tsa,omitempty"`
}

func ApplyTimestamp(ctx context.Context, b *Bundle) error {
	if b == nil {
		return errors.New("nil validation report bundle")
	}
	tsa := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_TSA_URL"))
	if tsa == "" {
		return nil
	}
	openssl := strings.TrimSpace(os.Getenv("QMIGRATION_OPENSSL_BIN"))
	if openssl == "" {
		openssl = "openssl"
	}
	caFile := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_TSA_CA_FILE"))
	if caFile == "" {
		return errors.New("RFC3161 TSA requires QMIGRATION_VALIDATION_REPORT_TSA_CA_FILE")
	}
	retries, _ := strconv.Atoi(strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_TSA_RETRIES")))
	if retries < 1 {
		retries = 3
	}
	if retries > 10 {
		retries = 10
	}
	dir, err := os.MkdirTemp("", "qmigration-tsa-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	dataFile := filepath.Join(dir, "manifest.json")
	qFile := filepath.Join(dir, "query.tsq")
	rFile := filepath.Join(dir, "response.tsr")
	if err := os.WriteFile(dataFile, b.ManifestJSON, 0600); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, openssl, "ts", "-query", "-data", dataFile, "-sha256", "-cert", "-out", qFile)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("openssl ts query: %w: %s", err, strings.TrimSpace(string(out)))
	}
	q, err := os.ReadFile(qFile)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	var token []byte
	for i := 0; i < retries; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, tsa, bytes.NewReader(q))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/timestamp-query")
		req.Header.Set("Accept", "application/timestamp-reply")
		resp, err := client.Do(req)
		if err == nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
			resp.Body.Close()
			if resp.StatusCode/100 == 2 && len(body) > 0 {
				token = body
				break
			}
			err = fmt.Errorf("TSA HTTP %s", resp.Status)
		}
		if i+1 < retries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(i+1) * time.Second):
			}
		}
		if i+1 == retries {
			return fmt.Errorf("RFC3161 TSA request failed: %w", err)
		}
	}
	if err := os.WriteFile(rFile, token, 0600); err != nil {
		return err
	}
	verify := exec.CommandContext(ctx, openssl, "ts", "-verify", "-data", dataFile, "-in", rFile, "-CAfile", caFile)
	if out, err := verify.CombinedOutput(); err != nil {
		return fmt.Errorf("RFC3161 TSA verification failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	textCmd := exec.CommandContext(ctx, openssl, "ts", "-reply", "-in", rFile, "-text")
	text, err := textCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("RFC3161 TSA inspect: %w: %s", err, strings.TrimSpace(string(text)))
	}
	proof := TimestampProof{SchemaVersion: SchemaVersion + "/timestamp", ManifestSHA256: SHA256Hex(b.ManifestJSON), TokenSHA256: SHA256Hex(token), TSA: tsa}
	parseTimestampText(string(text), &proof)
	pj, _ := json.MarshalIndent(proof, "", "  ")
	pj = append(pj, '\n')
	b.TimestampToken = token
	b.TimestampJSON = pj
	b.TimestampProof = proof
	return nil
}

var oidRE = regexp.MustCompile(`(?m)^Policy OID:\s*(.+)$`)
var serialRE = regexp.MustCompile(`(?m)^Serial number:\s*(.+)$`)
var timeRE = regexp.MustCompile(`(?m)^Time stamp:\s*(.+)$`)

func parseTimestampText(s string, p *TimestampProof) {
	if m := oidRE.FindStringSubmatch(s); len(m) > 1 {
		p.PolicyOID = strings.TrimSpace(m[1])
	}
	if m := serialRE.FindStringSubmatch(s); len(m) > 1 {
		p.SerialNumber = strings.TrimSpace(m[1])
	}
	if m := timeRE.FindStringSubmatch(s); len(m) > 1 {
		p.GenTime = strings.TrimSpace(m[1])
	}
}

func VerifyTimestampDirectory(ctx context.Context, dir, caFile string) (TimestampProof, error) {
	var proof TimestampProof
	meta, err := os.ReadFile(filepath.Join(dir, "timestamp.json"))
	if err != nil {
		return proof, err
	}
	if err := json.Unmarshal(meta, &proof); err != nil {
		return proof, err
	}
	manifest, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return proof, err
	}
	token, err := os.ReadFile(filepath.Join(dir, "manifest.tsr"))
	if err != nil {
		return proof, err
	}
	if !strings.EqualFold(proof.ManifestSHA256, SHA256Hex(manifest)) || !strings.EqualFold(proof.TokenSHA256, SHA256Hex(token)) {
		return proof, errors.New("RFC3161 timestamp proof digest mismatch")
	}
	if strings.TrimSpace(caFile) == "" {
		return proof, errors.New("trusted TSA CA file is required")
	}
	openssl := strings.TrimSpace(os.Getenv("QMIGRATION_OPENSSL_BIN"))
	if openssl == "" {
		openssl = "openssl"
	}
	cmd := exec.CommandContext(ctx, openssl, "ts", "-verify", "-data", filepath.Join(dir, "manifest.json"), "-in", filepath.Join(dir, "manifest.tsr"), "-CAfile", caFile)
	if out, err := cmd.CombinedOutput(); err != nil {
		return proof, fmt.Errorf("RFC3161 verify: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return proof, nil
}
