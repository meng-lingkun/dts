package validationreport

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const PublicKeySchemaVersion = SchemaVersion + "/public-key"

type PublicKeyDocument struct {
	SchemaVersion     string `json:"schema_version"`
	Algorithm         string `json:"algorithm"`
	KeyID             string `json:"key_id"`
	PublicKeyEd25519  string `json:"public_key_ed25519"`
	FingerprintSHA256 string `json:"fingerprint_sha256"`
}

func fingerprintPublicKey(pub ed25519.PublicKey) string {
	h := sha256.Sum256(pub)
	return hex.EncodeToString(h[:])
}

func defaultEd25519KeyID(pub ed25519.PublicKey) string {
	fp := fingerprintPublicKey(pub)
	if len(fp) > 16 {
		return "ed25519-" + fp[:16]
	}
	return "ed25519-" + fp
}

func parseEd25519PrivateKey(raw []byte) (ed25519.PrivateKey, error) {
	raw = []byte(strings.TrimSpace(string(raw)))
	if len(raw) == 0 {
		return nil, nil
	}
	if block, _ := pem.Decode(raw); block != nil {
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse Ed25519 PKCS#8 private key: %w", err)
		}
		priv, ok := key.(ed25519.PrivateKey)
		if !ok {
			return nil, errors.New("validation report private key is not Ed25519")
		}
		return append(ed25519.PrivateKey(nil), priv...), nil
	}
	decoded, err := base64.StdEncoding.DecodeString(string(raw))
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(string(raw))
	}
	if err != nil {
		return nil, errors.New("Ed25519 private key must be PKCS#8 PEM or base64 seed/private-key bytes")
	}
	switch len(decoded) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(decoded), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(append([]byte(nil), decoded...)), nil
	default:
		return nil, fmt.Errorf("Ed25519 private key bytes=%d; want %d-byte seed or %d-byte private key", len(decoded), ed25519.SeedSize, ed25519.PrivateKeySize)
	}
}

func ed25519PrivateKeyFromEnv() (ed25519.PrivateKey, string, error) {
	value := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_PRIVATE_KEY"))
	if file := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_PRIVATE_KEY_FILE")); file != "" {
		if value != "" {
			return nil, "", errors.New("set only one of QMIGRATION_VALIDATION_REPORT_ED25519_PRIVATE_KEY or QMIGRATION_VALIDATION_REPORT_ED25519_PRIVATE_KEY_FILE")
		}
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, "", fmt.Errorf("read validation report Ed25519 private key file: %w", err)
		}
		value = strings.TrimSpace(string(b))
	}
	if value == "" {
		return nil, "", nil
	}
	priv, err := parseEd25519PrivateKey([]byte(value))
	if err != nil {
		return nil, "", err
	}
	pub := priv.Public().(ed25519.PublicKey)
	keyID := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_KEY_ID"))
	if keyID == "" {
		keyID = defaultEd25519KeyID(pub)
	}
	return priv, keyID, nil
}

func PublicKeyDocumentForSigner(s Signer) (*PublicKeyDocument, error) {
	pub := s.publicEd25519()
	if len(pub) == 0 {
		return nil, nil
	}
	keyID := s.Ed25519KeyID
	if keyID == "" {
		keyID = defaultEd25519KeyID(pub)
	}
	return &PublicKeyDocument{
		SchemaVersion:     PublicKeySchemaVersion,
		Algorithm:         "Ed25519",
		KeyID:             keyID,
		PublicKeyEd25519:  base64.StdEncoding.EncodeToString(pub),
		FingerprintSHA256: fingerprintPublicKey(pub),
	}, nil
}

func MarshalPublicKeyDocument(doc *PublicKeyDocument) ([]byte, error) {
	if doc == nil {
		return nil, errors.New("Ed25519 signing key is not configured")
	}
	return marshalIndentedNewline(doc)
}

func parseEd25519PublicKeyBytes(raw []byte) (ed25519.PublicKey, error) {
	raw = []byte(strings.TrimSpace(string(raw)))
	if len(raw) == 0 {
		return nil, errors.New("empty Ed25519 public key")
	}
	var doc PublicKeyDocument
	if raw[0] == '{' && jsonUnmarshal(raw, &doc) == nil && strings.TrimSpace(doc.PublicKeyEd25519) != "" {
		raw = []byte(strings.TrimSpace(doc.PublicKeyEd25519))
	}
	if block, _ := pem.Decode(raw); block != nil {
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse Ed25519 public key PEM: %w", err)
		}
		pub, ok := key.(ed25519.PublicKey)
		if !ok {
			return nil, errors.New("public key is not Ed25519")
		}
		return append(ed25519.PublicKey(nil), pub...), nil
	}
	decoded, err := base64.StdEncoding.DecodeString(string(raw))
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(string(raw))
	}
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("Ed25519 public key must be public-key JSON, PKIX PEM, or base64 %d-byte key", ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(decoded), nil
}

type externalSignerClient struct {
	endpoints []string
	client    *http.Client
	doc       PublicKeyDocument
}

type externalSignRequest struct {
	DataBase64 string `json:"data_b64"`
}
type externalSignResponse struct {
	KeyID            string `json:"key_id"`
	PublicKeyEd25519 string `json:"public_key_ed25519"`
	SignatureBase64  string `json:"signature_b64"`
}

func newExternalSignerFromEnv() (*externalSignerClient, error) {
	raw := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_SIGNER_ENDPOINTS"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_SIGNER_ENDPOINT"))
	}
	if raw == "" {
		return nil, nil
	}
	var eps []string
	for _, e := range strings.Split(raw, ",") {
		e = strings.TrimRight(strings.TrimSpace(e), "/")
		if e != "" {
			eps = append(eps, e)
		}
	}
	if len(eps) == 0 {
		return nil, errors.New("external Ed25519 signer has no endpoint")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_SIGNER_SERVER_NAME"))}
	if ca := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_SIGNER_CA")); ca != "" {
		pool, _ := x509.SystemCertPool()
		if pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return nil, errors.New("external signer CA contains no certificate")
		}
		tc.RootCAs = pool
	}
	tr.TLSClientConfig = tc
	c := &externalSignerClient{endpoints: eps, client: &http.Client{Transport: tr, Timeout: 30 * time.Second}}
	var first *PublicKeyDocument
	for _, ep := range eps {
		req, _ := http.NewRequest(http.MethodGet, ep+"/v1/public-key", nil)
		if tok := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_SIGNER_TOKEN")); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		resp, err := c.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("external signer %s public key: %w", ep, err)
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return nil, fmt.Errorf("external signer %s public key status %s", ep, resp.Status)
		}
		var d PublicKeyDocument
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, err
		}
		pub, err := parseEd25519PublicKeyBytes([]byte(d.PublicKeyEd25519))
		if err != nil {
			return nil, err
		}
		fp := fingerprintPublicKey(pub)
		if d.Algorithm != "Ed25519" || d.KeyID == "" || !strings.EqualFold(d.FingerprintSHA256, fp) {
			return nil, fmt.Errorf("external signer %s returned invalid public-key identity", ep)
		}
		if first == nil {
			copy := d
			first = &copy
		} else if d.KeyID != first.KeyID || d.PublicKeyEd25519 != first.PublicKeyEd25519 || !strings.EqualFold(d.FingerprintSHA256, first.FingerprintSHA256) {
			return nil, errors.New("external signer HA endpoints expose different key identities")
		}
	}
	c.doc = *first
	return c, nil
}
func (c *externalSignerClient) sign(data []byte) ([]byte, error) {
	payload, _ := json.Marshal(externalSignRequest{DataBase64: base64.StdEncoding.EncodeToString(data)})
	var errs []string
	for _, ep := range c.endpoints {
		req, _ := http.NewRequest(http.MethodPost, ep+"/v1/sign", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		if tok := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_SIGNER_TOKEN")); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		resp, err := c.client.Do(req)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			errs = append(errs, resp.Status)
			continue
		}
		var out externalSignResponse
		if err := json.Unmarshal(b, &out); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		if out.KeyID != c.doc.KeyID || out.PublicKeyEd25519 != c.doc.PublicKeyEd25519 {
			errs = append(errs, "signer identity changed")
			continue
		}
		sig, err := base64.StdEncoding.DecodeString(out.SignatureBase64)
		if err != nil || len(sig) != ed25519.SignatureSize {
			errs = append(errs, "invalid signature encoding")
			continue
		}
		pub, _ := parseEd25519PublicKeyBytes([]byte(c.doc.PublicKeyEd25519))
		if !ed25519.Verify(pub, data, sig) {
			errs = append(errs, "signature verification failed")
			continue
		}
		return sig, nil
	}
	return nil, fmt.Errorf("all external signer endpoints failed: %s", strings.Join(errs, "; "))
}
