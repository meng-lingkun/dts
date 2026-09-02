package validationreport

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"os"
	"sort"
	"strings"
	"time"

	"qmigration/backend/internal/domain"
)

const SchemaVersion = "qmigration.validation-report/v1"

type TaskSnapshot struct {
	ID            string                 `json:"id"`
	Name          string                 `json:"name"`
	Mode          domain.MigrationMode   `json:"mode"`
	Status        domain.MigrationStatus `json:"status"`
	SourceID      string                 `json:"source_datasource_id"`
	TargetID      string                 `json:"target_datasource_id"`
	RowsMigrated  int64                  `json:"rows_migrated"`
	BytesMigrated int64                  `json:"bytes_migrated"`
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
}

type Report struct {
	SchemaVersion  string                   `json:"schema_version"`
	Product        string                   `json:"product"`
	ProductVersion string                   `json:"product_version"`
	GeneratedAt    time.Time                `json:"generated_at"`
	Task           TaskSnapshot             `json:"task"`
	Validation     domain.ValidationArchive `json:"validation"`
}

type Artifact struct {
	Name             string `json:"name"`
	ContentType      string `json:"content_type"`
	SizeBytes        int64  `json:"size_bytes"`
	SHA256           string `json:"sha256"`
	HMACSHA256       string `json:"hmac_sha256,omitempty"`
	Ed25519Signature string `json:"ed25519_signature,omitempty"`
	Data             []byte `json:"-"`
}

type Manifest struct {
	SchemaVersion              string     `json:"schema_version"`
	Product                    string     `json:"product"`
	ProductVersion             string     `json:"product_version"`
	TaskID                     string     `json:"task_id"`
	ArchiveEvidenceDigest      string     `json:"archive_evidence_digest"`
	GeneratedAt                time.Time  `json:"generated_at"`
	SignatureAlgorithm         string     `json:"signature_algorithm,omitempty"`
	SignatureKeyID             string     `json:"signature_key_id,omitempty"`
	PublicSignatureAlgorithm   string     `json:"public_signature_algorithm,omitempty"`
	PublicSignatureKeyID       string     `json:"public_signature_key_id,omitempty"`
	PublicKeyEd25519           string     `json:"public_key_ed25519,omitempty"`
	PublicKeyFingerprintSHA256 string     `json:"public_key_fingerprint_sha256,omitempty"`
	Artifacts                  []Artifact `json:"artifacts"`
	ManifestHMACSHA256         string     `json:"manifest_hmac_sha256,omitempty"`
	ManifestEd25519Signature   string     `json:"manifest_ed25519_signature,omitempty"`
}

type Bundle struct {
	Report            Report
	Artifacts         []Artifact
	Manifest          Manifest
	ManifestJSON      []byte
	SHA256SUMS        []byte
	HMACSHA256SUMS    []byte
	ED25519SIGNATURES []byte
	TimestampToken    []byte
	TimestampJSON     []byte
	TimestampProof    TimestampProof
}

type Signer struct {
	Key               []byte
	KeyID             string
	Ed25519PrivateKey ed25519.PrivateKey
	Ed25519KeyID      string
	Ed25519PublicKey  ed25519.PublicKey
	ExternalEd25519   *externalSignerClient
}

func SignerFromEnv() (Signer, error) {
	key := os.Getenv("QMIGRATION_VALIDATION_REPORT_HMAC_KEY")
	if file := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_HMAC_KEY_FILE")); file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return Signer{}, fmt.Errorf("read validation report HMAC key file: %w", err)
		}
		if key != "" {
			return Signer{}, fmt.Errorf("set only one of QMIGRATION_VALIDATION_REPORT_HMAC_KEY or QMIGRATION_VALIDATION_REPORT_HMAC_KEY_FILE")
		}
		key = strings.TrimSpace(string(b))
	}
	var signer Signer
	if key != "" {
		if len(key) < 16 {
			return Signer{}, fmt.Errorf("validation report HMAC key must be at least 16 bytes")
		}
		signer.Key = []byte(key)
		signer.KeyID = strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_HMAC_KEY_ID"))
	}
	ext, err := newExternalSignerFromEnv()
	if err != nil {
		return Signer{}, err
	}
	priv, keyID, err := ed25519PrivateKeyFromEnv()
	if err != nil {
		return Signer{}, err
	}
	if ext != nil && len(priv) > 0 {
		return Signer{}, errors.New("configure either local Ed25519 private key or external signer, not both")
	}
	if ext != nil {
		signer.ExternalEd25519 = ext
		signer.Ed25519KeyID = ext.doc.KeyID
		pub, _ := parseEd25519PublicKeyBytes([]byte(ext.doc.PublicKeyEd25519))
		signer.Ed25519PublicKey = pub
		return signer, nil
	}
	// Optional scheduled local rotation. The next key becomes the signer only at/after not_before.
	if nextRaw := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_NEXT_PRIVATE_KEY")); nextRaw != "" {
		nbRaw := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_NEXT_NOT_BEFORE"))
		nb, parseErr := time.Parse(time.RFC3339, nbRaw)
		if parseErr != nil {
			return Signer{}, fmt.Errorf("invalid NEXT_NOT_BEFORE: %w", parseErr)
		}
		if !time.Now().UTC().Before(nb.UTC()) {
			next, parseErr := parseEd25519PrivateKey([]byte(nextRaw))
			if parseErr != nil {
				return Signer{}, parseErr
			}
			priv = next
			keyID = strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_ED25519_NEXT_KEY_ID"))
			if keyID == "" {
				keyID = defaultEd25519KeyID(next.Public().(ed25519.PublicKey))
			}
		}
	}
	signer.Ed25519PrivateKey = priv
	signer.Ed25519KeyID = keyID
	if len(priv) > 0 {
		signer.Ed25519PublicKey = priv.Public().(ed25519.PublicKey)
	}
	return signer, nil
}

func NewReport(task *domain.MigrationTask, archive *domain.ValidationArchive, product, version string) (Report, error) {
	if task == nil || archive == nil {
		return Report{}, fmt.Errorf("task and validation archive are required")
	}
	if task.ID == "" || archive.TaskID == "" || task.ID != archive.TaskID {
		return Report{}, fmt.Errorf("task/archive identity mismatch")
	}
	if strings.TrimSpace(archive.EvidenceDigest) == "" {
		return Report{}, fmt.Errorf("validation archive evidence digest is empty")
	}
	archived := *archive
	archived.ArchivedAt = archive.ArchivedAt.UTC()
	archived.Tables = append([]domain.ValidationTableArchive(nil), archive.Tables...)
	for i := range archived.Tables {
		if !archived.Tables[i].FirstStartedAt.IsZero() {
			archived.Tables[i].FirstStartedAt = archived.Tables[i].FirstStartedAt.UTC()
		}
		if !archived.Tables[i].LastFinishedAt.IsZero() {
			archived.Tables[i].LastFinishedAt = archived.Tables[i].LastFinishedAt.UTC()
		}
	}
	sort.SliceStable(archived.Tables, func(i, j int) bool {
		if archived.Tables[i].TableID == archived.Tables[j].TableID {
			return archived.Tables[i].EvidenceDigest < archived.Tables[j].EvidenceDigest
		}
		return archived.Tables[i].TableID < archived.Tables[j].TableID
	})
	return Report{
		SchemaVersion: SchemaVersion, Product: product, ProductVersion: version,
		GeneratedAt: archived.ArchivedAt,
		Task:        TaskSnapshot{ID: task.ID, Name: task.Name, Mode: task.Mode, Status: task.Status, SourceID: task.SourceID, TargetID: task.TargetID, RowsMigrated: task.RowsMigrated, BytesMigrated: task.BytesMigrated, CreatedAt: task.CreatedAt.UTC(), UpdatedAt: task.UpdatedAt.UTC()},
		Validation:  archived,
	}, nil
}

func sumHex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func hmacHex(key, b []byte) string {
	if len(key) == 0 {
		return ""
	}
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

func (s Signer) hasEd25519() bool {
	return len(s.Ed25519PublicKey) == ed25519.PublicKeySize || len(s.Ed25519PrivateKey) > 0 || s.ExternalEd25519 != nil
}
func (s Signer) signEd25519(data []byte) ([]byte, error) {
	if s.ExternalEd25519 != nil {
		return s.ExternalEd25519.sign(data)
	}
	if len(s.Ed25519PrivateKey) > 0 {
		return ed25519.Sign(s.Ed25519PrivateKey, data), nil
	}
	return nil, nil
}
func (s Signer) publicEd25519() ed25519.PublicKey {
	if len(s.Ed25519PublicKey) > 0 {
		return s.Ed25519PublicKey
	}
	if len(s.Ed25519PrivateKey) > 0 {
		return s.Ed25519PrivateKey.Public().(ed25519.PublicKey)
	}
	return nil
}

func artifact(name, contentType string, data []byte, signer Signer) (Artifact, error) {
	a := Artifact{Name: name, ContentType: contentType, SizeBytes: int64(len(data)), SHA256: sumHex(data), HMACSHA256: hmacHex(signer.Key, data), Data: data}
	if signer.hasEd25519() {
		sig, err := signer.signEd25519(data)
		if err != nil {
			return Artifact{}, err
		}
		a.Ed25519Signature = base64.StdEncoding.EncodeToString(sig)
	}
	return a, nil
}

func manifestSigningBytes(m Manifest) ([]byte, error) {
	m.ManifestHMACSHA256 = ""
	m.ManifestEd25519Signature = ""
	return json.Marshal(m)
}

func BuildBundle(r Report, signer Signer) (Bundle, error) {
	jsonBytes, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return Bundle{}, err
	}
	jsonBytes = append(jsonBytes, '\n')
	htmlBytes, err := renderHTML(r)
	if err != nil {
		return Bundle{}, err
	}
	pdfBytes, err := renderPDFWithOptionalRenderer(r, htmlBytes)
	if err != nil {
		return Bundle{}, err
	}
	base := safeBase(r.Task.ID)
	arts := make([]Artifact, 0, 3)
	for _, spec := range []struct {
		name, ctype string
		data        []byte
	}{{base + "-validation-report.json", "application/json; charset=utf-8", jsonBytes}, {base + "-validation-report.html", "text/html; charset=utf-8", htmlBytes}, {base + "-validation-report.pdf", "application/pdf", pdfBytes}} {
		a, e := artifact(spec.name, spec.ctype, spec.data, signer)
		if e != nil {
			return Bundle{}, e
		}
		arts = append(arts, a)
	}
	manifest := Manifest{SchemaVersion: SchemaVersion + "/manifest", Product: r.Product, ProductVersion: r.ProductVersion, TaskID: r.Task.ID, ArchiveEvidenceDigest: r.Validation.EvidenceDigest, GeneratedAt: r.GeneratedAt, Artifacts: stripArtifactData(arts)}
	if len(signer.Key) > 0 {
		manifest.SignatureAlgorithm = "HMAC-SHA256"
		manifest.SignatureKeyID = signer.KeyID
	}
	if signer.hasEd25519() {
		doc, err := PublicKeyDocumentForSigner(signer)
		if err != nil {
			return Bundle{}, err
		}
		manifest.PublicSignatureAlgorithm = doc.Algorithm
		manifest.PublicSignatureKeyID = doc.KeyID
		manifest.PublicKeyEd25519 = doc.PublicKeyEd25519
		manifest.PublicKeyFingerprintSHA256 = doc.FingerprintSHA256
	}
	unsigned, err := manifestSigningBytes(manifest)
	if err != nil {
		return Bundle{}, err
	}
	if len(signer.Key) > 0 {
		manifest.ManifestHMACSHA256 = hmacHex(signer.Key, unsigned)
	}
	if signer.hasEd25519() {
		sig, e := signer.signEd25519(unsigned)
		if e != nil {
			return Bundle{}, e
		}
		manifest.ManifestEd25519Signature = base64.StdEncoding.EncodeToString(sig)
	}
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return Bundle{}, err
	}
	manifestJSON = append(manifestJSON, '\n')
	var shaLines, hmacLines, edLines []string
	for _, a := range arts {
		shaLines = append(shaLines, a.SHA256+"  "+a.Name)
		if a.HMACSHA256 != "" {
			hmacLines = append(hmacLines, a.HMACSHA256+"  "+a.Name)
		}
		if a.Ed25519Signature != "" {
			edLines = append(edLines, a.Ed25519Signature+"  "+a.Name)
		}
	}
	shaLines = append(shaLines, sumHex(manifestJSON)+"  manifest.json")
	sort.Strings(shaLines)
	sort.Strings(hmacLines)
	sort.Strings(edLines)
	shaSums := []byte(strings.Join(shaLines, "\n") + "\n")
	var hmacSums, edSums []byte
	if len(hmacLines) > 0 {
		hmacSums = []byte(strings.Join(hmacLines, "\n") + "\n")
	}
	if len(edLines) > 0 {
		edSums = []byte(strings.Join(edLines, "\n") + "\n")
	}
	return Bundle{Report: r, Artifacts: arts, Manifest: manifest, ManifestJSON: manifestJSON, SHA256SUMS: shaSums, HMACSHA256SUMS: hmacSums, ED25519SIGNATURES: edSums}, nil
}

func stripArtifactData(in []Artifact) []Artifact {
	out := make([]Artifact, len(in))
	for i, a := range in {
		a.Data = nil
		out[i] = a
	}
	return out
}

func safeBase(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "qmigration"
	}
	var b strings.Builder
	for _, r := range v {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func FindArtifact(b Bundle, format string) (Artifact, bool) {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "html"
	}
	suffix := "." + format
	for _, a := range b.Artifacts {
		if strings.HasSuffix(a.Name, suffix) {
			return a, true
		}
	}
	return Artifact{}, false
}

var htmlTemplate = template.Must(template.New("validation-report").Funcs(template.FuncMap{
	"fmtTime": func(t time.Time) string {
		if t.IsZero() {
			return "-"
		}
		return t.UTC().Format(time.RFC3339)
	},
}).Parse(`<!doctype html>
<html><head><meta charset="utf-8"><title>QMigration Validation Report - {{.Task.ID}}</title>
<style>body{font-family:Arial,sans-serif;margin:40px;color:#222}h1,h2{margin-bottom:8px}.meta{display:grid;grid-template-columns:220px 1fr;gap:6px 14px;margin-bottom:24px}.ok{color:#16794a;font-weight:700}.bad{color:#b42318;font-weight:700}table{border-collapse:collapse;width:100%;font-size:13px}th,td{border:1px solid #ddd;padding:7px;text-align:left}th{background:#f4f5f7}.mono{font-family:monospace;word-break:break-all}.footer{margin-top:28px;font-size:12px;color:#666}</style></head><body>
<h1>QMigration Validation Acceptance Report</h1>
<div class="meta"><b>Task</b><span>{{.Task.Name}} ({{.Task.ID}})</span><b>Mode</b><span>{{.Task.Mode}}</span><b>Terminal status</b><span>{{.Validation.TerminalStatus}}</span><b>Generated</b><span>{{fmtTime .GeneratedAt}}</span><b>Product version</b><span>{{.ProductVersion}}</span><b>Archive evidence SHA-256</b><span class="mono">{{.Validation.EvidenceDigest}}</span></div>
<h2>Validation summary</h2><div class="meta"><b>Total tables</b><span>{{.Validation.TotalTables}}</span><b>Total chunks</b><span>{{.Validation.TotalChunks}}</span><b>Covered chunks</b><span>{{.Validation.CoveredChunks}}</span><b>Successful</b><span class="ok">{{.Validation.SuccessChunks}}</span><b>Mismatch</b><span class="bad">{{.Validation.MismatchChunks}}</span><b>Error</b><span class="bad">{{.Validation.ErrorChunks}}</span><b>Missing</b><span class="bad">{{.Validation.MissingChunks}}</span><b>Barrier</b><span>{{.Validation.ValidationBarrierPositionType}} {{.Validation.ValidationBarrierPosition}} {{.Validation.ValidationBarrierResource}}</span></div>
<h2>Table evidence</h2><table><thead><tr><th>Source</th><th>Target</th><th>Scope</th><th>Chunks</th><th>Success/Mismatch/Error/Missing</th><th>Source rows</th><th>Target rows</th><th>Evidence SHA-256</th></tr></thead><tbody>{{range .Validation.Tables}}<tr><td>{{.SourceSchema}}.{{.SourceTable}}</td><td>{{.TargetSchema}}.{{.TargetTable}}</td><td>{{.EvidenceScope}}</td><td>{{.CoveredChunks}} / {{.TotalChunks}}</td><td>{{.SuccessChunks}} / {{.MismatchChunks}} / {{.ErrorChunks}} / {{.MissingChunks}}</td><td>{{.SourceRows}}</td><td>{{.TargetRows}}</td><td class="mono">{{.EvidenceDigest}}</td></tr>{{end}}</tbody></table>
<div class="footer">This report is derived from QMigration's immutable Validation Archive. Verify the artifact SHA-256/HMAC values in the accompanying manifest before relying on a copied report.</div></body></html>`))

func renderHTML(r Report) ([]byte, error) {
	var b strings.Builder
	if err := htmlTemplate.Execute(&b, r); err != nil {
		return nil, err
	}
	b.WriteByte('\n')
	return []byte(b.String()), nil
}
