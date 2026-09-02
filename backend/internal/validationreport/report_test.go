package validationreport

import (
	"bytes"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"qmigration/backend/internal/domain"
)

func testReport(t *testing.T) Report {
	t.Helper()
	ts := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	task := &domain.MigrationTask{ID: "m1", Name: "acceptance", Mode: domain.MigrationMode("FULL_CDC"), Status: domain.StatusFinished, SourceID: "src", TargetID: "dst", RowsMigrated: 100, BytesMigrated: 2048, CreatedAt: ts.Add(-time.Hour), UpdatedAt: ts}
	archive := &domain.ValidationArchive{TaskID: "m1", TerminalStatus: domain.StatusFinished, ValidationMode: "ROW_COUNT_CHECKSUM", TotalTables: 1, TotalChunks: 2, CoveredChunks: 2, SuccessChunks: 2, EvidenceDigest: strings.Repeat("a", 64), ArchivedAt: ts, Tables: []domain.ValidationTableArchive{{TableID: "t1", SourceSchema: "app", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders", EvidenceScope: "CHUNK_SET", ChecksumKind: "CHUNK_SET_SHA256", TotalChunks: 2, CoveredChunks: 2, SuccessChunks: 2, SourceRows: 100, TargetRows: 100, EvidenceDigest: strings.Repeat("b", 64)}}}
	r, err := NewReport(task, archive, "QMigration", "0.15.0-rc46")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestBuildBundleDeterministicAndSigned(t *testing.T) {
	r := testReport(t)
	signer := Signer{Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "ops-2026"}
	a, err := BuildBundle(r, signer)
	if err != nil {
		t.Fatal(err)
	}
	b, err := BuildBundle(r, signer)
	if err != nil {
		t.Fatal(err)
	}
	if string(a.ManifestJSON) != string(b.ManifestJSON) || string(a.SHA256SUMS) != string(b.SHA256SUMS) {
		t.Fatal("report bundle is not deterministic")
	}
	if len(a.Artifacts) != 3 {
		t.Fatalf("artifacts=%d", len(a.Artifacts))
	}
	pdf, ok := FindArtifact(a, "pdf")
	if !ok || !strings.HasPrefix(string(pdf.Data), "%PDF-1.4") {
		t.Fatal("missing built-in PDF")
	}
	html, ok := FindArtifact(a, "html")
	if !ok || !strings.Contains(string(html.Data), "Validation Acceptance Report") {
		t.Fatal("missing HTML report")
	}
	js, ok := FindArtifact(a, "json")
	if !ok {
		t.Fatal("missing JSON report")
	}
	var parsed Report
	if err := json.Unmarshal(js.Data, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.Validation.EvidenceDigest != r.Validation.EvidenceDigest {
		t.Fatal("evidence digest changed")
	}
	if a.Manifest.SignatureAlgorithm != "HMAC-SHA256" || a.Manifest.ManifestHMACSHA256 == "" {
		t.Fatal("manifest HMAC missing")
	}
	unsigned := a.Manifest
	sig := unsigned.ManifestHMACSHA256
	unsigned.ManifestHMACSHA256 = ""
	raw, _ := json.Marshal(unsigned)
	h := hmac.New(sha256.New, signer.Key)
	_, _ = h.Write(raw)
	if hex.EncodeToString(h.Sum(nil)) != sig {
		t.Fatal("manifest HMAC cannot be verified")
	}
}

func TestNewReportRejectsIdentityMismatch(t *testing.T) {
	r := testReport(t)
	a := r.Validation
	a.TaskID = "other"
	task := domain.MigrationTask{ID: "m1"}
	if _, err := NewReport(&task, &a, "QMigration", "x"); err == nil {
		t.Fatal("expected identity mismatch")
	}
}

func TestRC47Ed25519PublicVerificationAndTrustedKeyPin(t *testing.T) {
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	signer := Signer{Ed25519PrivateKey: priv, Ed25519KeyID: "acceptance-2026"}
	b, err := BuildBundle(testReport(t), signer)
	if err != nil {
		t.Fatal(err)
	}
	if b.Manifest.PublicSignatureAlgorithm != "Ed25519" || b.Manifest.ManifestEd25519Signature == "" || b.Manifest.PublicKeyFingerprintSHA256 == "" {
		t.Fatal("public Ed25519 manifest signature metadata missing")
	}
	for _, a := range b.Artifacts {
		if a.Ed25519Signature == "" {
			t.Fatalf("artifact %s missing Ed25519 signature", a.Name)
		}
	}
	if len(b.ED25519SIGNATURES) == 0 {
		t.Fatal("ED25519SIGNATURES not generated")
	}

	dir := t.TempDir()
	for _, a := range b.Artifacts {
		if err := os.WriteFile(filepath.Join(dir, a.Name), a.Data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b.ManifestJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SHA256SUMS"), b.SHA256SUMS, 0o600); err != nil {
		t.Fatal(err)
	}
	ready, _ := json.MarshalIndent(map[string]any{"task_id": b.Report.Task.ID, "archive_evidence_digest": b.Report.Validation.EvidenceDigest, "manifest_sha256": SHA256Hex(b.ManifestJSON)}, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "READY.json"), append(ready, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, err := PublicKeyDocumentForSigner(signer)
	if err != nil {
		t.Fatal(err)
	}
	pubJSON, err := MarshalPublicKeyDocument(doc)
	if err != nil {
		t.Fatal(err)
	}
	result, err := VerifyReportDirectory(dir, pubJSON)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Verified || !result.TrustedPublicKeyPinned || result.ArtifactsVerified != 3 || !result.ReadyMarkerVerified || !result.SHA256SUMSVerified {
		t.Fatalf("unexpected verification result: %+v", result)
	}
	wrongDoc := *doc
	wrongDoc.KeyID = "unexpected-key-id"
	wrongJSON, _ := MarshalPublicKeyDocument(&wrongDoc)
	if _, err := VerifyReportDirectory(dir, wrongJSON); err == nil {
		t.Fatal("trusted public key document key-id mismatch was accepted")
	}

	bad := append([]byte(nil), b.Artifacts[0].Data...)
	bad[0] ^= 1
	if err := os.WriteFile(filepath.Join(dir, b.Artifacts[0].Name), bad, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyReportDirectory(dir, pubJSON); err == nil {
		t.Fatal("tampered artifact was accepted")
	}
}

func TestRC47SignerFromEnvBase64SeedAndPublicKey(t *testing.T) {
	seed := bytes.Repeat([]byte{7}, ed25519.SeedSize)
	t.Setenv("QMIGRATION_VALIDATION_REPORT_ED25519_PRIVATE_KEY", base64.StdEncoding.EncodeToString(seed))
	t.Setenv("QMIGRATION_VALIDATION_REPORT_ED25519_KEY_ID", "customer-proof")
	s, err := SignerFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := PublicKeyDocumentForSigner(s)
	if err != nil {
		t.Fatal(err)
	}
	if doc == nil || doc.KeyID != "customer-proof" || len(doc.FingerprintSHA256) != 64 {
		t.Fatalf("unexpected public key doc: %+v", doc)
	}
}
