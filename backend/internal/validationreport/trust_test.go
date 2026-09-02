package validationreport

import (
	"crypto/ed25519"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func signerFromByte(seedByte byte, keyID string) Signer {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte + byte(i)
	}
	return Signer{Ed25519PrivateKey: ed25519.NewKeyFromSeed(seed), Ed25519KeyID: keyID}
}

func writeBundleDir(t *testing.T, b Bundle) string {
	t.Helper()
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
	return dir
}

func TestRC48TrustStoreRotationKeepsHistoricalReportsAndTrustsNewKey(t *testing.T) {
	oldSigner := signerFromByte(1, "acceptance-2026")
	newSigner := signerFromByte(41, "acceptance-2027")
	oldDoc, _ := PublicKeyDocumentForSigner(oldSigner)
	newDoc, _ := PublicKeyDocumentForSigner(newSigner)
	store, err := NewTrustStore(*oldDoc, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}

	oldReport := testReport(t) // 2026-09-01
	oldBundle, err := BuildBundle(oldReport, oldSigner)
	if err != nil {
		t.Fatal(err)
	}

	rotateAt := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	cert, err := NewKeyTransitionCertificate(oldSigner, *newDoc, rotateAt, "annual signing-key rotation")
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyKeyTransitionCertificate(cert); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyTransition(cert); err != nil {
		t.Fatal(err)
	}

	oldResult, err := VerifyReportDirectoryWithTrustStore(writeBundleDir(t, oldBundle), store)
	if err != nil {
		t.Fatal(err)
	}
	if !oldResult.Verified || oldResult.TrustKeyStatus != TrustKeyRetired || len(oldResult.TrustPath) != 1 || oldResult.TrustPath[0] != oldSigner.Ed25519KeyID {
		t.Fatalf("unexpected old-key verification result: %+v", oldResult)
	}

	newReport := testReport(t)
	newReport.GeneratedAt = rotateAt.Add(time.Hour)
	newReport.Validation.ArchivedAt = newReport.GeneratedAt
	newBundle, err := BuildBundle(newReport, newSigner)
	if err != nil {
		t.Fatal(err)
	}
	newResult, err := VerifyReportDirectoryWithTrustStore(writeBundleDir(t, newBundle), store)
	if err != nil {
		t.Fatal(err)
	}
	if !newResult.Verified || newResult.TrustKeyStatus != TrustKeyActive || strings.Join(newResult.TrustPath, ">") != "acceptance-2026>acceptance-2027" {
		t.Fatalf("unexpected new-key verification result: %+v", newResult)
	}
}

func TestRC48TrustStoreRejectsNewKeyReportPredatingTransition(t *testing.T) {
	oldSigner := signerFromByte(2, "old")
	newSigner := signerFromByte(52, "new")
	oldDoc, _ := PublicKeyDocumentForSigner(oldSigner)
	newDoc, _ := PublicKeyDocumentForSigner(newSigner)
	store, _ := NewTrustStore(*oldDoc, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	rotateAt := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	cert, _ := NewKeyTransitionCertificate(oldSigner, *newDoc, rotateAt, "rotation")
	if err := store.ApplyTransition(cert); err != nil {
		t.Fatal(err)
	}

	r := testReport(t) // generated 2026-09-01 < transition
	b, _ := BuildBundle(r, newSigner)
	if _, err := VerifyReportDirectoryWithTrustStore(writeBundleDir(t, b), store); err == nil || !strings.Contains(err.Error(), "predates signed trust transition") {
		t.Fatalf("expected pre-transition rejection, got %v", err)
	}
}

func TestRC48RevocationRejectsReportsAndIsFailClosed(t *testing.T) {
	oldSigner := signerFromByte(3, "old")
	newSigner := signerFromByte(63, "new")
	oldDoc, _ := PublicKeyDocumentForSigner(oldSigner)
	newDoc, _ := PublicKeyDocumentForSigner(newSigner)
	store, _ := NewTrustStore(*oldDoc, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	rotateAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	transition, _ := NewKeyTransitionCertificate(oldSigner, *newDoc, rotateAt, "rotation")
	if err := store.ApplyTransition(transition); err != nil {
		t.Fatal(err)
	}

	revocation, err := NewKeyRevocationCertificate(newSigner, *oldDoc, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), "old key compromise")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyRevocation(revocation); err != nil {
		t.Fatal(err)
	}

	oldBundle, _ := BuildBundle(testReport(t), oldSigner)
	if _, err := VerifyReportDirectoryWithTrustStore(writeBundleDir(t, oldBundle), store); err == nil || !strings.Contains(err.Error(), "REVOKED") {
		t.Fatalf("expected revoked key rejection, got %v", err)
	}

	// Revoking the only active key must fail without mutating the trust store.
	selfRevoke, _ := NewKeyRevocationCertificate(newSigner, *newDoc, time.Now().UTC(), "test")
	if err := store.ApplyRevocation(selfRevoke); err == nil {
		t.Fatal("expected revocation of last active key to fail")
	}
	key, err := store.findKey("new", "")
	if err != nil || key.Status != TrustKeyActive {
		t.Fatalf("failed revocation mutated trust store: %+v err=%v", key, err)
	}
}

func TestRC48TrustStoreSaveLoadAndTransitionTamper(t *testing.T) {
	oldSigner := signerFromByte(4, "root")
	newSigner := signerFromByte(74, "next")
	oldDoc, _ := PublicKeyDocumentForSigner(oldSigner)
	newDoc, _ := PublicKeyDocumentForSigner(newSigner)
	store, _ := NewTrustStore(*oldDoc, time.Now().UTC())
	cert, _ := NewKeyTransitionCertificate(oldSigner, *newDoc, time.Now().UTC().Add(time.Minute), "rotate")

	tampered := cert
	tampered.To.KeyID = "attacker"
	if err := VerifyKeyTransitionCertificate(tampered); err == nil {
		t.Fatal("tampered transition certificate verified")
	}
	if err := store.ApplyTransition(cert); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "trust.json")
	if err := SaveTrustStore(path, store); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadTrustStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Keys) != 2 || loaded.Keys[0].Status != TrustKeyRetired || loaded.Keys[1].Status != TrustKeyActive {
		t.Fatalf("unexpected loaded trust store: %+v", loaded)
	}
}

func TestRC48RetiredKeyCannotExtendOrRevokeTrust(t *testing.T) {
	oldSigner := signerFromByte(7, "old-root")
	activeSigner := signerFromByte(87, "active-next")
	attackerSigner := signerFromByte(117, "attacker-next")
	oldDoc, _ := PublicKeyDocumentForSigner(oldSigner)
	activeDoc, _ := PublicKeyDocumentForSigner(activeSigner)
	attackerDoc, _ := PublicKeyDocumentForSigner(attackerSigner)
	store, _ := NewTrustStore(*oldDoc, time.Now().UTC().Add(-time.Hour))
	first, _ := NewKeyTransitionCertificate(oldSigner, *activeDoc, time.Now().UTC(), "rotate")
	if err := store.ApplyTransition(first); err != nil {
		t.Fatal(err)
	}

	forgedExtension, _ := NewKeyTransitionCertificate(oldSigner, *attackerDoc, time.Now().UTC().Add(time.Minute), "retired key extension")
	if err := store.ApplyTransition(forgedExtension); err == nil || !strings.Contains(err.Error(), "only ACTIVE keys") {
		t.Fatalf("retired key unexpectedly extended trust: %v", err)
	}
	forgedRevoke, _ := NewKeyRevocationCertificate(oldSigner, *activeDoc, time.Now().UTC().Add(time.Minute), "retired key revoke")
	if err := store.ApplyRevocation(forgedRevoke); err == nil || !strings.Contains(err.Error(), "only ACTIVE keys") {
		t.Fatalf("retired key unexpectedly revoked trust: %v", err)
	}
}
