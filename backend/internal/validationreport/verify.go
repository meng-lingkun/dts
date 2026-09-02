package validationreport

import (
	"bufio"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type VerificationResult struct {
	Verified                   bool     `json:"verified"`
	TaskID                     string   `json:"task_id"`
	ArchiveEvidenceDigest      string   `json:"archive_evidence_digest"`
	ManifestSHA256             string   `json:"manifest_sha256"`
	PublicSignatureAlgorithm   string   `json:"public_signature_algorithm,omitempty"`
	PublicSignatureKeyID       string   `json:"public_signature_key_id,omitempty"`
	PublicKeyFingerprintSHA256 string   `json:"public_key_fingerprint_sha256,omitempty"`
	TrustedPublicKeyPinned     bool     `json:"trusted_public_key_pinned"`
	TrustStorePinned           bool     `json:"trust_store_pinned"`
	TrustKeyStatus             string   `json:"trust_key_status,omitempty"`
	TrustPath                  []string `json:"trust_path,omitempty"`
	ArtifactsVerified          int      `json:"artifacts_verified"`
	SHA256SUMSVerified         bool     `json:"sha256sums_verified"`
	ReadyMarkerVerified        bool     `json:"ready_marker_verified"`
	TimestampVerified          bool     `json:"timestamp_verified,omitempty"`
	TimestampPolicyOID         string   `json:"timestamp_policy_oid,omitempty"`
	TimestampSerial            string   `json:"timestamp_serial,omitempty"`
	TimestampGenTime           string   `json:"timestamp_gen_time,omitempty"`
	Warnings                   []string `json:"warnings,omitempty"`
}

type readyMarker struct {
	TaskID                string `json:"task_id"`
	ArchiveEvidenceDigest string `json:"archive_evidence_digest"`
	ManifestSHA256        string `json:"manifest_sha256"`
}

func SHA256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func parseManifest(data []byte) (Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("parse manifest: %w", err)
	}
	if strings.TrimSpace(m.TaskID) == "" || strings.TrimSpace(m.ArchiveEvidenceDigest) == "" {
		return Manifest{}, errors.New("manifest is missing task_id or archive_evidence_digest")
	}
	return m, nil
}

func manifestPublicKey(m Manifest) (ed25519.PublicKey, error) {
	if m.PublicSignatureAlgorithm == "" && m.ManifestEd25519Signature == "" {
		return nil, nil
	}
	if m.PublicSignatureAlgorithm != "Ed25519" {
		return nil, fmt.Errorf("unsupported public signature algorithm %q", m.PublicSignatureAlgorithm)
	}
	pub, err := parseEd25519PublicKeyBytes([]byte(m.PublicKeyEd25519))
	if err != nil {
		return nil, err
	}
	fp := fingerprintPublicKey(pub)
	if m.PublicKeyFingerprintSHA256 != "" && !strings.EqualFold(m.PublicKeyFingerprintSHA256, fp) {
		return nil, errors.New("manifest public key fingerprint does not match embedded Ed25519 key")
	}
	return pub, nil
}

func verifyManifestSignatureWithPublicKey(m Manifest, pub ed25519.PublicKey) error {
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(m.ManifestEd25519Signature))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return errors.New("invalid manifest Ed25519 signature encoding")
	}
	payload, err := manifestSigningBytes(m)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, payload, sig) {
		return errors.New("manifest Ed25519 signature verification failed")
	}
	return nil
}

func VerifyManifestSignature(m Manifest, trustedPublicKey []byte) (ed25519.PublicKey, bool, error) {
	pub, err := manifestPublicKey(m)
	if err != nil {
		return nil, false, err
	}
	if len(pub) == 0 {
		return nil, false, errors.New("manifest does not contain an Ed25519 signature")
	}
	pinned := false
	if len(trustedPublicKey) > 0 {
		trustedRaw := []byte(strings.TrimSpace(string(trustedPublicKey)))
		var trustedDoc PublicKeyDocument
		if len(trustedRaw) > 0 && trustedRaw[0] == '{' && json.Unmarshal(trustedRaw, &trustedDoc) == nil && trustedDoc.PublicKeyEd25519 != "" {
			if trustedDoc.Algorithm != "" && trustedDoc.Algorithm != "Ed25519" {
				return nil, false, fmt.Errorf("trusted public key document algorithm %q is not Ed25519", trustedDoc.Algorithm)
			}
		}
		trusted, err := parseEd25519PublicKeyBytes(trustedRaw)
		if err != nil {
			return nil, false, fmt.Errorf("parse trusted public key: %w", err)
		}
		trustedFP := fingerprintPublicKey(trusted)
		if trustedDoc.FingerprintSHA256 != "" && !strings.EqualFold(trustedDoc.FingerprintSHA256, trustedFP) {
			return nil, false, errors.New("trusted public key document fingerprint does not match its key bytes")
		}
		if trustedDoc.KeyID != "" && m.PublicSignatureKeyID != "" && trustedDoc.KeyID != m.PublicSignatureKeyID {
			return nil, false, fmt.Errorf("manifest public signature key id %q does not match trusted key id %q", m.PublicSignatureKeyID, trustedDoc.KeyID)
		}
		if !ed25519.PublicKey(pub).Equal(trusted) {
			return nil, false, errors.New("manifest Ed25519 public key does not match trusted public key")
		}
		pinned = true
	}
	if err := verifyManifestSignatureWithPublicKey(m, pub); err != nil {
		return nil, false, err
	}
	return pub, pinned, nil
}

func safeLocalArtifactPath(dir, name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || filepath.IsAbs(name) || name != filepath.Base(name) || name == "." || name == ".." {
		return "", fmt.Errorf("unsafe artifact path %q", name)
	}
	return filepath.Join(dir, name), nil
}

func verifySHA256SUMS(dir string, manifestData []byte) (bool, error) {
	path := filepath.Join(dir, "SHA256SUMS")
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	seen := 0
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			return false, fmt.Errorf("invalid SHA256SUMS line %q", line)
		}
		want, name := strings.ToLower(parts[0]), parts[len(parts)-1]
		var data []byte
		if name == "manifest.json" {
			data = manifestData
		} else {
			artifactPath, pathErr := safeLocalArtifactPath(dir, name)
			if pathErr != nil {
				return false, pathErr
			}
			data, err = os.ReadFile(artifactPath)
			if err != nil {
				return false, fmt.Errorf("read SHA256SUMS artifact %s: %w", name, err)
			}
		}
		if SHA256Hex(data) != want {
			return false, fmt.Errorf("SHA256SUMS mismatch for %s", name)
		}
		seen++
	}
	if err := s.Err(); err != nil {
		return false, err
	}
	return seen > 0, nil
}

func verifyReportDirectory(dir string, trustedPublicKey []byte, trustStore *TrustStore) (VerificationResult, error) {
	dir = filepath.Clean(dir)
	manifestData, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return VerificationResult{}, fmt.Errorf("read manifest.json: %w", err)
	}
	m, err := parseManifest(manifestData)
	if err != nil {
		return VerificationResult{}, err
	}
	var pub ed25519.PublicKey
	var pinned bool
	var trustKey TrustKey
	var trustPath []string
	if trustStore != nil {
		pub, trustKey, trustPath, err = trustStore.VerifyManifestTrust(m)
		if err == nil {
			err = verifyManifestSignatureWithPublicKey(m, pub)
		}
		pinned = err == nil
	} else {
		pub, pinned, err = VerifyManifestSignature(m, trustedPublicKey)
	}
	if err != nil {
		return VerificationResult{}, err
	}
	out := VerificationResult{
		TaskID: m.TaskID, ArchiveEvidenceDigest: m.ArchiveEvidenceDigest,
		ManifestSHA256: SHA256Hex(manifestData), PublicSignatureAlgorithm: m.PublicSignatureAlgorithm,
		PublicSignatureKeyID: m.PublicSignatureKeyID, PublicKeyFingerprintSHA256: fingerprintPublicKey(pub),
		TrustedPublicKeyPinned: pinned, TrustStorePinned: trustStore != nil,
		TrustKeyStatus: trustKey.Status, TrustPath: trustPath,
	}
	for _, a := range m.Artifacts {
		artifactPath, pathErr := safeLocalArtifactPath(dir, a.Name)
		if pathErr != nil {
			return out, pathErr
		}
		data, err := os.ReadFile(artifactPath)
		if err != nil {
			return out, fmt.Errorf("read artifact %s: %w", a.Name, err)
		}
		if int64(len(data)) != a.SizeBytes {
			return out, fmt.Errorf("artifact %s size mismatch", a.Name)
		}
		if !strings.EqualFold(SHA256Hex(data), a.SHA256) {
			return out, fmt.Errorf("artifact %s SHA-256 mismatch", a.Name)
		}
		if strings.TrimSpace(a.Ed25519Signature) == "" {
			return out, fmt.Errorf("artifact %s is missing Ed25519 signature", a.Name)
		}
		sig, err := base64.StdEncoding.DecodeString(a.Ed25519Signature)
		if err != nil || len(sig) != ed25519.SignatureSize || !ed25519.Verify(pub, data, sig) {
			return out, fmt.Errorf("artifact %s Ed25519 signature verification failed", a.Name)
		}
		if strings.HasSuffix(a.Name, "-validation-report.json") {
			var report Report
			if err := json.Unmarshal(data, &report); err != nil {
				return out, fmt.Errorf("parse report JSON: %w", err)
			}
			if report.Task.ID != m.TaskID || !strings.EqualFold(report.Validation.EvidenceDigest, m.ArchiveEvidenceDigest) {
				return out, errors.New("report JSON identity/evidence does not match manifest")
			}
		}
		out.ArtifactsVerified++
	}
	if ok, err := verifySHA256SUMS(dir, manifestData); err != nil {
		return out, err
	} else {
		out.SHA256SUMSVerified = ok
	}
	readyPath := filepath.Join(dir, "READY.json")
	if b, err := os.ReadFile(readyPath); err == nil {
		var ready readyMarker
		if err := json.Unmarshal(b, &ready); err != nil {
			return out, fmt.Errorf("parse READY.json: %w", err)
		}
		if ready.TaskID != m.TaskID || !strings.EqualFold(ready.ArchiveEvidenceDigest, m.ArchiveEvidenceDigest) || !strings.EqualFold(ready.ManifestSHA256, out.ManifestSHA256) {
			return out, errors.New("READY.json does not match manifest")
		}
		out.ReadyMarkerVerified = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return out, err
	}
	if !pinned {
		out.Warnings = append(out.Warnings, "signature is mathematically valid but no external trusted public key was pinned")
	}
	if trustStore != nil && trustKey.Status == TrustKeyRetired {
		out.Warnings = append(out.Warnings, "report was signed by a RETIRED key; historical verification remains valid, but compromise-safe historical timing requires WORM/external trusted timestamping")
	}
	out.Verified = true
	return out, nil
}

func VerifyReportDirectory(dir string, trustedPublicKey []byte) (VerificationResult, error) {
	return verifyReportDirectory(dir, trustedPublicKey, nil)
}

func VerifyReportDirectoryWithTrustStore(dir string, store *TrustStore) (VerificationResult, error) {
	if store == nil {
		return VerificationResult{}, errors.New("trust store is required")
	}
	return verifyReportDirectory(dir, nil, store)
}

func LoadTrustedPublicKeyFile(path string) ([]byte, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	return os.ReadFile(path)
}
