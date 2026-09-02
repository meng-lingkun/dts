package validationreport

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	TrustStoreSchemaVersion    = SchemaVersion + "/trust-store/v1"
	KeyTransitionSchemaVersion = SchemaVersion + "/key-transition/v1"
	KeyRevocationSchemaVersion = SchemaVersion + "/key-revocation/v1"
	TrustKeyPending            = "PENDING"
	TrustKeyActive             = "ACTIVE"
	TrustKeyRetired            = "RETIRED"
	TrustKeyRevoked            = "REVOKED"
)

type KeyTransitionCertificate struct {
	SchemaVersion    string            `json:"schema_version"`
	From             PublicKeyDocument `json:"from"`
	To               PublicKeyDocument `json:"to"`
	IssuedAt         time.Time         `json:"issued_at"`
	NotBefore        time.Time         `json:"not_before,omitempty"`
	OverlapUntil     time.Time         `json:"overlap_until,omitempty"`
	Reason           string            `json:"reason,omitempty"`
	SignatureEd25519 string            `json:"signature_ed25519"`
}

type KeyRevocationCertificate struct {
	SchemaVersion    string            `json:"schema_version"`
	Issuer           PublicKeyDocument `json:"issuer"`
	Target           PublicKeyDocument `json:"target"`
	RevokedAt        time.Time         `json:"revoked_at"`
	Reason           string            `json:"reason"`
	SignatureEd25519 string            `json:"signature_ed25519"`
}

type TrustKey struct {
	KeyID                       string    `json:"key_id"`
	PublicKeyEd25519            string    `json:"public_key_ed25519"`
	FingerprintSHA256           string    `json:"fingerprint_sha256"`
	Status                      string    `json:"status"`
	TrustedAt                   time.Time `json:"trusted_at"`
	NotBefore                   time.Time `json:"not_before,omitempty"`
	RetireAfter                 time.Time `json:"retire_after,omitempty"`
	RetiredAt                   time.Time `json:"retired_at,omitempty"`
	RevokedAt                   time.Time `json:"revoked_at,omitempty"`
	StatusReason                string    `json:"status_reason,omitempty"`
	ParentKeyID                 string    `json:"parent_key_id,omitempty"`
	TransitionCertificateSHA256 string    `json:"transition_certificate_sha256,omitempty"`
	RevocationCertificateSHA256 string    `json:"revocation_certificate_sha256,omitempty"`
}

type TrustStore struct {
	SchemaVersion string     `json:"schema_version"`
	UpdatedAt     time.Time  `json:"updated_at"`
	Keys          []TrustKey `json:"keys"`
}

func normalizePublicKeyDocument(doc PublicKeyDocument) (PublicKeyDocument, ed25519.PublicKey, error) {
	if doc.Algorithm == "" {
		doc.Algorithm = "Ed25519"
	}
	if doc.Algorithm != "Ed25519" {
		return doc, nil, fmt.Errorf("unsupported public key algorithm %q", doc.Algorithm)
	}
	pub, err := parseEd25519PublicKeyBytes([]byte(doc.PublicKeyEd25519))
	if err != nil {
		return doc, nil, err
	}
	fp := fingerprintPublicKey(pub)
	if doc.FingerprintSHA256 != "" && !strings.EqualFold(doc.FingerprintSHA256, fp) {
		return doc, nil, errors.New("public key document fingerprint does not match key bytes")
	}
	if doc.KeyID == "" {
		doc.KeyID = defaultEd25519KeyID(pub)
	}
	doc.SchemaVersion = PublicKeySchemaVersion
	doc.PublicKeyEd25519 = base64.StdEncoding.EncodeToString(pub)
	doc.FingerprintSHA256 = fp
	return doc, pub, nil
}

func transitionSigningBytes(cert KeyTransitionCertificate) ([]byte, error) {
	cert.SignatureEd25519 = ""
	return json.Marshal(cert)
}

func revocationSigningBytes(cert KeyRevocationCertificate) ([]byte, error) {
	cert.SignatureEd25519 = ""
	return json.Marshal(cert)
}

func NewKeyTransitionCertificate(signer Signer, to PublicKeyDocument, issuedAt time.Time, reason string) (KeyTransitionCertificate, error) {
	if !signer.hasEd25519() {
		return KeyTransitionCertificate{}, errors.New("Ed25519 signing key is not configured")
	}
	from, err := PublicKeyDocumentForSigner(signer)
	if err != nil || from == nil {
		return KeyTransitionCertificate{}, errors.New("current Ed25519 public key is unavailable")
	}
	normalizedTo, _, err := normalizePublicKeyDocument(to)
	if err != nil {
		return KeyTransitionCertificate{}, fmt.Errorf("normalize next public key: %w", err)
	}
	if strings.EqualFold(from.FingerprintSHA256, normalizedTo.FingerprintSHA256) {
		return KeyTransitionCertificate{}, errors.New("key transition target is the current Ed25519 key")
	}
	if issuedAt.IsZero() {
		issuedAt = time.Now().UTC()
	}
	cert := KeyTransitionCertificate{SchemaVersion: KeyTransitionSchemaVersion, From: *from, To: normalizedTo, IssuedAt: issuedAt.UTC(), Reason: strings.TrimSpace(reason)}
	payload, err := transitionSigningBytes(cert)
	if err != nil {
		return KeyTransitionCertificate{}, err
	}
	sig, err := signer.signEd25519(payload)
	if err != nil {
		return KeyTransitionCertificate{}, err
	}
	cert.SignatureEd25519 = base64.StdEncoding.EncodeToString(sig)
	return cert, nil
}

func NewScheduledKeyTransitionCertificate(signer Signer, to PublicKeyDocument, issuedAt, notBefore, overlapUntil time.Time, reason string) (KeyTransitionCertificate, error) {
	cert, err := NewKeyTransitionCertificate(signer, to, issuedAt, reason)
	if err != nil {
		return cert, err
	}
	if notBefore.IsZero() {
		notBefore = issuedAt
	}
	if notBefore.Before(issuedAt) {
		return cert, errors.New("key transition not_before cannot predate issued_at")
	}
	if !overlapUntil.IsZero() && overlapUntil.Before(notBefore) {
		return cert, errors.New("key transition overlap_until cannot predate not_before")
	}
	cert.NotBefore = notBefore.UTC()
	if !overlapUntil.IsZero() {
		cert.OverlapUntil = overlapUntil.UTC()
	}
	payload, err := transitionSigningBytes(cert)
	if err != nil {
		return cert, err
	}
	sig, err := signer.signEd25519(payload)
	if err != nil {
		return cert, err
	}
	cert.SignatureEd25519 = base64.StdEncoding.EncodeToString(sig)
	return cert, nil
}

func VerifyKeyTransitionCertificate(cert KeyTransitionCertificate) error {
	if cert.SchemaVersion != KeyTransitionSchemaVersion {
		return fmt.Errorf("unsupported key transition schema %q", cert.SchemaVersion)
	}
	from, pub, err := normalizePublicKeyDocument(cert.From)
	if err != nil {
		return fmt.Errorf("transition from key: %w", err)
	}
	to, _, err := normalizePublicKeyDocument(cert.To)
	if err != nil {
		return fmt.Errorf("transition to key: %w", err)
	}
	if strings.EqualFold(from.FingerprintSHA256, to.FingerprintSHA256) {
		return errors.New("transition source and target keys are identical")
	}
	if !cert.NotBefore.IsZero() && cert.NotBefore.Before(cert.IssuedAt) {
		return errors.New("transition not_before predates issued_at")
	}
	if !cert.OverlapUntil.IsZero() {
		nb := cert.NotBefore
		if nb.IsZero() {
			nb = cert.IssuedAt
		}
		if cert.OverlapUntil.Before(nb) {
			return errors.New("transition overlap_until predates not_before")
		}
	}
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(cert.SignatureEd25519))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return errors.New("invalid key transition Ed25519 signature encoding")
	}
	cert.From, cert.To = from, to
	payload, err := transitionSigningBytes(cert)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, payload, sig) {
		return errors.New("key transition Ed25519 signature verification failed")
	}
	return nil
}

func NewKeyRevocationCertificate(signer Signer, target PublicKeyDocument, revokedAt time.Time, reason string) (KeyRevocationCertificate, error) {
	if !signer.hasEd25519() {
		return KeyRevocationCertificate{}, errors.New("Ed25519 signing key is not configured")
	}
	issuer, err := PublicKeyDocumentForSigner(signer)
	if err != nil || issuer == nil {
		return KeyRevocationCertificate{}, errors.New("current Ed25519 public key is unavailable")
	}
	normalizedTarget, _, err := normalizePublicKeyDocument(target)
	if err != nil {
		return KeyRevocationCertificate{}, fmt.Errorf("normalize revocation target: %w", err)
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return KeyRevocationCertificate{}, errors.New("key revocation reason is required")
	}
	if revokedAt.IsZero() {
		revokedAt = time.Now().UTC()
	}
	cert := KeyRevocationCertificate{SchemaVersion: KeyRevocationSchemaVersion, Issuer: *issuer, Target: normalizedTarget, RevokedAt: revokedAt.UTC(), Reason: reason}
	payload, err := revocationSigningBytes(cert)
	if err != nil {
		return KeyRevocationCertificate{}, err
	}
	sig, err := signer.signEd25519(payload)
	if err != nil {
		return KeyRevocationCertificate{}, err
	}
	cert.SignatureEd25519 = base64.StdEncoding.EncodeToString(sig)
	return cert, nil
}

func VerifyKeyRevocationCertificate(cert KeyRevocationCertificate) error {
	if cert.SchemaVersion != KeyRevocationSchemaVersion {
		return fmt.Errorf("unsupported key revocation schema %q", cert.SchemaVersion)
	}
	issuer, pub, err := normalizePublicKeyDocument(cert.Issuer)
	if err != nil {
		return fmt.Errorf("revocation issuer key: %w", err)
	}
	target, _, err := normalizePublicKeyDocument(cert.Target)
	if err != nil {
		return fmt.Errorf("revocation target key: %w", err)
	}
	if strings.TrimSpace(cert.Reason) == "" {
		return errors.New("key revocation reason is required")
	}
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(cert.SignatureEd25519))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return errors.New("invalid key revocation Ed25519 signature encoding")
	}
	cert.Issuer, cert.Target = issuer, target
	payload, err := revocationSigningBytes(cert)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, payload, sig) {
		return errors.New("key revocation Ed25519 signature verification failed")
	}
	return nil
}

func NewTrustStore(root PublicKeyDocument, trustedAt time.Time) (*TrustStore, error) {
	doc, _, err := normalizePublicKeyDocument(root)
	if err != nil {
		return nil, err
	}
	if trustedAt.IsZero() {
		trustedAt = time.Now().UTC()
	}
	return &TrustStore{SchemaVersion: TrustStoreSchemaVersion, UpdatedAt: trustedAt.UTC(), Keys: []TrustKey{{KeyID: doc.KeyID, PublicKeyEd25519: doc.PublicKeyEd25519, FingerprintSHA256: doc.FingerprintSHA256, Status: TrustKeyActive, TrustedAt: trustedAt.UTC()}}}, nil
}

func (s *TrustStore) validate() error {
	if s == nil {
		return errors.New("nil trust store")
	}
	if s.SchemaVersion != TrustStoreSchemaVersion {
		return fmt.Errorf("unsupported trust store schema %q", s.SchemaVersion)
	}
	ids, fps := map[string]bool{}, map[string]bool{}
	active := 0
	for i := range s.Keys {
		k := &s.Keys[i]
		doc, _, err := normalizePublicKeyDocument(PublicKeyDocument{Algorithm: "Ed25519", KeyID: k.KeyID, PublicKeyEd25519: k.PublicKeyEd25519, FingerprintSHA256: k.FingerprintSHA256})
		if err != nil {
			return fmt.Errorf("trust key %q: %w", k.KeyID, err)
		}
		k.KeyID, k.PublicKeyEd25519, k.FingerprintSHA256 = doc.KeyID, doc.PublicKeyEd25519, doc.FingerprintSHA256
		if ids[k.KeyID] || fps[strings.ToLower(k.FingerprintSHA256)] {
			return errors.New("trust store contains duplicate key id or fingerprint")
		}
		ids[k.KeyID], fps[strings.ToLower(k.FingerprintSHA256)] = true, true
		switch k.Status {
		case TrustKeyActive:
			active++
		case TrustKeyPending, TrustKeyRetired, TrustKeyRevoked:
		default:
			return fmt.Errorf("trust key %q has invalid status %q", k.KeyID, k.Status)
		}
	}
	if len(s.Keys) == 0 || active == 0 {
		return errors.New("trust store must contain at least one ACTIVE key")
	}
	return nil
}

func (s *TrustStore) findKey(keyID, fingerprint string) (*TrustKey, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	fingerprint = strings.ToLower(strings.TrimSpace(fingerprint))
	for i := range s.Keys {
		k := &s.Keys[i]
		if keyID != "" && k.KeyID == keyID {
			if fingerprint != "" && !strings.EqualFold(k.FingerprintSHA256, fingerprint) {
				return nil, errors.New("trust store key id matches but fingerprint differs")
			}
			return k, nil
		}
		if keyID == "" && fingerprint != "" && strings.EqualFold(k.FingerprintSHA256, fingerprint) {
			return k, nil
		}
	}
	return nil, errors.New("report signing key is not present in trust store")
}

func certificateDigest(v any) (string, error) {
	b, err := marshalIndentedNewline(v)
	if err != nil {
		return "", err
	}
	return SHA256Hex(b), nil
}

func cloneTrustStore(s *TrustStore) *TrustStore {
	if s == nil {
		return nil
	}
	out := *s
	out.Keys = append([]TrustKey(nil), s.Keys...)
	return &out
}

func (s *TrustStore) ApplyTransition(cert KeyTransitionCertificate) error {
	if err := VerifyKeyTransitionCertificate(cert); err != nil {
		return err
	}
	work := cloneTrustStore(s)
	from, err := work.findKey(cert.From.KeyID, cert.From.FingerprintSHA256)
	if err != nil {
		return fmt.Errorf("transition source is not trusted: %w", err)
	}
	if from.Status != TrustKeyActive {
		return fmt.Errorf("cannot rotate from trust key %q in status %s; only ACTIVE keys may extend trust", from.KeyID, from.Status)
	}
	if existing, findErr := work.findKey(cert.To.KeyID, cert.To.FingerprintSHA256); findErr == nil {
		if existing.ParentKeyID == cert.From.KeyID || existing.KeyID == cert.To.KeyID {
			return nil
		}
		return errors.New("transition target already exists with a different trust path")
	}
	digest, err := certificateDigest(cert)
	if err != nil {
		return err
	}
	nb := cert.NotBefore.UTC()
	if cert.NotBefore.IsZero() {
		nb = cert.IssuedAt.UTC()
	}
	status := TrustKeyActive
	if nb.After(cert.IssuedAt.UTC()) {
		status = TrustKeyPending
	} else if cert.OverlapUntil.IsZero() {
		from.Status = TrustKeyRetired
		from.RetiredAt = nb
		from.StatusReason = strings.TrimSpace(cert.Reason)
	}
	if !cert.OverlapUntil.IsZero() {
		from.RetireAfter = cert.OverlapUntil.UTC()
	}
	work.Keys = append(work.Keys, TrustKey{KeyID: cert.To.KeyID, PublicKeyEd25519: cert.To.PublicKeyEd25519, FingerprintSHA256: cert.To.FingerprintSHA256, Status: status, TrustedAt: cert.IssuedAt.UTC(), NotBefore: nb, ParentKeyID: cert.From.KeyID, TransitionCertificateSHA256: digest})
	work.UpdatedAt = cert.IssuedAt.UTC()
	sort.SliceStable(work.Keys, func(i, j int) bool { return work.Keys[i].TrustedAt.Before(work.Keys[j].TrustedAt) })
	if err := work.validate(); err != nil {
		return err
	}
	*s = *work
	return nil
}

func (s *TrustStore) ApplyRevocation(cert KeyRevocationCertificate) error {
	if err := VerifyKeyRevocationCertificate(cert); err != nil {
		return err
	}
	work := cloneTrustStore(s)
	issuer, err := work.findKey(cert.Issuer.KeyID, cert.Issuer.FingerprintSHA256)
	if err != nil {
		return fmt.Errorf("revocation issuer is not trusted: %w", err)
	}
	if issuer.Status != TrustKeyActive {
		return fmt.Errorf("revocation certificate issuer %q is %s; only ACTIVE keys may revoke trust", issuer.KeyID, issuer.Status)
	}
	target, err := work.findKey(cert.Target.KeyID, cert.Target.FingerprintSHA256)
	if err != nil {
		return fmt.Errorf("revocation target is not trusted: %w", err)
	}
	digest, err := certificateDigest(cert)
	if err != nil {
		return err
	}
	if target.Status == TrustKeyRevoked {
		if strings.EqualFold(target.RevocationCertificateSHA256, digest) {
			return nil
		}
		return errors.New("trust key is already revoked by a different certificate")
	}
	target.Status = TrustKeyRevoked
	target.RevokedAt = cert.RevokedAt.UTC()
	target.StatusReason = strings.TrimSpace(cert.Reason)
	target.RevocationCertificateSHA256 = digest
	work.UpdatedAt = cert.RevokedAt.UTC()
	if err := work.validate(); err != nil {
		return fmt.Errorf("revocation would invalidate trust store: %w", err)
	}
	*s = *work
	return nil
}

func (s *TrustStore) VerifyManifestTrust(m Manifest) (ed25519.PublicKey, TrustKey, []string, error) {
	pub, err := manifestPublicKey(m)
	if err != nil {
		return nil, TrustKey{}, nil, err
	}
	fp := fingerprintPublicKey(pub)
	key, err := s.findKey(m.PublicSignatureKeyID, fp)
	if err != nil {
		return nil, TrustKey{}, nil, err
	}
	if key.PublicKeyEd25519 != base64.StdEncoding.EncodeToString(pub) {
		return nil, TrustKey{}, nil, errors.New("manifest key bytes do not match trust store")
	}
	if key.Status == TrustKeyRevoked {
		return nil, *key, nil, fmt.Errorf("report signing key %q is REVOKED: %s", key.KeyID, key.StatusReason)
	}
	effective := *key
	if key.Status == TrustKeyPending {
		if key.NotBefore.IsZero() || m.GeneratedAt.Before(key.NotBefore) {
			return nil, *key, nil, fmt.Errorf("report predates scheduled activation for key %q", key.KeyID)
		}
		effective.Status = TrustKeyActive
	}
	if key.Status == TrustKeyActive && !key.RetireAfter.IsZero() && m.GeneratedAt.After(key.RetireAfter) {
		effective.Status = TrustKeyRetired
		effective.RetiredAt = key.RetireAfter
	}
	key = &effective
	if key.ParentKeyID != "" && !key.NotBefore.IsZero() && m.GeneratedAt.Before(key.NotBefore) {
		return nil, *key, nil, fmt.Errorf("report predates signed trust transition not_before for key %q", key.KeyID)
	}
	if key.ParentKeyID != "" && !key.TrustedAt.IsZero() && m.GeneratedAt.Before(key.TrustedAt) {
		return nil, *key, nil, fmt.Errorf("report predates signed trust transition for key %q", key.KeyID)
	}
	if key.Status == TrustKeyRetired && !key.RetiredAt.IsZero() && m.GeneratedAt.After(key.RetiredAt) {
		return nil, *key, nil, fmt.Errorf("report was signed after key %q retirement time", key.KeyID)
	}
	path := []string{key.KeyID}
	parent := key.ParentKeyID
	seen := map[string]bool{key.KeyID: true}
	for parent != "" {
		if seen[parent] {
			return nil, *key, nil, errors.New("trust store contains a key-transition cycle")
		}
		seen[parent] = true
		p, err := s.findKey(parent, "")
		if err != nil {
			return nil, *key, nil, errors.New("trust path is incomplete")
		}
		path = append(path, p.KeyID)
		parent = p.ParentKeyID
	}
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	return pub, *key, path, nil
}

func LoadTrustStore(path string) (*TrustStore, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s TrustStore
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("parse trust store: %w", err)
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

func SaveTrustStore(path string, store *TrustStore) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("trust store path is required")
	}
	if err := store.validate(); err != nil {
		return err
	}
	b, err := marshalIndentedNewline(store)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".qmigration-trust-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func LoadPublicKeyDocumentFile(path string) (PublicKeyDocument, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return PublicKeyDocument{}, err
	}
	var doc PublicKeyDocument
	if err := json.Unmarshal(b, &doc); err == nil && strings.TrimSpace(doc.PublicKeyEd25519) != "" {
		normalized, _, err := normalizePublicKeyDocument(doc)
		return normalized, err
	}
	pub, err := parseEd25519PublicKeyBytes(b)
	if err != nil {
		return PublicKeyDocument{}, err
	}
	normalized, _, err := normalizePublicKeyDocument(PublicKeyDocument{Algorithm: "Ed25519", PublicKeyEd25519: base64.StdEncoding.EncodeToString(pub)})
	return normalized, err
}

func MarshalKeyTransitionCertificate(cert KeyTransitionCertificate) ([]byte, error) {
	if err := VerifyKeyTransitionCertificate(cert); err != nil {
		return nil, err
	}
	return marshalIndentedNewline(cert)
}

func ParseKeyTransitionCertificate(data []byte) (KeyTransitionCertificate, error) {
	var cert KeyTransitionCertificate
	if err := json.Unmarshal(data, &cert); err != nil {
		return cert, fmt.Errorf("parse key transition certificate: %w", err)
	}
	if err := VerifyKeyTransitionCertificate(cert); err != nil {
		return cert, err
	}
	return cert, nil
}

func MarshalKeyRevocationCertificate(cert KeyRevocationCertificate) ([]byte, error) {
	if err := VerifyKeyRevocationCertificate(cert); err != nil {
		return nil, err
	}
	return marshalIndentedNewline(cert)
}

func ParseKeyRevocationCertificate(data []byte) (KeyRevocationCertificate, error) {
	var cert KeyRevocationCertificate
	if err := json.Unmarshal(data, &cert); err != nil {
		return cert, fmt.Errorf("parse key revocation certificate: %w", err)
	}
	if err := VerifyKeyRevocationCertificate(cert); err != nil {
		return cert, err
	}
	return cert, nil
}
