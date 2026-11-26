package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type SessionClaims struct {
	Federated         *FederatedIdentity `json:"federated,omitempty"`
	Nonce             string             `json:"jti,omitempty"`
	Subject           string             `json:"sub"`
	Username          string             `json:"username"`
	Role              Role               `json:"role"`
	Expires           int64              `json:"exp"`
	CredentialVersion string             `json:"cv,omitempty"`
}

type FederatedIdentity struct {
	Issuer    string `json:"iss"`
	Subject   string `json:"sub"`
	Assurance string `json:"acr,omitempty"`
}

type SessionManager struct {
	secret []byte
	ttl    time.Duration
}

func NewSessionManager(secret string, ttl time.Duration) *SessionManager {
	if ttl <= 0 {
		ttl = 12 * time.Hour
	}
	return &SessionManager{secret: []byte(secret), ttl: ttl}
}

func (m *SessionManager) Issue(userID, username string, role Role, passwordHash string) (string, time.Time, error) {
	return m.IssueWithIdentity(userID, username, role, passwordHash, nil)
}

func (m *SessionManager) IssueWithIdentity(userID, username string, role Role, passwordHash string, identity *FederatedIdentity) (string, time.Time, error) {
	if m == nil || len(m.secret) < 16 {
		return "", time.Time{}, errors.New("session secret must be at least 16 bytes")
	}
	expiry := time.Now().Add(m.ttl).UTC()
	claims := SessionClaims{Subject: userID, Username: username, Role: role, Expires: expiry.Unix(), CredentialVersion: m.credentialVersion(userID, passwordHash)}
	claims.Federated = identity
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", time.Time{}, err
	}
	claims.Nonce = base64.RawURLEncoding.EncodeToString(nonce)
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", time.Time{}, err
	}
	enc := base64.RawURLEncoding
	body := enc.EncodeToString(payload)
	sig := m.sign(body)
	return body + "." + enc.EncodeToString(sig), expiry, nil
}

// SessionID identifies the signed payload, independent of signature text
// encoding. It can safely be displayed and cannot be used as a credential.
func SessionID(token string) string {
	body, _, _ := strings.Cut(token, ".")
	digest := sha256.Sum256([]byte(body))
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func (m *SessionManager) Verify(token string) (SessionClaims, error) {
	var claims SessionClaims
	if m == nil || len(m.secret) < 16 {
		return claims, errors.New("session manager is not configured")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return claims, errors.New("invalid session token")
	}
	enc := base64.RawURLEncoding
	sig, err := enc.DecodeString(parts[1])
	if err != nil {
		return claims, errors.New("invalid session signature")
	}
	expected := m.sign(parts[0])
	if len(sig) != len(expected) || subtle.ConstantTimeCompare(sig, expected) != 1 {
		return claims, errors.New("invalid session signature")
	}
	payload, err := enc.DecodeString(parts[0])
	if err != nil || json.Unmarshal(payload, &claims) != nil {
		return SessionClaims{}, errors.New("invalid session payload")
	}
	if claims.Subject == "" || claims.Username == "" || claims.Expires <= time.Now().Unix() {
		return SessionClaims{}, errors.New("session expired")
	}
	switch claims.Role {
	case RoleAdmin, RoleDBA, RoleOperator, RoleViewer:
	default:
		return SessionClaims{}, errors.New("invalid session role")
	}
	return claims, nil
}

func (m *SessionManager) sign(body string) []byte {
	mac := hmac.New(sha256.New, m.secret)
	_, _ = mac.Write([]byte(body))
	return mac.Sum(nil)
}

const PersonalTokenPrefix = "dts_pat_"

func (m *SessionManager) PersonalTokenID(raw string) string {
	return base64.RawURLEncoding.EncodeToString(m.sign("dts-personal-token-v1\x00" + raw))
}

func (m *SessionManager) NewPersonalToken() (string, string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	raw := PersonalTokenPrefix + base64.RawURLEncoding.EncodeToString(b)
	return raw, m.PersonalTokenID(raw), nil
}

// Bind sessions to the persisted password hash without publishing a password
// verifier in the readable token. Password resets revoke prior sessions across
// replicas and restarts, without relying on an in-process revocation list.
func (m *SessionManager) credentialVersion(userID, passwordHash string) string {
	return base64.RawURLEncoding.EncodeToString(m.sign("dts-session-credential-v1\x00" + userID + "\x00" + passwordHash))
}

func (m *SessionManager) CredentialsMatch(claims SessionClaims, passwordHash string) bool {
	if m == nil || claims.CredentialVersion == "" || passwordHash == "" {
		return false
	}
	expected := m.credentialVersion(claims.Subject, passwordHash)
	return subtle.ConstantTimeCompare([]byte(claims.CredentialVersion), []byte(expected)) == 1
}
