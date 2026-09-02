package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type SessionClaims struct {
	Subject  string `json:"sub"`
	Username string `json:"username"`
	Role     Role   `json:"role"`
	Expires  int64  `json:"exp"`
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

func (m *SessionManager) Issue(userID, username string, role Role) (string, time.Time, error) {
	if m == nil || len(m.secret) < 16 {
		return "", time.Time{}, errors.New("session secret must be at least 16 bytes")
	}
	expiry := time.Now().Add(m.ttl).UTC()
	claims := SessionClaims{Subject: userID, Username: username, Role: role, Expires: expiry.Unix()}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", time.Time{}, err
	}
	enc := base64.RawURLEncoding
	body := enc.EncodeToString(payload)
	sig := m.sign(body)
	return body + "." + enc.EncodeToString(sig), expiry, nil
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
