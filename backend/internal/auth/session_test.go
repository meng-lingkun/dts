package auth

import (
	"testing"
	"time"
)

func TestSessionIssueVerify(t *testing.T) {
	m := NewSessionManager("this-is-a-long-test-secret-value", time.Hour)
	token, exp, err := m.Issue("usr_1", "admin", RoleAdmin, "stored-password-hash")
	if err != nil {
		t.Fatal(err)
	}
	if exp.Before(time.Now()) {
		t.Fatal("expiry should be in future")
	}
	c, err := m.Verify(token)
	if err != nil {
		t.Fatal(err)
	}
	if c.Subject != "usr_1" || c.Username != "admin" || c.Role != RoleAdmin {
		t.Fatalf("unexpected claims: %+v", c)
	}
	if _, err := m.Verify(token + "x"); err == nil {
		t.Fatal("tampered token should fail")
	}
}

func TestExpiredSession(t *testing.T) {
	m := NewSessionManager("this-is-a-long-test-secret-value", time.Hour)
	m.ttl = time.Nanosecond
	token, _, err := m.Issue("usr_1", "admin", RoleAdmin, "stored-password-hash")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if _, err := m.Verify(token); err == nil {
		t.Fatal("expired token should fail")
	}
}

func TestSessionCredentialBindingSurvivesRestartAndRejectsReset(t *testing.T) {
	m := NewSessionManager("this-is-a-long-test-secret-value", time.Hour)
	token, _, err := m.Issue("usr_1", "admin", RoleAdmin, "old-password-hash")
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewSessionManager("this-is-a-long-test-secret-value", time.Hour)
	claims, err := restarted.Verify(token)
	if err != nil || !restarted.CredentialsMatch(claims, "old-password-hash") {
		t.Fatalf("valid session lost after restart: %v", err)
	}
	if restarted.CredentialsMatch(claims, "new-password-hash") {
		t.Fatal("old session survived a password reset")
	}
	claims.CredentialVersion = ""
	if restarted.CredentialsMatch(claims, "old-password-hash") {
		t.Fatal("legacy tokens must require login again")
	}
}
