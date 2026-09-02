package auth

import (
	"testing"
	"time"
)

func TestSessionIssueVerify(t *testing.T) {
	m := NewSessionManager("this-is-a-long-test-secret-value", time.Hour)
	token, exp, err := m.Issue("usr_1", "admin", RoleAdmin)
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
	token, _, err := m.Issue("usr_1", "admin", RoleAdmin)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if _, err := m.Verify(token); err == nil {
		t.Fatal("expired token should fail")
	}
}
