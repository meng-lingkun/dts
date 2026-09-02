package auth

import "testing"

func TestPasswordHashAndVerify(t *testing.T) {
	h, err := HashPassword("change-me-123")
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyPassword(h, "change-me-123") {
		t.Fatal("password should verify")
	}
	if VerifyPassword(h, "wrong-password") {
		t.Fatal("wrong password verified")
	}
	h2, _ := HashPassword("change-me-123")
	if h == h2 {
		t.Fatal("random salt expected")
	}
}

func TestPasswordMinimumLength(t *testing.T) {
	if _, err := HashPassword("short"); err == nil {
		t.Fatal("expected minimum length error")
	}
}
