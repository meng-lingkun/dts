package auth

import (
	"encoding/hex"
	"testing"
)

func TestPBKDF2SHA256Compatibility(t *testing.T) {
	// Known PBKDF2-HMAC-SHA256 output with two rounds exercises HMAC reuse.
	got := pbkdf2SHA256([]byte("password"), []byte("salt"), 2, 32)
	if hex.EncodeToString(got) != "ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43" {
		t.Fatal("existing password verifier format changed")
	}
}

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
