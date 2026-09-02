package mysqlconnector

import (
	"bytes"
	"testing"
)

func TestNativePasswordScrambleStable(t *testing.T) {
	got := scrambleNativePassword("secret", []byte("12345678901234567890"))
	if len(got) != 20 {
		t.Fatalf("expected 20 bytes, got %d", len(got))
	}
	if bytes.Equal(got, make([]byte, 20)) {
		t.Fatal("scramble should not be zero")
	}
}
func TestCachingSHA2ScrambleStable(t *testing.T) {
	got := scrambleCachingSHA2("secret", []byte("12345678901234567890"))
	if len(got) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(got))
	}
}
func TestQuoteIdent(t *testing.T) {
	if got := quoteIdent("a`b"); got != "`a``b`" {
		t.Fatalf("got %s", got)
	}
}
func TestLenEnc(t *testing.T) {
	b := []byte{0xfc, 0x34, 0x12}
	v, n, ok := readLenEncInt(b, 0)
	if !ok || v != 0x1234 || n != 3 {
		t.Fatalf("got %d %d %v", v, n, ok)
	}
}
