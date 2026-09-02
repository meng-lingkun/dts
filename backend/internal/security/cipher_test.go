package security

import "testing"

func TestCipherRoundTrip(t *testing.T) {
	c, err := New("unit-test-master-key")
	if err != nil {
		t.Fatal(err)
	}
	enc, err := c.Encrypt("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	if enc == "s3cret" || enc == "" {
		t.Fatalf("unexpected ciphertext %q", enc)
	}
	got, err := c.Decrypt(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got != "s3cret" {
		t.Fatalf("got %q", got)
	}
}
