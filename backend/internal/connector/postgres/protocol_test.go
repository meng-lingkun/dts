package postgresconnector

import (
	"encoding/hex"
	"testing"
)

func TestPBKDF2SHA256(t *testing.T) {
	got := hex.EncodeToString(pbkdf2SHA256([]byte("password"), []byte("salt"), 4096, 32))
	want := "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a"
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}
func TestSASLEscape(t *testing.T) {
	if got := saslEscape("a=b,c"); got != "a=3Db=2Cc" {
		t.Fatal(got)
	}
}
