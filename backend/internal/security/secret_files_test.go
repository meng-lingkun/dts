package security

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSecretFiles(t *testing.T) {
	const key = "DTS_TEST_MOUNTED_SECRET"
	path := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(path, []byte(" significant spaces \r\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(key, "")
	t.Setenv(key+"_FILE", path)
	if err := LoadSecretFiles(key); err != nil || os.Getenv(key) != " significant spaces " {
		t.Fatalf("unexpected secret load result: %v", err)
	}
	if LoadSecretFiles(key) == nil {
		t.Fatal("conflicting sources accepted")
	}
	t.Setenv(key, "")
	if err := os.WriteFile(path, make([]byte, 65537), 0600); err != nil {
		t.Fatal(err)
	}
	if LoadSecretFiles(key) == nil {
		t.Fatal("oversize secret accepted")
	}
}
