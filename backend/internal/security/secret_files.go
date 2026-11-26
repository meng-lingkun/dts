package security

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// LoadSecretFiles supports read-only projected volumes, including CSI and
// secret-agent mounts. Resolve them before constructing clients or subprocesses.
func LoadSecretFiles(names ...string) error {
	for _, key := range names {
		path := os.Getenv(key + "_FILE")
		if path == "" {
			continue
		}
		if os.Getenv(key) != "" {
			return fmt.Errorf("configure only one of %s and %s_FILE", key, key)
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open %s_FILE: %w", key, err)
		}
		value, err := io.ReadAll(io.LimitReader(file, 65537))
		closeErr := file.Close()
		if err != nil || closeErr != nil || len(value) > 65536 {
			return fmt.Errorf("failed to read %s_FILE (64 KiB maximum)", key)
		}
		text := strings.TrimSuffix(strings.TrimSuffix(string(value), "\n"), "\r")
		if text == "" || strings.ContainsRune(text, 0) {
			return fmt.Errorf("%s_FILE contains an empty or invalid secret", key)
		}
		if err := os.Setenv(key, text); err != nil {
			return fmt.Errorf("set %s: %w", key, err)
		}
	}
	return nil
}
