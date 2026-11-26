package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"dts/backend/internal/security"
)

type manifest struct {
	Format                      int
	Bucket, Prefix, DatabaseSHA string
	Metadata                    metadata
	Objects                     []object
}

func digest(f string) (string, error) {
	file, e := regular(f)
	if e != nil {
		return "", e
	}
	defer file.Close()
	h := sha256.New()
	if _, e = io.Copy(h, file); e != nil {
		return "", e
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func regular(name string) (*os.File, error) {
	st, e := os.Lstat(name)
	if e != nil {
		return nil, e
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("expected regular file: %s", name)
	}
	return os.Open(name)
}
func bytesFile(name string) ([]byte, error) {
	f, e := regular(name)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	return io.ReadAll(f)
}
func sign(data []byte, key string) []byte {
	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte("DTS-S3-recovery/v1\x00"))
	h.Write(data)
	return h.Sum(nil)
}
func isDigest(value string) bool {
	raw, e := hex.DecodeString(value)
	return e == nil && len(raw) == sha256.Size && value == strings.ToLower(value)
}
func (m manifest) validate(dir string, c *security.Cipher) error {
	if m.Format != 1 || m.Metadata.Schema == "" || !isDigest(m.DatabaseSHA) || m.Bucket == "" || path.Clean(m.Prefix)+"/" != m.Prefix || strings.HasPrefix(m.Prefix, "..") || strings.HasPrefix(m.Prefix, "/") || m.Prefix == "./" {
		return fmt.Errorf("invalid recovery manifest")
	}
	st, e := os.Lstat(filepath.Join(dir, "objects"))
	if e != nil || !st.IsDir() {
		return fmt.Errorf("missing regular objects directory")
	}
	keys := map[string]object{}
	files := map[string]int64{}
	for _, o := range m.Objects {
		if !validKey(o.Key, m.Prefix, o.Size) || o.Key == m.Prefix+marker || !isDigest(o.SHA) {
			return fmt.Errorf("invalid object descriptor")
		}
		if _, found := keys[o.Key]; found {
			return fmt.Errorf("duplicate object key")
		}
		keys[o.Key] = o
		if size, ok := files[o.SHA]; ok {
			if size != o.Size {
				return fmt.Errorf("object size mismatch")
			}
			continue
		}
		files[o.SHA] = o.Size
		file := filepath.Join(dir, "objects", o.SHA)
		st, e := os.Lstat(file)
		if e != nil || !st.Mode().IsRegular() || st.Size() != o.Size {
			return fmt.Errorf("invalid archived object: %s", o.Key)
		}
		sum, e := digest(file)
		if e != nil || sum != o.SHA {
			return fmt.Errorf("object checksum mismatch: %s", o.Key)
		}
	}
	entries, e := os.ReadDir(filepath.Join(dir, "objects"))
	if e != nil {
		return e
	}
	if len(entries) != len(files) {
		return fmt.Errorf("unexpected object files in recovery set")
	}
	checked := map[string]bool{}
	for _, r := range m.Metadata.Pending {
		var key, sum string
		switch {
		case strings.HasPrefix(r.Value, "spools3:v2:"):
			var ok bool
			sum, key, ok = strings.Cut(strings.TrimPrefix(r.Value, "spools3:v2:"), ":")
			sum = strings.ToLower(sum)
			if !ok || !isDigest(sum) {
				return fmt.Errorf("bad v2 reference: %s", r.ID)
			}
		case strings.HasPrefix(r.Value, "spools3:v1:"):
			key = strings.TrimPrefix(r.Value, "spools3:v1:")
		default:
			return fmt.Errorf("unsupported pending reference: %s", r.ID)
		}
		x, ok := keys[key]
		if !ok || (sum != "" && sum != x.SHA) {
			return fmt.Errorf("missing or mismatched pending object for %s", r.ID)
		}
		if !checked[x.SHA] {
			value, e := bytesFile(filepath.Join(dir, "objects", x.SHA))
			if e != nil {
				return e
			}
			if _, e = c.Decrypt(string(value)); e != nil {
				return fmt.Errorf("master key cannot authenticate pending object for %s", r.ID)
			}
			checked[x.SHA] = true
		}
	}
	sum, e := digest(filepath.Join(dir, "metadata.dump"))
	if e != nil || sum != m.DatabaseSHA {
		return fmt.Errorf("metadata dump checksum mismatch")
	}
	return nil
}
func verify(ctx context.Context, dir, key string, c *security.Cipher) (manifest, error) {
	var m manifest
	raw, e := bytesFile(filepath.Join(dir, "manifest.json"))
	if e != nil {
		return m, e
	}
	ready, e := bytesFile(filepath.Join(dir, "READY"))
	if e != nil {
		return m, fmt.Errorf("recovery set incomplete: %w", e)
	}
	sig, e := hex.DecodeString(strings.TrimSpace(string(ready)))
	if e != nil || !hmac.Equal(sig, sign(raw, key)) {
		return m, fmt.Errorf("recovery manifest authentication failed (master key or manifest differs)")
	}
	if e = json.Unmarshal(raw, &m); e != nil {
		return m, e
	}
	if e = m.validate(dir, c); e != nil {
		return m, e
	}
	return m, pg(ctx, "pg_restore", "--list", filepath.Join(dir, "metadata.dump"))
}
