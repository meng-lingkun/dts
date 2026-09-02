package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

// Cipher encrypts datasource credentials at rest with AES-256-GCM.
// The key is derived from QMIGRATION_MASTER_KEY using SHA-256 so operators can
// provide a human-manageable secret while the cipher always receives 32 bytes.
type Cipher struct{ aead cipher.AEAD }

func New(masterKey string) (*Cipher, error) {
	if masterKey == "" {
		return nil, errors.New("master key is empty")
	}
	sum := sha256.Sum256([]byte(masterKey))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Cipher{aead: aead}, nil
}

func (c *Cipher) Encrypt(plain string) (string, error) {
	if plain == "" {
		return "", nil
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	sealed := c.aead.Seal(nil, nonce, []byte(plain), []byte("qmigration-datasource-v1"))
	payload := append(nonce, sealed...)
	return "v1:" + base64.RawStdEncoding.EncodeToString(payload), nil
}

func (c *Cipher) Decrypt(encoded string) (string, error) {
	if encoded == "" {
		return "", nil
	}
	if len(encoded) < 3 || encoded[:3] != "v1:" {
		return "", fmt.Errorf("unsupported ciphertext format")
	}
	payload, err := base64.RawStdEncoding.DecodeString(encoded[3:])
	if err != nil {
		return "", err
	}
	n := c.aead.NonceSize()
	if len(payload) < n {
		return "", errors.New("ciphertext too short")
	}
	plain, err := c.aead.Open(nil, payload[:n], payload[n:], []byte("qmigration-datasource-v1"))
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
