package main

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateProducesGoCompatiblePKI(t *testing.T) {
	directory := t.TempDir()
	if err := generate(directory, "10.10.156.22", time.Unix(1_800_000_000, 0)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ca.crt", "server.crt", "client/client.crt"} {
		contents, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			t.Fatal(err)
		}
		block, rest := pem.Decode(contents)
		if block == nil || block.Type != "CERTIFICATE" || len(rest) != 0 {
			t.Fatalf("%s is not one canonical certificate PEM block", name)
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		if certificate.SignatureAlgorithm != x509.SHA256WithRSA {
			t.Fatalf("%s signature algorithm is %s", name, certificate.SignatureAlgorithm)
		}
	}
	endpoint, err := os.ReadFile(filepath.Join(directory, "client/endpoint"))
	if err != nil || string(endpoint) != "10.10.156.22:5000\n" {
		t.Fatalf("unexpected endpoint %q (%v)", endpoint, err)
	}
	secret, err := os.ReadFile(filepath.Join(directory, "http.secret"))
	if err != nil || len(secret) != 65 {
		t.Fatalf("unexpected HTTP secret length %d (%v)", len(secret), err)
	}
}

func TestGenerateRejectsInvalidAddress(t *testing.T) {
	if err := generate(t.TempDir(), "registry.example.com", time.Now()); err == nil {
		t.Fatal("expected invalid IPv4 address to fail")
	}
}
