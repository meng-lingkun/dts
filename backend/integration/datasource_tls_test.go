//go:build integration

package integration

import (
	"context"
	"crypto/x509"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"dts/backend/internal/connector"
	mysql "dts/backend/internal/connector/mysql"
	postgres "dts/backend/internal/connector/postgres"
	"dts/backend/internal/domain"
)

func TestNativeDatasourceTLS(t *testing.T) {
	requireIsolated(t)
	caPath := os.Getenv("DTS_E2E_TLS_CA_FILE")
	if caPath == "" {
		t.Skip("use the isolated TLS database qualification runner")
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	wrongCA, err := os.ReadFile(os.Getenv("DTS_E2E_TLS_WRONG_CA_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	for _, db := range []struct {
		name    string
		source  domain.DataSource
		factory connector.Factory
	}{
		{"PostgreSQL", pgDS(), postgres.NewFactory()},
		{"MySQL", mysqlDS(), mysql.NewFactory()},
	} {
		t.Run(db.name, func(t *testing.T) {
			// The runner enforces TLS on each server. The ephemeral signer is
			// outside the system trust store and the leaf has only a DNS SAN.
			for _, test := range []struct {
				name, host string
				root       []byte
				mode       domain.TLSMode
			}{
				{"trusted", "db.dts-qa.invalid", ca, domain.TLSModeRequired},
				{"untrusted_CA", "db.dts-qa.invalid", wrongCA, domain.TLSModeRequired},
				{"wrong_hostname", "mismatch.dts-qa.invalid", ca, domain.TLSModeRequired},
				{"plaintext", "", nil, domain.TLSModeDisable},
			} {
				t.Run(test.name, func(t *testing.T) {
					source := db.source
					source.TLSMode, source.TLSServerName, source.TLSCACert = test.mode, test.host, string(test.root)
					c, err := db.factory.New(source)
					if err != nil {
						t.Fatal(err)
					}
					defer c.Close()
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()
					err = c.TestConnection(ctx)
					switch test.name {
					case "trusted":
						if err != nil {
							t.Fatal(err)
						}
						version, err := c.GetVersion(ctx)
						if err != nil || version == "" {
							t.Fatalf("authenticated version query: version=%q err=%v", version, err)
						}
						t.Logf("authenticated query via TLS: %s", version)
					case "untrusted_CA":
						var unknown x509.UnknownAuthorityError
						if !errors.As(err, &unknown) {
							t.Fatalf("expected rejection by certificate authority verification, got %v", err)
						}
					case "wrong_hostname":
						var mismatch x509.HostnameError
						if !errors.As(err, &mismatch) || mismatch.Host != test.host {
							t.Fatalf("expected certificate hostname mismatch for %q, got %v", test.host, err)
						}
					case "plaintext":
						if err == nil || !(strings.Contains(err.Error(), "require_secure_transport") || strings.Contains(err.Error(), "pg_hba.conf rejects connection")) {
							t.Fatalf("expected server to require encrypted transport, got %v", err)
						}
					}
				})
			}
		})
	}
}
