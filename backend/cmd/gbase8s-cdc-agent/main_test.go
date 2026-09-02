package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"qmigration/backend/internal/cdc/gbase8scdc"
)

func TestValidateAgentExposure(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:9188", "[::1]:9188", "localhost:9188"} {
		if err := validateAgentExposure(addr, "", "", ""); err != nil {
			t.Fatalf("loopback %s: %v", addr, err)
		}
	}
	if err := validateAgentExposure("0.0.0.0:9188", "", "", ""); err == nil {
		t.Fatal("expected token/TLS requirement")
	}
	if err := validateAgentExposure("10.0.0.2:9188", "token", "", ""); err == nil {
		t.Fatal("expected TLS requirement")
	}
	if err := validateAgentExposure("10.0.0.2:9188", "token", "cert.pem", "key.pem"); err != nil {
		t.Fatal(err)
	}
	if err := validateAgentExposure("127.0.0.1:9188", "", "cert.pem", ""); err == nil {
		t.Fatal("expected cert/key pair requirement")
	}
}

func TestBearerMatches(t *testing.T) {
	if !bearerMatches("Bearer secret", "secret") {
		t.Fatal("valid bearer rejected")
	}
	if bearerMatches("Bearer secrex", "secret") || bearerMatches("secret", "secret") {
		t.Fatal("invalid bearer accepted")
	}
}

func TestProviderConfigFilePermissions(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/provider.json"
	if err := os.WriteFile(path, []byte(`{"server":"gbase8s"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("QMIGRATION_GBASE8S_CDC_PROVIDER_CONFIG_JSON", "")
	t.Setenv("QMIGRATION_GBASE8S_CDC_PROVIDER_CONFIG_FILE", path)
	got, err := providerConfig()
	if err != nil || got != `{"server":"gbase8s"}` {
		t.Fatalf("got=%q err=%v", got, err)
	}
	if err := os.Chmod(path, 0o604); err != nil {
		t.Fatal(err)
	}
	if _, err := providerConfig(); err == nil {
		t.Fatal("expected other-user permission rejection")
	}
}

func TestProviderSelectionIsUnambiguous(t *testing.T) {
	t.Setenv("QMIGRATION_GBASE8S_CDC_PROVIDER_LIBRARY", "/tmp/native.so")
	t.Setenv("QMIGRATION_GBASE8S_CDC_PROVIDER_PLUGIN", "/tmp/legacy.so")
	if _, err := loadProvider(); err == nil {
		t.Fatal("expected ambiguous provider rejection")
	}
}

type endpointAgent struct{}

func (endpointAgent) Health(context.Context) error { return nil }
func (endpointAgent) Checkpoint(context.Context, gbase8scdc.CheckpointRequest) (*gbase8scdc.CheckpointResponse, error) {
	return &gbase8scdc.CheckpointResponse{Sequence: "7"}, nil
}
func (endpointAgent) Read(context.Context, gbase8scdc.ReadRequest) (*gbase8scdc.ReadResponse, error) {
	return &gbase8scdc.ReadResponse{NextSequence: "8", ReadToCurrent: true}, nil
}
func (endpointAgent) ProviderInfo() gbase8scdc.ProviderInfo {
	return gbase8scdc.ProviderInfo{Kind: "native-c-abi", ABIVersion: "4", SHA256Pinned: true}
}

func TestStatusAndMetricsEndpoints(t *testing.T) {
	provider := gbase8scdc.ObserveAgent(gbase8scdc.SerializeAgent(endpointAgent{}))
	srv := httptest.NewServer(newMux(provider, "secret"))
	defer srv.Close()
	for _, path := range []string{"/v1/status", "/metrics"} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s unauth status=%d", path, resp.StatusCode)
		}
		req, _ = http.NewRequest(http.MethodGet, srv.URL+path, nil)
		req.Header.Set("Authorization", "Bearer secret")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, resp.StatusCode, b)
		}
		if path == "/v1/status" && !strings.Contains(string(b), `"api_version":"v4"`) {
			t.Fatalf("status body=%s", b)
		}
		if path == "/metrics" && !strings.Contains(string(b), "qmigration_gbase8s_cdc_agent_up") {
			t.Fatalf("metrics body=%s", b)
		}
	}
}
