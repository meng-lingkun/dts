package main

import (
	"strings"
	"testing"
)

func setValidProductionEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("DTS_PRODUCTION", "true")
	t.Setenv("DTS_REPOSITORY", "postgres")
	t.Setenv("DTS_AUTH_REQUIRED", "true")
	t.Setenv("DTS_METADATA_PASSWORD", strings.Repeat("m", 20))
	t.Setenv("DTS_MASTER_KEY", strings.Repeat("a", 40))
	t.Setenv("DTS_WORKER_TOKEN", strings.Repeat("b", 40))
	t.Setenv("DTS_AUTH_SECRET", strings.Repeat("c", 40))
	t.Setenv("DTS_CORS_ORIGIN", "https://dts.internal")
}

func TestValidateProductionEnvironment(t *testing.T) {
	setValidProductionEnvironment(t)
	if err := validateProductionEnvironment(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateProductionEnvironmentRejectsOpenMode(t *testing.T) {
	setValidProductionEnvironment(t)
	t.Setenv("DTS_AUTH_REQUIRED", "false")
	if err := validateProductionEnvironment(); err == nil {
		t.Fatal("expected open-mode rejection")
	}
}

func TestValidateProductionEnvironmentRejectsPlaceholderAndSharedSecrets(t *testing.T) {
	setValidProductionEnvironment(t)
	t.Setenv("DTS_MASTER_KEY", "change-me-to-a-long-random-master-key")
	if err := validateProductionEnvironment(); err == nil {
		t.Fatal("expected placeholder rejection")
	}
	setValidProductionEnvironment(t)
	shared := strings.Repeat("z", 40)
	t.Setenv("DTS_MASTER_KEY", shared)
	t.Setenv("DTS_WORKER_TOKEN", shared)
	if err := validateProductionEnvironment(); err == nil {
		t.Fatal("expected shared-secret rejection")
	}
}
