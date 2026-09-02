package main

import (
	"strings"
	"testing"
)

func setValidProductionEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("QMIGRATION_PRODUCTION", "true")
	t.Setenv("QMIGRATION_REPOSITORY", "postgres")
	t.Setenv("QMIGRATION_AUTH_REQUIRED", "true")
	t.Setenv("QMIGRATION_METADATA_PASSWORD", strings.Repeat("m", 20))
	t.Setenv("QMIGRATION_MASTER_KEY", strings.Repeat("a", 40))
	t.Setenv("QMIGRATION_WORKER_TOKEN", strings.Repeat("b", 40))
	t.Setenv("QMIGRATION_AUTH_SECRET", strings.Repeat("c", 40))
	t.Setenv("QMIGRATION_CORS_ORIGIN", "https://qmigration.internal")
}

func TestValidateProductionEnvironment(t *testing.T) {
	setValidProductionEnvironment(t)
	if err := validateProductionEnvironment(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateProductionEnvironmentRejectsOpenMode(t *testing.T) {
	setValidProductionEnvironment(t)
	t.Setenv("QMIGRATION_AUTH_REQUIRED", "false")
	if err := validateProductionEnvironment(); err == nil {
		t.Fatal("expected open-mode rejection")
	}
}

func TestValidateProductionEnvironmentRejectsPlaceholderAndSharedSecrets(t *testing.T) {
	setValidProductionEnvironment(t)
	t.Setenv("QMIGRATION_MASTER_KEY", "change-me-to-a-long-random-master-key")
	if err := validateProductionEnvironment(); err == nil {
		t.Fatal("expected placeholder rejection")
	}
	setValidProductionEnvironment(t)
	shared := strings.Repeat("z", 40)
	t.Setenv("QMIGRATION_MASTER_KEY", shared)
	t.Setenv("QMIGRATION_WORKER_TOKEN", shared)
	if err := validateProductionEnvironment(); err == nil {
		t.Fatal("expected shared-secret rejection")
	}
}
