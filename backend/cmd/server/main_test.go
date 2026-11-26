package main

import (
	"context"
	"strings"
	"testing"

	"dts/backend/internal/domain"
	"dts/backend/internal/repository/memory"
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

func TestBootstrapAndProductionAuthenticationFlagsAgree(t *testing.T) {
	setValidProductionEnvironment(t)
	t.Setenv("DTS_RBAC_TOKENS", "")
	t.Setenv("DTS_API_TOKEN", "")
	for _, flag := range []string{"1", " YES ", "on", "true"} {
		t.Setenv("DTS_AUTH_REQUIRED", flag)
		if err := validateProductionEnvironment(); err != nil {
			t.Fatal(flag, err)
		}
		if err := validateAuthBootstrap(t.Context(), memory.New()); err == nil {
			t.Fatalf("empty authenticated startup accepted with %q", flag)
		}
	}
	t.Setenv("DTS_AUTH_REQUIRED", "typo")
	if validateProductionEnvironment() == nil {
		t.Fatal("invalid authentication mode accepted")
	}
	t.Setenv("DTS_AUTH_REQUIRED", "true")
	t.Setenv("DTS_PRODUCTION", "typo")
	if validateProductionEnvironment() == nil {
		t.Fatal("invalid production mode accepted")
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

func TestProductionBootstrapRejectsKnownAndPlaceholderPasswords(t *testing.T) {
	t.Setenv("DTS_PRODUCTION", "true")
	for _, password := range []string{"Cljslrl0620!", "change-me-to-a-long-password", "replace-with-an-independent-random-admin-password"} {
		t.Setenv("DTS_BOOTSTRAP_ADMIN_PASSWORD", password)
		base := memory.New()
		if err := bootstrapAdmin(context.Background(), base); err == nil {
			t.Fatal("weak production bootstrap password was accepted")
		}
		users, _ := base.ListUsers(context.Background())
		if len(users) != 0 {
			t.Fatal("rejected bootstrap created an account")
		}
	}
}

func TestProductionBootstrapDoesNotResetAnExistingAdministrator(t *testing.T) {
	t.Setenv("DTS_PRODUCTION", "true")
	t.Setenv("DTS_BOOTSTRAP_ADMIN_USER", "admin")
	t.Setenv("DTS_BOOTSTRAP_ADMIN_PASSWORD", "Cljslrl0620!")
	base := memory.New()
	if err := base.CreateUser(context.Background(), &domain.User{ID: "admin", Username: "admin", PasswordHash: "existing-hash", Role: "admin", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := bootstrapAdmin(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	u, _ := base.GetUser(context.Background(), "admin")
	if u.PasswordHash != "existing-hash" {
		t.Fatal("upgrade reset an existing password")
	}
}

func TestProductionBootstrapAcceptsIndependentPasswordAndRejectsSecretReuse(t *testing.T) {
	t.Setenv("DTS_PRODUCTION", "true")
	t.Setenv("DTS_BOOTSTRAP_ADMIN_USER", "admin")
	password := "generated-admin-credential-987654321"
	t.Setenv("DTS_BOOTSTRAP_ADMIN_PASSWORD", password)
	t.Setenv("DTS_MASTER_KEY", password)
	if err := bootstrapAdmin(context.Background(), memory.New()); err == nil {
		t.Fatal("bootstrap reused the encryption key as a password")
	}
	t.Setenv("DTS_MASTER_KEY", "different-encryption-secret")
	base := memory.New()
	if err := bootstrapAdmin(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	u, err := base.GetUserByUsername(context.Background(), "admin")
	if err != nil || !u.Enabled || u.Role != "admin" {
		t.Fatalf("valid production bootstrap failed: %v", err)
	}
}
