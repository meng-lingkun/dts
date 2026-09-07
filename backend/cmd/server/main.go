package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"dts/backend/internal/api"
	"dts/backend/internal/auth"
	"dts/backend/internal/connector"
	damengconnector "dts/backend/internal/connector/dameng"
	db2connector "dts/backend/internal/connector/db2"
	gbaseconnector "dts/backend/internal/connector/gbase"
	gbase8sconnector "dts/backend/internal/connector/gbase8s"
	mysqlconnector "dts/backend/internal/connector/mysql"
	oracleconnector "dts/backend/internal/connector/oracle"
	postgresconnector "dts/backend/internal/connector/postgres"
	sqlserverconnector "dts/backend/internal/connector/sqlserver"
	"dts/backend/internal/domain"
	"dts/backend/internal/engine"
	"dts/backend/internal/maintenance"
	"dts/backend/internal/repository"
	"dts/backend/internal/repository/memory"
	pgrepo "dts/backend/internal/repository/postgres"
	securerepo "dts/backend/internal/repository/secure"
	spoolfilerepo "dts/backend/internal/repository/spoolfile"
	spools3repo "dts/backend/internal/repository/spools3"
	"dts/backend/internal/security"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envEnabled(k string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(k))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func validateProductionEnvironment() error {
	if !envEnabled("DTS_PRODUCTION") {
		return nil
	}
	if os.Getenv("DTS_REPOSITORY") != "postgres" {
		return errors.New("DTS_PRODUCTION requires DTS_REPOSITORY=postgres")
	}
	if !envEnabled("DTS_AUTH_REQUIRED") {
		return errors.New("DTS_PRODUCTION requires DTS_AUTH_REQUIRED=true")
	}
	required := map[string]int{
		"DTS_METADATA_PASSWORD": 16,
		"DTS_MASTER_KEY":        32,
		"DTS_WORKER_TOKEN":      32,
		"DTS_AUTH_SECRET":       32,
	}
	values := make(map[string]string, len(required))
	for name, minLength := range required {
		value := strings.TrimSpace(os.Getenv(name))
		lower := strings.ToLower(value)
		if len(value) < minLength || strings.Contains(lower, "change-me") || strings.Contains(lower, "change-this") || strings.Contains(lower, "example") {
			return fmt.Errorf("DTS_PRODUCTION requires a non-placeholder %s with at least %d characters", name, minLength)
		}
		values[name] = value
	}
	secretNames := []string{"DTS_MASTER_KEY", "DTS_WORKER_TOKEN", "DTS_AUTH_SECRET"}
	for i := range secretNames {
		for j := i + 1; j < len(secretNames); j++ {
			if values[secretNames[i]] == values[secretNames[j]] {
				return fmt.Errorf("DTS_PRODUCTION requires distinct values for %s and %s", secretNames[i], secretNames[j])
			}
		}
	}
	origin := strings.TrimSpace(os.Getenv("DTS_CORS_ORIGIN"))
	if origin == "" || origin == "*" {
		return errors.New("DTS_PRODUCTION requires an explicit DTS_CORS_ORIGIN")
	}
	return nil
}

func main() {
	if err := validateProductionEnvironment(); err != nil {
		log.Fatal(err)
	}
	repoMode := os.Getenv("DTS_REPOSITORY")
	var base repository.Repository
	if repoMode == "postgres" {
		port := 5432
		if v := os.Getenv("DTS_METADATA_PORT"); v != "" {
			if n, e := strconv.Atoi(v); e == nil {
				port = n
			}
		}
		ds := domain.DataSource{Type: domain.DataSourcePostgreSQL, Host: env("DTS_METADATA_HOST", "127.0.0.1"), Port: port, Username: env("DTS_METADATA_USER", "dts"), Password: os.Getenv("DTS_METADATA_PASSWORD"), Database: env("DTS_METADATA_DATABASE", "dts")}
		st, err := pgrepo.New(context.Background(), ds, true)
		if err != nil {
			log.Fatalf("open PostgreSQL metadata repository: %v", err)
		}
		base = st
		log.Printf("metadata repository: PostgreSQL %s:%d/%s", ds.Host, ds.Port, ds.Database)
	} else {
		stateFile := os.Getenv("DTS_STATE_FILE")
		if stateFile == ":memory:" {
			base = memory.New()
			log.Printf("metadata repository: memory")
		} else {
			if stateFile == "" {
				stateFile = filepath.Join("data", "state.json")
			}
			st, err := memory.NewPersistent(stateFile)
			if err != nil {
				log.Fatalf("open metadata repository: %v", err)
			}
			base = st
			log.Printf("metadata repository: %s", stateFile)
		}
	}
	metadataBase := base
	spoolStorage := strings.ToLower(strings.TrimSpace(env("DTS_CDC_SPOOL_STORAGE", "file")))
	switch spoolStorage {
	case "file", "shared-fs":
		spoolStore, err := spoolfilerepo.New(base, spoolfilerepo.ConfigFromEnv())
		if err != nil {
			log.Fatalf("open CDC spool file store: %v", err)
		}
		if err := spoolStore.Reconcile(context.Background()); err != nil {
			log.Fatalf("reconcile CDC spool file store: %v", err)
		}
		base = spoolStore
		log.Printf("CDC spool storage: %s root=%s", spoolStorage, spoolStore.Root())
	case "s3":
		spoolStore, err := spools3repo.New(base, spools3repo.ConfigFromEnv())
		if err != nil {
			log.Fatalf("open CDC spool S3 store: %v", err)
		}
		if err := spoolStore.CDCSpoolStorageReady(context.Background()); err != nil {
			log.Fatalf("check CDC spool S3 store: %v", err)
		}
		if err := spoolStore.Reconcile(context.Background()); err != nil {
			log.Fatalf("reconcile CDC spool S3 store: %v", err)
		}
		base = spoolStore
		log.Printf("CDC spool storage: s3-compatible")
	case "metadata":
		log.Printf("CDC spool storage: metadata (not recommended for large snapshots)")
	default:
		log.Fatalf("unsupported DTS_CDC_SPOOL_STORAGE=%q; use file, shared-fs, s3, or metadata", spoolStorage)
	}

	master := os.Getenv("DTS_MASTER_KEY")
	if master == "" {
		master = "dts-development-key-change-me"
		log.Printf("WARNING: DTS_MASTER_KEY is not set; using development key")
	}
	cipher, err := security.New(master)
	if err != nil {
		log.Fatal(err)
	}
	if closer, ok := base.(interface{ Close() error }); ok {
		defer func() { _ = closer.Close() }()
	}
	store := securerepo.New(base, cipher)
	if err := bootstrapAdmin(context.Background(), store); err != nil {
		log.Fatalf("bootstrap admin: %v", err)
	}
	if err := validateAuthBootstrap(context.Background(), store); err != nil {
		log.Fatal(err)
	}

	registry := connector.NewRegistry()
	mf := mysqlconnector.NewFactory()
	for _, t := range []domain.DataSourceType{domain.DataSourceMySQL, domain.DataSourceMariaDB, domain.DataSourcePolarDBX, domain.DataSourceTiDB, domain.DataSourceOceanBase, domain.DataSourcePolarDBMySQL} {
		registry.Register(t, mf)
	}
	pf := postgresconnector.NewFactory()
	for _, t := range []domain.DataSourceType{domain.DataSourcePostgreSQL, domain.DataSourcePolarDBPostgreSQL, domain.DataSourceOpenGauss, domain.DataSourceKingbase, domain.DataSourceGaussDB} {
		registry.Register(t, pf)
	}
	registry.Register(domain.DataSourceOracle, oracleconnector.NewFactory())
	registry.Register(domain.DataSourceSQLServer, sqlserverconnector.NewFactory())
	registry.Register(domain.DataSourceDB2, db2connector.NewFactory())
	registry.Register(domain.DataSourceDameng, damengconnector.NewFactory())
	registry.Register(domain.DataSourceGBase, gbaseconnector.NewFactory())
	registry.Register(domain.DataSourceGBase8s, gbase8sconnector.NewFactory())
	engines := engine.NewRegistry()
	// DTS exposes exactly one migration engine. Third-party projects are
	// design inspirations only; they are not runtime dependencies or selectable
	// execution engines.
	engines.Register(engine.NewUnified())
	srv := api.New(store, registry, engines)
	addr := os.Getenv("DTS_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       120 * time.Second,
	}
	cert, key := os.Getenv("DTS_TLS_CERT"), os.Getenv("DTS_TLS_KEY")
	if (cert == "") != (key == "") {
		log.Fatal("DTS_TLS_CERT and DTS_TLS_KEY must be set together")
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go srv.StartRecoveryLoop(signalCtx)
	maintenance.Start(signalCtx, metadataBase, maintenance.ConfigFromEnv())
	serveErr := make(chan error, 1)
	go func() {
		if cert != "" {
			log.Printf("DTS HTTPS server listening on %s", addr)
			serveErr <- httpServer.ListenAndServeTLS(cert, key)
			return
		}
		log.Printf("DTS HTTP server listening on %s (production deployments should enable TLS or terminate TLS at a trusted proxy)", addr)
		serveErr <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-signalCtx.Done():
		log.Printf("shutdown signal received; draining HTTP requests")
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("graceful HTTP shutdown failed: %v", err)
			_ = httpServer.Close()
		}
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server stopped with error: %v", err)
		}
	}
}

func bootstrapAdmin(ctx context.Context, repo repository.Repository) error {
	password := os.Getenv("DTS_BOOTSTRAP_ADMIN_PASSWORD")
	if password == "" {
		return nil
	}
	username := strings.ToLower(strings.TrimSpace(env("DTS_BOOTSTRAP_ADMIN_USER", "admin")))
	if _, err := repo.GetUserByUsername(ctx, username); err == nil {
		log.Printf("bootstrap admin %q already exists; password was not reset", username)
		return nil
	}
	hash, err := auth.HashPassword(password)
	if err != nil {
		return err
	}
	idBytes := make([]byte, 6)
	if _, err := rand.Read(idBytes); err != nil {
		return err
	}
	now := time.Now().UTC()
	u := &domain.User{ID: "usr_" + hex.EncodeToString(idBytes), Username: username, PasswordHash: hash, Role: string(auth.RoleAdmin), Enabled: true, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateUser(ctx, u); err != nil {
		return err
	}
	log.Printf("bootstrap admin %q created", username)
	return nil
}

func validateAuthBootstrap(ctx context.Context, repo repository.Repository) error {
	if !strings.EqualFold(os.Getenv("DTS_AUTH_REQUIRED"), "true") {
		return nil
	}
	users, err := repo.ListUsers(ctx)
	if err != nil {
		return err
	}
	spec := os.Getenv("DTS_RBAC_TOKENS")
	if spec == "" && os.Getenv("DTS_API_TOKEN") != "" {
		spec = "admin:" + os.Getenv("DTS_API_TOKEN")
	}
	if len(users) == 0 && auth.ParseTokens(spec).Empty() {
		return fmt.Errorf("DTS_AUTH_REQUIRED=true but no users or static RBAC tokens exist; set DTS_BOOTSTRAP_ADMIN_PASSWORD for first startup")
	}
	return nil
}
