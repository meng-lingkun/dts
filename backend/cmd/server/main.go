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
	"qmigration/backend/internal/api"
	"qmigration/backend/internal/auth"
	"qmigration/backend/internal/connector"
	damengconnector "qmigration/backend/internal/connector/dameng"
	db2connector "qmigration/backend/internal/connector/db2"
	gbaseconnector "qmigration/backend/internal/connector/gbase"
	gbase8sconnector "qmigration/backend/internal/connector/gbase8s"
	mysqlconnector "qmigration/backend/internal/connector/mysql"
	oracleconnector "qmigration/backend/internal/connector/oracle"
	postgresconnector "qmigration/backend/internal/connector/postgres"
	sqlserverconnector "qmigration/backend/internal/connector/sqlserver"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/engine"
	"qmigration/backend/internal/maintenance"
	"qmigration/backend/internal/repository"
	"qmigration/backend/internal/repository/memory"
	pgrepo "qmigration/backend/internal/repository/postgres"
	securerepo "qmigration/backend/internal/repository/secure"
	spoolfilerepo "qmigration/backend/internal/repository/spoolfile"
	spools3repo "qmigration/backend/internal/repository/spools3"
	"qmigration/backend/internal/security"
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

func main() {
	repoMode := os.Getenv("QMIGRATION_REPOSITORY")
	var base repository.Repository
	if repoMode == "postgres" {
		port := 5432
		if v := os.Getenv("QMIGRATION_METADATA_PORT"); v != "" {
			if n, e := strconv.Atoi(v); e == nil {
				port = n
			}
		}
		ds := domain.DataSource{Type: domain.DataSourcePostgreSQL, Host: env("QMIGRATION_METADATA_HOST", "127.0.0.1"), Port: port, Username: env("QMIGRATION_METADATA_USER", "qmigration"), Password: os.Getenv("QMIGRATION_METADATA_PASSWORD"), Database: env("QMIGRATION_METADATA_DATABASE", "qmigration")}
		st, err := pgrepo.New(context.Background(), ds, true)
		if err != nil {
			log.Fatalf("open PostgreSQL metadata repository: %v", err)
		}
		base = st
		log.Printf("metadata repository: PostgreSQL %s:%d/%s", ds.Host, ds.Port, ds.Database)
	} else {
		stateFile := os.Getenv("QMIGRATION_STATE_FILE")
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
	spoolStorage := strings.ToLower(strings.TrimSpace(env("QMIGRATION_CDC_SPOOL_STORAGE", "file")))
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
		log.Fatalf("unsupported QMIGRATION_CDC_SPOOL_STORAGE=%q; use file, shared-fs, s3, or metadata", spoolStorage)
	}

	master := os.Getenv("QMIGRATION_MASTER_KEY")
	if master == "" {
		master = "qmigration-development-key-change-me"
		log.Printf("WARNING: QMIGRATION_MASTER_KEY is not set; using development key")
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
	// QMigration exposes exactly one migration engine. Third-party projects are
	// design inspirations only; they are not runtime dependencies or selectable
	// execution engines.
	engines.Register(engine.NewUnified())
	srv := api.New(store, registry, engines)
	addr := os.Getenv("QMIGRATION_ADDR")
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
	cert, key := os.Getenv("QMIGRATION_TLS_CERT"), os.Getenv("QMIGRATION_TLS_KEY")
	if (cert == "") != (key == "") {
		log.Fatal("QMIGRATION_TLS_CERT and QMIGRATION_TLS_KEY must be set together")
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	maintenance.Start(signalCtx, metadataBase, maintenance.ConfigFromEnv())
	serveErr := make(chan error, 1)
	go func() {
		if cert != "" {
			log.Printf("QMigration HTTPS server listening on %s", addr)
			serveErr <- httpServer.ListenAndServeTLS(cert, key)
			return
		}
		log.Printf("QMigration HTTP server listening on %s (production deployments should enable TLS or terminate TLS at a trusted proxy)", addr)
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
	password := os.Getenv("QMIGRATION_BOOTSTRAP_ADMIN_PASSWORD")
	if password == "" {
		return nil
	}
	username := strings.ToLower(strings.TrimSpace(env("QMIGRATION_BOOTSTRAP_ADMIN_USER", "admin")))
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
	if !strings.EqualFold(os.Getenv("QMIGRATION_AUTH_REQUIRED"), "true") {
		return nil
	}
	users, err := repo.ListUsers(ctx)
	if err != nil {
		return err
	}
	spec := os.Getenv("QMIGRATION_RBAC_TOKENS")
	if spec == "" && os.Getenv("QMIGRATION_API_TOKEN") != "" {
		spec = "admin:" + os.Getenv("QMIGRATION_API_TOKEN")
	}
	if len(users) == 0 && auth.ParseTokens(spec).Empty() {
		return fmt.Errorf("QMIGRATION_AUTH_REQUIRED=true but no users or static RBAC tokens exist; set QMIGRATION_BOOTSTRAP_ADMIN_PASSWORD for first startup")
	}
	return nil
}
