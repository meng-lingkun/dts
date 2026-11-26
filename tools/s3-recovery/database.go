package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"dts/backend/internal/security"
	"github.com/jackc/pgx/v5"
)

type reference struct{ ID, Value string }
type metadata struct {
	Schema  string
	Pending []reference
}
type reader interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func database(ctx context.Context) (*pgx.Conn, error) {
	for _, p := range [][3]string{
		{"HOST", "PGHOST", "127.0.0.1"},
		{"PORT", "PGPORT", "5432"},
		{"DATABASE", "PGDATABASE", ""},
		{"USER", "PGUSER", "dts"},
		{"PASSWORD", "PGPASSWORD", ""},
	} {
		v := defaultTo(os.Getenv("DTS_METADATA_"+p[0]), p[2])
		if v == "" {
			return nil, fmt.Errorf("DTS_METADATA_%s is required", p[0])
		}
		if p[0] == "DATABASE" && (strings.Contains(v, "=") || strings.Contains(v, "://") || v == "postgres" || v == "template1" || v == "template0") {
			return nil, fmt.Errorf("select a dedicated metadata database by name")
		}
		if e := os.Setenv(p[1], v); e != nil {
			return nil, e
		}
	}
	db, e := pgx.Connect(ctx, "")
	if e != nil {
		return nil, e
	}
	var locked bool
	e = db.QueryRow(ctx, "SELECT pg_try_advisory_lock(112393504,58342)").Scan(&locked)
	if e != nil || !locked {
		db.Close(ctx)
		return nil, fmt.Errorf("could not acquire exclusive DTS recovery session lock")
	}
	if e = idle(ctx, db); e != nil {
		db.Close(ctx)
		return nil, e
	}
	return db, nil
}
func idle(ctx context.Context, db reader) error {
	var clients int
	e := db.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND pid<>pg_backend_pid()").Scan(&clients)
	if e != nil {
		return e
	}
	if clients != 0 {
		return fmt.Errorf("dedicated metadata database has %d other clients", clients)
	}
	return nil
}
func emptyDB(ctx context.Context, db reader) error {
	const q = `SELECT EXISTS (
      SELECT FROM pg_namespace WHERE nspname NOT IN ('pg_catalog','public','information_schema') AND nspname !~ '^pg_(toast|temp)'
      UNION ALL SELECT FROM pg_depend d WHERE d.refclassid='pg_namespace'::regclass AND d.refobjid='public'::regnamespace
      UNION ALL SELECT FROM pg_extension WHERE extname <> 'plpgsql'
      UNION ALL SELECT FROM pg_largeobject_metadata
      UNION ALL SELECT FROM pg_publication
      UNION ALL SELECT FROM pg_foreign_data_wrapper
      UNION ALL SELECT FROM pg_foreign_server
      UNION ALL SELECT FROM pg_subscription WHERE subdbid=(SELECT oid FROM pg_database WHERE datname=current_database())
      UNION ALL SELECT FROM pg_language WHERE lanname NOT IN ('internal','c','sql','plpgsql')
      UNION ALL SELECT FROM pg_event_trigger)`
	var objects bool
	if e := db.QueryRow(ctx, q).Scan(&objects); e != nil {
		return e
	}
	if objects {
		return fmt.Errorf("restore requires an empty dedicated database")
	}
	return nil
}
func readMetadata(ctx context.Context, r reader, cipher *security.Cipher) (metadata, error) {
	m := metadata{Pending: []reference{}}
	if e := r.QueryRow(ctx, "SELECT schema_version FROM public.metadata_schema_state WHERE id=1").Scan(&m.Schema); e != nil || m.Schema == "" {
		return m, fmt.Errorf("missing DTS metadata schema marker: %v", e)
	}
	// Authenticate stored credentials, keys, dead letters and inline spool without outputting plaintext.
	rows, e := r.Query(ctx, `SELECT password_ciphertext FROM public.datasources WHERE password_ciphertext<>''
        UNION ALL SELECT tls_client_key_ciphertext FROM public.datasources WHERE tls_client_key_ciphertext<>''
        UNION ALL SELECT events_ciphertext FROM public.cdc_dead_letters WHERE events_ciphertext<>''
        UNION ALL SELECT events_ciphertext FROM public.cdc_spool WHERE events_ciphertext LIKE 'v1:%'`)
	if e != nil {
		return m, e
	}
	for rows.Next() {
		var v string
		if e = rows.Scan(&v); e != nil {
			rows.Close()
			return m, e
		}
		if _, e = cipher.Decrypt(v); e != nil {
			rows.Close()
			return m, fmt.Errorf("master key cannot authenticate stored ciphertext")
		}
	}
	if e = rows.Err(); e != nil {
		return m, e
	}
	rows, e = r.Query(ctx, `SELECT id, events_ciphertext FROM public.cdc_spool WHERE status<>'APPLIED' AND coalesce(events_ciphertext,'')<>'' AND events_ciphertext NOT LIKE 'v1:%' ORDER BY id COLLATE "C"`)
	if e != nil {
		return m, e
	}
	defer rows.Close()
	for rows.Next() {
		var ref reference
		if e = rows.Scan(&ref.ID, &ref.Value); e != nil {
			return m, e
		}
		if !strings.HasPrefix(ref.Value, "spools3:") {
			return m, fmt.Errorf("unsupported external spool reference for %s", ref.ID)
		}
		m.Pending = append(m.Pending, ref)
	}
	return m, rows.Err()
}
func pg(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stderr = os.Stderr
	if e := cmd.Run(); e != nil {
		return fmt.Errorf("%s: %w", name, e)
	}
	return nil
}
