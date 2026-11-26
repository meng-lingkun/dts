package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"syscall"

	"dts/backend/internal/security"
	"github.com/jackc/pgx/v5"
)

func defaultTo(v, d string) string {
	if v == "" {
		return d
	}
	return v
}
func backup(ctx context.Context, dir, key string, c *security.Cipher, db *pgx.Conn, s *store) error {
	if e := os.Mkdir(dir, 0700); e != nil {
		return e
	}
	objectsDir := filepath.Join(dir, "objects")
	if e := os.Mkdir(objectsDir, 0700); e != nil {
		return e
	}
	tx, e := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	_, e = tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; DO $$DECLARE r record; BEGIN FOR r IN SELECT c.oid::regclass obj FROM pg_class c WHERE c.relnamespace='public'::regnamespace AND c.relkind IN ('r','p') ORDER BY c.oid LOOP EXECUTE format('LOCK TABLE %s IN SHARE MODE',r.obj); END LOOP; END$$`)
	if e != nil {
		return e
	}
	var snapshot string
	if e = tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot); e != nil {
		return e
	}
	m := manifest{Format: 1, Bucket: s.bucket, Prefix: s.prefix}
	if m.Metadata, e = readMetadata(ctx, tx, c); e != nil {
		return e
	}
	if e = s.inactive(ctx); e != nil {
		return e
	}
	before, e := s.list(ctx)
	if e != nil {
		return e
	}
	dump := filepath.Join(dir, "metadata.dump")
	if e = pg(ctx, "pg_dump", "-w", "--format=custom", "--no-owner", "--no-acl", "--snapshot="+snapshot, "--file="+dump); e != nil {
		return e
	}
	if m.DatabaseSHA, e = digest(dump); e != nil {
		return e
	}
	for _, x := range before {
		if x.Key == s.prefix+marker {
			return fmt.Errorf("source is quarantined by an incomplete restore")
		}
		tmp, e := os.CreateTemp(objectsDir, ".copy-")
		if e != nil {
			return e
		}
		x.SHA, e = s.copyOut(ctx, x, tmp)
		closeErr := tmp.Close()
		if e != nil {
			return e
		}
		if closeErr != nil {
			return closeErr
		}
		dest := filepath.Join(objectsDir, x.SHA)
		if _, e = os.Lstat(dest); errors.Is(e, os.ErrNotExist) {
			e = os.Rename(tmp.Name(), dest)
		} else if e == nil {
			e = os.Remove(tmp.Name())
		}
		if e != nil {
			return e
		}
		m.Objects = append(m.Objects, x)
	}
	if e = m.validate(dir, c); e != nil {
		return e
	}
	if e = pg(ctx, "pg_restore", "--list", dump); e != nil {
		return e
	}
	after, e := s.list(ctx)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(before, after) {
		return fmt.Errorf("S3 keys or versions changed during backup")
	}
	if e = s.inactive(ctx); e != nil {
		return e
	}
	if e = idle(ctx, tx); e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	if e = idle(ctx, db); e != nil {
		return e
	}
	raw, e := json.MarshalIndent(m, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0600); e != nil {
		return e
	}
	return os.WriteFile(filepath.Join(dir, "READY"), []byte(hex.EncodeToString(sign(raw, key))+"\n"), 0600)
}
func checkTarget(ctx context.Context, s *store, m manifest, markerETag string) error {
	actual, e := s.list(ctx)
	if e != nil {
		return e
	}
	expect := map[string]object{}
	for _, o := range m.Objects {
		expect[o.Key] = o
	}
	if len(actual) != len(expect)+1 {
		return fmt.Errorf("destination membership changed")
	}
	foundMarker := false
	for _, x := range actual {
		if x.Key == s.prefix+marker && x.ETag == markerETag {
			foundMarker = true
			continue
		}
		o, ok := expect[x.Key]
		if !ok || o.Size != x.Size {
			return fmt.Errorf("unexpected destination object: %s", x.Key)
		}
		sum, e := s.copyOut(ctx, x, io.Discard)
		if e != nil {
			return e
		}
		if sum != o.SHA {
			return fmt.Errorf("destination content differs: %s", x.Key)
		}
	}
	if !foundMarker {
		return fmt.Errorf("lost recovery marker")
	}
	return s.inactive(ctx)
}
func restore(ctx context.Context, dir string, c *security.Cipher, db *pgx.Conn, s *store, m manifest) error {
	if m.Prefix != s.prefix {
		return fmt.Errorf("destination must retain original spool prefix %s", m.Prefix)
	}
	if e := emptyDB(ctx, db); e != nil {
		return e
	}
	if e := s.virgin(ctx); e != nil {
		return e
	}
	tag, e := s.reserve(ctx)
	if e != nil {
		return e
	}
	if tag == "" {
		return fmt.Errorf("missing recovery marker ETag")
	}
	for _, x := range m.Objects {
		f, e := regular(filepath.Join(dir, "objects", x.SHA))
		if e != nil {
			return e
		}
		e = s.put(ctx, x, f)
		f.Close()
		if e != nil {
			return e
		}
	}
	if e = checkTarget(ctx, s, m, tag); e != nil {
		return e
	}
	if e = idle(ctx, db); e != nil {
		return e
	}
	if e = emptyDB(ctx, db); e != nil {
		return e
	}
	dump := filepath.Join(dir, "metadata.dump")
	if sum, err := digest(dump); err != nil || sum != m.DatabaseSHA {
		return fmt.Errorf("metadata dump changed before restore")
	}
	if e = pg(ctx, "pg_restore", "-w", "--exit-on-error", "--single-transaction", "--no-owner", "--no-acl", "-d", os.Getenv("PGDATABASE"), dump); e != nil {
		return e
	}
	restored, e := readMetadata(ctx, db, c)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(m.Metadata, restored) {
		return fmt.Errorf("restored schema or pending metadata reference differs")
	}
	if e = checkTarget(ctx, s, m, tag); e != nil {
		return e
	}
	if e = idle(ctx, db); e != nil {
		return e
	}
	return s.finish(ctx, tag)
}
func run(ctx context.Context, args []string) error {
	if len(args) != 2 || (args[0] != "backup" && args[0] != "restore" && args[0] != "verify") {
		return fmt.Errorf("usage: dts-s3-recovery backup|verify|restore RECOVERY_DIRECTORY")
	}
	op := args[0]
	dir, e := filepath.Abs(args[1])
	if e != nil {
		return e
	}
	key := os.Getenv("DTS_MASTER_KEY")
	c, e := security.New(key)
	if e != nil {
		return e
	}
	var m manifest
	if op == "verify" || op == "restore" {
		m, e = verify(ctx, dir, key, c)
		if e != nil || op == "verify" {
			return e
		}
	}
	if os.Getenv("DTS_WRITERS_STOPPED") != "YES" {
		return fmt.Errorf("quiesce DTS, exclude all external writers and set DTS_WRITERS_STOPPED=YES")
	}
	if op == "restore" && os.Getenv("DTS_RESTORE_CONFIRM") != "YES" {
		return fmt.Errorf("set DTS_RESTORE_CONFIRM=YES for a new isolated recovery target")
	}
	db, e := database(ctx)
	if e != nil {
		return e
	}
	defer db.Close(ctx)
	s, e := configuredStore()
	if e != nil {
		return e
	}
	if op == "backup" {
		return backup(ctx, dir, key, c, db, s)
	}
	return restore(ctx, dir, c, db, s, m)
}
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx, os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	fmt.Printf("%s complete\n", os.Args[1])
}
