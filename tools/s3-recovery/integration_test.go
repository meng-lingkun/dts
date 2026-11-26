//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dts/backend/internal/security"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgx/v5"
)

type httpFunc func(*http.Request) (*http.Response, error)

func (f httpFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }
func intercept(s *store, fn func(*http.Request)) *store {
	cfg := s.client.Options()
	httpClient := cfg.HTTPClient
	cfg.HTTPClient = httpFunc(func(r *http.Request) (*http.Response, error) { fn(r); return httpClient.Do(r) })
	copy := *s
	copy.client = s3.New(cfg)
	return &copy
}
func TestIsolatedRecovery(t *testing.T) {
	if os.Getenv("DTS_S3_DR_INTEGRATION") != "LOCAL_ONLY" {
		t.Skip("requires isolated PostgreSQL and SeaweedFS on loopback")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	const master = "integration-only-master-key-on-isolated-test"
	env := map[string]string{"DTS_CDC_SPOOL_S3_ENDPOINT": "http://127.0.0.1:40473", "DTS_CDC_SPOOL_S3_ACCESS_KEY": "test-spool", "DTS_CDC_SPOOL_S3_SECRET_KEY": "test-only-secret", "DTS_CDC_SPOOL_S3_PREFIX": "dts/spool", "DTS_MASTER_KEY": master, "DTS_WRITERS_STOPPED": "YES", "DTS_RESTORE_CONFIRM": "YES", "DTS_METADATA_HOST": "127.0.0.1", "DTS_METADATA_PORT": "45983", "DTS_METADATA_USER": "postgres", "DTS_METADATA_PASSWORD": "test-only", "PGSSLMODE": "disable"}
	for k, v := range env {
		t.Setenv(k, v)
	}
	check := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	refuse := func(e error, s string) {
		t.Helper()
		if e == nil || !strings.Contains(e.Error(), s) {
			t.Fatalf("expected %q, got %v", s, e)
		}
	}
	admin, e := pgx.Connect(ctx, "postgres://postgres@127.0.0.1:45983/postgres?sslmode=disable")
	check(e)
	defer admin.Close(ctx)
	id := fmt.Sprintf("dr%d", time.Now().UnixNano())
	setupDB := func(name string) string {
		dbName := id + name
		_, e := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()+" TEMPLATE template0")
		check(e)
		return dbName
	}
	open := func(name string) *pgx.Conn {
		t.Setenv("DTS_METADATA_DATABASE", name)
		db, e := database(ctx)
		check(e)
		return db
	}
	srcDB := setupDB("src")
	destDB := setupDB("dest")
	storeFor := func(name string) *store {
		t.Setenv("DTS_CDC_SPOOL_S3_BUCKET", id+"-"+name)
		s, e := configuredStore()
		check(e)
		s.page = 2
		_, e = s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &s.bucket})
		check(e)
		return s
	}
	source := storeFor("source")
	dest := storeFor("dest")
	put := func(s *store, suffix string, data []byte) {
		k := s.prefix + suffix
		_, e := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &k, Body: bytes.NewReader(data)})
		check(e)
	}
	c, e := security.New(master)
	check(e)
	src := open(srcDB)
	schema, e := os.ReadFile("../../backend/internal/repository/postgres/schema.sql")
	check(e)
	_, e = src.Exec(ctx, string(schema))
	check(e)
	insert := func(id, status, value string) {
		_, e := src.Exec(ctx, "INSERT INTO public.cdc_spool(id,task_id,direction,status,created_at,events_ciphertext) VALUES($1,'t','forward',$2,now(),$3)", id, status, value)
		check(e)
	}
	cipher, e := c.Encrypt(`[{"id":"event"}]`)
	check(e)
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(cipher)))
	put(source, "pending/one", []byte(cipher))
	put(source, "pending/two", []byte(cipher))
	put(source, "applied/kept", []byte("retained"))
	put(source, "orphan", []byte("orphan"))
	put(source, "large", bytes.Repeat([]byte("X"), 17<<20))
	insert("one", "PENDING", "spools3:v2:"+hash+":"+source.prefix+"pending/one")
	insert("two", "PENDING", "spools3:v1:"+source.prefix+"pending/two")
	insert("inline", "PENDING", cipher)
	insert("applied", "APPLIED", "spools3:v1:"+source.prefix+"deleted-old-pending")
	dir := filepath.Join(t.TempDir(), "set")
	check(backup(ctx, dir, master, c, src, source))
	m, e := verify(ctx, dir, master, c)
	check(e)
	if len(m.Objects) != 5 || len(m.Metadata.Pending) != 2 {
		t.Fatal("multi-page metadata/object inventory mismatch")
	}
	t.Log("backup and verify five objects, two-item pages, v1 and v2 pending references, dedup, inline and APPLIED")
	t.Run("wrong-key-incomplete-and-damaged", func(t *testing.T) {
		_, err := verify(ctx, dir, "different", c)
		refuse(err, "authentication failed")
		ready := filepath.Join(dir, "READY")
		saved := ready + ".saved"
		check(os.Rename(ready, saved))
		_, err = verify(ctx, dir, master, c)
		refuse(err, "incomplete")
		check(os.Rename(saved, ready))
		file := filepath.Join(dir, "objects", hash)
		b, e := os.ReadFile(file)
		check(e)
		b[0] ^= 1
		check(os.WriteFile(file, b, 0600))
		_, err = verify(ctx, dir, master, c)
		refuse(err, "checksum mismatch")
		b[0] ^= 1
		check(os.WriteFile(file, b, 0600))
		dump := filepath.Join(dir, "metadata.dump")
		dbBytes, e := os.ReadFile(dump)
		check(e)
		dbBytes[len(dbBytes)-1] ^= 1
		check(os.WriteFile(dump, dbBytes, 0600))
		_, err = verify(ctx, dir, master, c)
		refuse(err, "dump checksum mismatch")
		dbBytes[len(dbBytes)-1] ^= 1
		check(os.WriteFile(dump, dbBytes, 0600))
		wrong, _ := security.New("wrong")
		refuse(backup(ctx, filepath.Join(t.TempDir(), "wrong"), "wrong", wrong, src, source), "master key")
	})
	t.Run("pending-reference-and-source-mutation", func(t *testing.T) {
		insert("missing", "PENDING", "spools3:v1:"+source.prefix+"missing")
		refuse(backup(ctx, filepath.Join(t.TempDir(), "missing"), master, c, src, source), "missing or mismatched")
		_, e := src.Exec(ctx, "DELETE FROM public.cdc_spool WHERE id='missing'")
		check(e)
		changed := false
		s := intercept(source, func(r *http.Request) {
			if !changed && r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/orphan") {
				changed = true
				put(source, "added-during-copy", []byte("change"))
			}
		})
		failed := filepath.Join(t.TempDir(), "changed")
		refuse(backup(ctx, failed, master, c, src, s), "changed during backup")
		if _, e := os.Stat(filepath.Join(failed, "READY")); !os.IsNotExist(e) {
			t.Fatal("changed source published READY")
		}
	})
	src.Close(ctx)
	t.Run("cli-and-concurrent-session", func(t *testing.T) {
		t.Setenv("DTS_METADATA_DATABASE", srcDB)
		t.Setenv("DTS_CDC_SPOOL_S3_BUCKET", source.bucket)
		fixture := filepath.Join(t.TempDir(), "cli")
		other := open(srcDB)
		refuse(run(ctx, []string{"backup", fixture}), "exclusive DTS recovery")
		other.Close(ctx)
		check(run(ctx, []string{"backup", fixture}))
		check(run(ctx, []string{"verify", fixture}))
		t.Setenv("DTS_WRITERS_STOPPED", "NO")
		refuse(run(ctx, []string{"backup", filepath.Join(t.TempDir(), "stopped")}), "quiesce DTS")
	})
	target := open(destDB)
	defer target.Close(ctx)
	t.Run("restore-and-nonempty-target", func(t *testing.T) {
		check(restore(ctx, dir, c, target, dest, m))
		if items, e := dest.list(ctx); e != nil || len(items) != 5 {
			t.Fatal("restored objects")
		}
		refuse(restore(ctx, dir, c, target, dest, m), "empty dedicated database")
		target.Close(ctx)
		db := open(setupDB("clean"))
		defer db.Close(ctx)
		refuse(restore(ctx, dir, c, db, dest, m), "prefix is not empty")
	})
	t.Run("failed-restore-keeps-quarantine-and-no-metadata", func(t *testing.T) {
		dst := storeFor("quarantine")
		db := open(setupDB("quarantine"))
		defer db.Close(ctx)
		race := false
		s := intercept(dst, func(r *http.Request) {
			if !race && r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/applied/kept") {
				race = true
				put(dst, "applied/kept", []byte("competing write"))
			}
		})
		if e := restore(ctx, dir, c, db, s, m); e == nil {
			t.Fatal("concurrent destination write accepted")
		}
		check(emptyDB(ctx, db))
		_, e := dst.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &dst.bucket, Key: aws.String(dst.prefix + marker)})
		check(e)
		refuse(restore(ctx, dir, c, db, dst, m), "prefix is not empty")
	})
	t.Run("unfinished-multipart-and-prior-versions", func(t *testing.T) {
		s := storeFor("history")
		up, e := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &s.bucket, Key: aws.String(s.prefix + "upload")})
		check(e)
		refuse(s.virgin(ctx), "outstanding multipart")
		_, e = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &s.bucket, Key: aws.String(s.prefix + "upload"), UploadId: up.UploadId})
		check(e)
		_, e = s.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: &s.bucket, VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
		check(e)
		put(s, "removed", []byte("history"))
		_, e = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + "removed")})
		check(e)
		refuse(s.virgin(ctx), "contains object history")
	})
	t.Run("pg-restore-failure-rolls-back-and-quarantines", func(t *testing.T) {
		dst := storeFor("rollback")
		dbName := setupDB("rollback")
		role := id + "reader"
		_, err := admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" LOGIN")
		check(err)
		db := open(dbName)
		_, err = db.Exec(ctx, "REVOKE CREATE ON SCHEMA public FROM PUBLIC")
		check(err)
		db.Close(ctx)
		t.Setenv("DTS_METADATA_USER", role)
		db = open(dbName)
		defer db.Close(ctx)
		refuse(restore(ctx, dir, c, db, dst, m), "pg_restore")
		check(emptyDB(ctx, db))
		_, err = dst.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &dst.bucket, Key: aws.String(dst.prefix + marker)})
		check(err)
	})
}
