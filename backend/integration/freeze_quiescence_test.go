//go:build integration

package integration

import (
	"context"
	"dts/backend/internal/connector"
	my "dts/backend/internal/connector/mysql"
	pg "dts/backend/internal/connector/postgres"
	"dts/backend/internal/domain"
	"fmt"
	"testing"
	"time"
)

func TestPostgresSelectedTableFreezeLifetime(t *testing.T) {
	requireIsolated(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	newConnection := func() *pg.Connector {
		raw, err := pg.NewFactory().New(pgDS())
		if err != nil {
			t.Fatal(err)
		}
		c := raw.(*pg.Connector)
		t.Cleanup(func() { c.Close() })
		return c
	}
	holder, observer := newConnection(), newConnection()
	name := fmt.Sprintf("locked_%d", time.Now().UnixNano())
	parent := "dts_e2e." + name
	if err := observer.ExecSQL(ctx, "CREATE SCHEMA IF NOT EXISTS dts_e2e; CREATE TABLE "+parent+"(id int PRIMARY KEY) PARTITION BY RANGE(id); CREATE TABLE "+parent+"_part PARTITION OF "+parent+" DEFAULT; CREATE TABLE "+parent+"_other(id int)"); err != nil {
		t.Fatal(err)
	}
	defer observer.ExecSQL(ctx, "DROP TABLE "+parent+" CASCADE; DROP TABLE "+parent+"_other")
	tables := []domain.MigrationTable{{SourceSchema: "dts_e2e", SourceTable: name}}
	proof, err := holder.HoldFreezeTables(ctx, tables)
	if err != nil {
		t.Fatal(err)
	}
	if err := observer.VerifyTableFreeze(ctx, proof); err != nil {
		t.Fatal(err)
	}
	settings, err := holder.QuerySQL(ctx, "SHOW idle_in_transaction_session_timeout")
	if err != nil || string(settings.Rows[0][0]) != "15min" {
		t.Fatalf("idle guard timeout: %v %v", settings, err)
	}
	if err := observer.ExecSQL(ctx, "SET lock_timeout='120ms'"); err != nil {
		t.Fatal(err)
	}
	if err := observer.ExecSQL(ctx, "INSERT INTO "+parent+"_part VALUES(1)"); err == nil {
		t.Fatal("partition write crossed the freeze")
	}
	if err := observer.ExecSQL(ctx, "INSERT INTO "+parent+"_other VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err := observer.VerifyTableFreeze(ctx, proof); err != nil {
		t.Fatal(err)
	}
	holder.Close()
	waitForRelease := func(token string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for observer.VerifyTableFreeze(ctx, token) == nil {
			if time.Now().After(deadline) {
				t.Fatal("orphan freeze still accepted")
			}
			time.Sleep(25 * time.Millisecond)
		}
		if err := observer.ExecSQL(ctx, "INSERT INTO "+parent+" VALUES(1) ON CONFLICT DO NOTHING"); err != nil {
			t.Fatal(err)
		}
	}
	waitForRelease(proof)
	short := newConnection()
	proof, err = short.HoldFreezeTables(ctx, tables)
	if err != nil {
		t.Fatal(err)
	}
	if err := short.ExecSQL(ctx, "SET LOCAL idle_in_transaction_session_timeout='100ms'"); err != nil {
		t.Fatal(err)
	}
	waitForRelease(proof)
}

func TestNativeFreezeRejectsOpenAndPreparedWriters(t *testing.T) {
	requireIsolated(t)
	for _, kind := range []string{"mysql", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var a, b nativeTarget
			if kind == "mysql" {
				x, _ := my.NewFactory().New(mysqlDS())
				y, _ := my.NewFactory().New(mysqlDS())
				a = x.(nativeTarget)
				b = y.(nativeTarget)
			} else {
				x, _ := pg.NewFactory().New(pgDS())
				y, _ := pg.NewFactory().New(pgDS())
				a = x.(nativeTarget)
				b = y.(nativeTarget)
				if err := a.ExecSQL(ctx, "CREATE SCHEMA IF NOT EXISTS dts_e2e"); err != nil {
					t.Fatal(err)
				}
			}
			defer a.Close()
			defer b.Close()
			inspector := b.(connector.CDCQuiescenceVerifier)
			table := fmt.Sprintf("freeze_writers_%d", time.Now().UnixNano())
			if err := a.ExecSQL(ctx, "CREATE TABLE dts_e2e."+table+" (id bigint PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			defer a.ExecSQL(context.Background(), "DROP TABLE dts_e2e."+table)
			// Prime an empty transaction snapshot immediately before the writer
			// starts. MySQL can cache that snapshot across the next observation.
			if err := inspector.VerifyCDCQuiescence(ctx); err != nil {
				t.Fatalf("initially drained source rejected: %v", err)
			}
			if err := a.BeginCDCTransaction(ctx); err != nil {
				t.Fatal(err)
			}
			if err := a.ExecSQL(ctx, "INSERT INTO dts_e2e."+table+" VALUES(1)"); err != nil {
				t.Fatal(err)
			}
			if err := inspector.VerifyCDCQuiescence(ctx); err == nil {
				t.Fatal("open writer accepted as frozen")
			}
			if err := a.RollbackCDCTransaction(ctx); err != nil {
				t.Fatal(err)
			}
			xid := fmt.Sprintf("dts_qual_%d", time.Now().UnixNano())
			if kind == "mysql" {
				for _, q := range []string{"XA START '" + xid + "'", "INSERT INTO dts_e2e." + table + " VALUES(2)", "XA END '" + xid + "'", "XA PREPARE '" + xid + "'"} {
					if err := a.ExecSQL(ctx, q); err != nil {
						t.Fatal(err)
					}
				}
				defer b.ExecSQL(context.Background(), "XA ROLLBACK '"+xid+"'")
				if err := inspector.VerifyCDCQuiescence(ctx); err == nil {
					t.Fatal("prepared XA writer accepted as frozen")
				}
				if err := b.ExecSQL(ctx, "XA ROLLBACK '"+xid+"'"); err != nil {
					t.Fatal(err)
				}
			} else {
				for _, q := range []string{"BEGIN", "INSERT INTO dts_e2e." + table + " VALUES(2)", "PREPARE TRANSACTION '" + xid + "'"} {
					if err := a.ExecSQL(ctx, q); err != nil {
						t.Fatal(err)
					}
				}
				defer b.ExecSQL(context.Background(), "ROLLBACK PREPARED '"+xid+"'")
				if err := inspector.VerifyCDCQuiescence(ctx); err == nil {
					t.Fatal("prepared writer accepted as frozen")
				}
				if err := b.ExecSQL(ctx, "ROLLBACK PREPARED '"+xid+"'"); err != nil {
					t.Fatal(err)
				}
			}
			if err := inspector.VerifyCDCQuiescence(ctx); err != nil {
				t.Fatalf("drained source rejected: %v", err)
			}
		})
	}
}
