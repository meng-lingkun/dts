package postgresconnector

import (
	"context"
	"dts/backend/internal/domain"
	"errors"
	"fmt"
)

func (c *Connector) EnsureExecutionFence(ctx context.Context) error {
	if c.ds.Type != domain.DataSourcePostgreSQL && c.ds.Type != domain.DataSourcePolarDBPostgreSQL {
		return errors.New("target transaction fencing is not qualified for this PostgreSQL-compatible database")
	}
	return c.InTransaction(ctx, func(tx *SQLTransaction) error {
		if err := tx.ExecSQL(ctx, "SELECT pg_advisory_xact_lock(5849328471002)"); err != nil {
			return err
		}
		return tx.ExecSQL(ctx, "CREATE SCHEMA IF NOT EXISTS dts_internal; CREATE TABLE IF NOT EXISTS dts_internal.execution_fence(scope text PRIMARY KEY,generation bigint NOT NULL CHECK(generation>0))")
	})
}
func (c *Connector) FenceTransaction(ctx context.Context, scope string, generation int64) error {
	if generation <= 0 || scope == "" {
		return errors.New("invalid target fence")
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	q := fmt.Sprintf("INSERT INTO dts_internal.execution_fence(scope,generation) VALUES(%s,%d) ON CONFLICT(scope) DO UPDATE SET generation=EXCLUDED.generation WHERE execution_fence.generation<=EXCLUDED.generation RETURNING generation", pgString(scope), generation)
	r, err := p.query(ctx, q)
	if err != nil {
		return err
	}
	if len(r.rows) != 1 {
		return errors.New("stale execution rejected by PostgreSQL target fence")
	}
	return nil
}

func (c *Connector) VerifyFencedTable(ctx context.Context, schema, table string) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	if err := p.exec(ctx, "LOCK TABLE "+pgIdent(schema)+"."+pgIdent(table)+" IN ROW EXCLUSIVE MODE"); err != nil {
		return err
	}
	r, err := p.query(ctx, "SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname="+pgString(schema)+" AND c.relname="+pgString(table)+" AND c.relkind IN ('r','p')")
	if err != nil {
		return err
	}
	if len(r.rows) != 1 {
		return errors.New("target fence requires an ordinary or partitioned PostgreSQL table")
	}
	return nil
}
