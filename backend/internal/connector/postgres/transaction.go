package postgresconnector

import (
	"context"
	"fmt"
	"time"
)

// SQLTransaction is bound to one connection for the duration of InTransaction.
// The callback must not call Connector methods, which acquire the same mutex.
type SQLTransaction struct{ client *pgClient }

func (t *SQLTransaction) ExecSQL(ctx context.Context, sql string) error {
	return t.client.exec(ctx, sql)
}
func (t *SQLTransaction) QuerySQL(ctx context.Context, sql string) (*RawRows, error) {
	return t.QueryParams(ctx, sql)
}

func (t *SQLTransaction) QueryParams(ctx context.Context, sql string, params ...string) (*RawRows, error) {
	r, err := t.client.query(ctx, sql, params...)
	if err != nil {
		return nil, err
	}
	return &RawRows{Columns: r.columns, Rows: r.rows, Nulls: r.nulls}, nil
}

func (c *Connector) InTransaction(ctx context.Context, work func(*SQLTransaction) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.connectLocked(ctx); err != nil {
		return err
	}
	client := c.client
	if err := client.exec(ctx, "BEGIN"); err != nil {
		return err
	}
	finished := false
	defer func() {
		if !finished {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := client.exec(rollbackCtx, "ROLLBACK"); err != nil {
				_ = client.close()
			}
		}
	}()
	if err := work(&SQLTransaction{client: client}); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A failed COMMIT response cannot establish whether it committed. Never
	// reconnect/replay the transaction, or run rollback on another connection.
	finished = true
	if err := client.exec(ctx, "COMMIT"); err != nil {
		_ = client.close()
		return fmt.Errorf("metadata COMMIT outcome unknown: %w", err)
	}
	return nil
}
