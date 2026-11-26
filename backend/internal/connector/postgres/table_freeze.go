package postgresconnector

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"

	"dts/backend/internal/domain"
)

// The insert position includes asynchronous commits which may not have been
// written out at the instant a physical freeze baseline is sampled.
func (c *Connector) CurrentFreezeHead(ctx context.Context) (*domain.CDCPosition, error) {
	if c.ds.Type != domain.DataSourcePostgreSQL && c.ds.Type != domain.DataSourcePolarDBPostgreSQL {
		return c.CurrentCDCPosition(ctx)
	}
	r, err := c.QuerySQL(ctx, "SELECT pg_current_wal_insert_lsn()::text")
	if err != nil {
		return nil, err
	}
	if len(r.Rows) != 1 || len(r.Rows[0]) != 1 || len(r.Rows[0][0]) == 0 {
		return nil, errors.New("source returned no WAL insert position")
	}
	return &domain.CDCPosition{DatabaseType: string(c.ds.Type), PositionType: "LSN", PositionValue: string(r.Rows[0][0])}, nil
}

func freezeRelationNames(tables []domain.MigrationTable) ([]string, error) {
	seen := make(map[string]bool)
	var names []string
	for _, table := range tables {
		if table.SourceSchema == "" || table.SourceTable == "" || strings.ContainsRune(table.SourceSchema+table.SourceTable, 0) {
			return nil, errors.New("table freeze requires fully qualified selected relations")
		}
		name := pgIdent(table.SourceSchema) + "." + pgIdent(table.SourceTable)
		if !seen[name] {
			names = append(names, name)
			seen[name] = true
		}
	}
	if len(names) == 0 {
		return nil, errors.New("table freeze requires a prepared, nonempty table selection")
	}
	sort.Strings(names)
	return names, nil
}

// The advisory lock is used only as a transaction lifetime witness. The
// relation locks taken in the same transaction enforce the actual barrier.
const freezeLockRows = `SELECT concat_ws('/', database::text, pid::text, virtualtransaction, classid::text, objid::text) AS proof
FROM pg_catalog.pg_locks WHERE locktype='advisory' AND granted AND mode='ExclusiveLock' AND objsubid=1
AND database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())`

func (c *Connector) HoldFreezeTables(ctx context.Context, tables []domain.MigrationTable) (string, error) {
	if c.ds.Type != domain.DataSourcePostgreSQL && c.ds.Type != domain.DataSourcePolarDBPostgreSQL {
		return "", errors.New("selected-table freeze requires native PostgreSQL")
	}
	names, err := freezeRelationNames(tables)
	if err != nil {
		return "", err
	}
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return "", err
	}
	key := binary.BigEndian.Uint64(nonce[:]) >> 1
	if err := c.BeginCDCTransaction(ctx); err != nil {
		return "", err
	}
	retain := false
	defer func() {
		if !retain {
			c.Close()
		}
	}()
	if err := c.ExecSQL(ctx, "SET LOCAL idle_in_transaction_session_timeout='15min'"); err != nil {
		return "", err
	}
	// Omitting ONLY locks all descendants as well. NOWAIT rejects uncommitted
	// and prepared writers without putting a pending lock in the source queue.
	if err := c.ExecSQL(ctx, "LOCK TABLE "+strings.Join(names, ",")+" IN SHARE MODE NOWAIT"); err != nil {
		return "", fmt.Errorf("lock selected source relations: %w", err)
	}
	// Foreign tables and their remote rows cannot be frozen by a local lock.
	rows, err := c.QuerySQL(ctx, `SELECT count(*) FROM pg_catalog.pg_locks l JOIN pg_catalog.pg_class c ON c.oid=l.relation
WHERE l.pid=pg_backend_pid() AND l.granted AND l.mode='ShareLock' AND (c.relkind NOT IN ('r','p','i','I') OR c.relpersistence<>'p')`)
	if err != nil {
		return "", err
	}
	if len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || string(rows.Rows[0][0]) != "0" {
		return "", errors.New("selected-table freeze requires permanent local PostgreSQL tables and partitions")
	}
	rows, err = c.QuerySQL(ctx, fmt.Sprintf("SELECT pg_try_advisory_xact_lock(%d::bigint)::text", key))
	if err != nil {
		return "", err
	}
	if len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || string(rows.Rows[0][0]) != "true" {
		return "", errors.New("could not establish a unique freeze transaction witness")
	}
	rows, err = c.QuerySQL(ctx, fmt.Sprintf("%s AND pid=pg_backend_pid() AND classid=%d::oid AND objid=%d::oid", freezeLockRows, key>>32, uint32(key)))
	if err != nil {
		return "", err
	}
	if len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || len(rows.Rows[0][0]) == 0 {
		return "", errors.New("missing freeze transaction witness")
	}
	retain = true
	return string(rows.Rows[0][0]), nil
}

func (c *Connector) VerifyTableFreeze(ctx context.Context, proof string) error {
	rows, err := c.QuerySQL(ctx, "SELECT proof FROM ("+freezeLockRows+") l WHERE proof="+pgString(proof))
	if err != nil {
		return err
	}
	if len(rows.Rows) != 1 {
		return errors.New("selected-table freeze transaction is gone; confirm a new freeze")
	}
	return nil
}
