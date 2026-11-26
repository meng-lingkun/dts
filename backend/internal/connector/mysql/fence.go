package mysqlconnector

import (
	"context"
	"dts/backend/internal/domain"
	"errors"
	"fmt"
	"strconv"
)

func (c *Connector) EnsureExecutionFence(ctx context.Context) error {
	if c.ds.Type != domain.DataSourceMySQL && c.ds.Type != domain.DataSourcePolarDBMySQL {
		return errors.New("target transaction fencing is not qualified for this MySQL-compatible database")
	}
	if c.ds.Database == "" {
		return errors.New("target fencing requires an explicit target database")
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	_, err = p.exec(ctx, "CREATE TABLE IF NOT EXISTS "+quoteIdent(c.ds.Database)+".__dts_execution_fence(scope varchar(512) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,generation bigint NOT NULL) ENGINE=InnoDB")
	if err != nil {
		return err
	}
	return c.VerifyFencedTable(ctx, c.ds.Database, "__dts_execution_fence")
}

func (c *Connector) VerifyFencedTable(ctx context.Context, schema, table string) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	// Resolve the table inside the open business transaction first. MySQL holds
	// its metadata lock until COMMIT, so ENGINE cannot change after inspection.
	if _, err := p.query(ctx, "SELECT 1 FROM "+quoteIdent(schema)+"."+quoteIdent(table)+" LIMIT 0"); err != nil {
		return err
	}
	r, err := p.query(ctx, fmt.Sprintf("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=CONVERT(0x%x USING utf8mb4) AND TABLE_NAME=CONVERT(0x%x USING utf8mb4)", []byte(schema), []byte(table)))
	if err != nil {
		return err
	}
	if len(r.rows) != 1 || len(r.rows[0]) != 1 || string(r.rows[0][0]) != "InnoDB" {
		return errors.New("target fence requires an InnoDB table")
	}
	return nil
}
func (c *Connector) FenceTransaction(ctx context.Context, scope string, generation int64) error {
	if generation <= 0 || scope == "" || len(scope) > 512 {
		return errors.New("invalid target fence")
	}
	if err := c.VerifyFencedTable(ctx, c.ds.Database, "__dts_execution_fence"); err != nil {
		return err
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	table := quoteIdent(c.ds.Database) + ".__dts_execution_fence"
	// Scope is hex encoded to avoid dependence on SQL string escape modes.
	literal := fmt.Sprintf("CONVERT(0x%x USING ascii)", []byte(scope))
	_, err = p.exec(ctx, fmt.Sprintf("INSERT INTO %s(scope,generation) VALUES(%s,%d) ON DUPLICATE KEY UPDATE generation=GREATEST(generation,%d)", table, literal, generation, generation))
	if err != nil {
		return err
	}
	r, err := p.query(ctx, "SELECT generation FROM "+table+" WHERE scope="+literal+" FOR UPDATE")
	if err != nil {
		return err
	}
	if len(r.rows) != 1 || len(r.rows[0]) != 1 || string(r.rows[0][0]) != strconv.FormatInt(generation, 10) {
		return errors.New("stale execution rejected by MySQL target fence")
	}
	return nil
}
