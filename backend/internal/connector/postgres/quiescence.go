package postgresconnector

import (
	"context"
	"dts/backend/internal/domain"
	"errors"
)

func (c *Connector) CDCSourceIdentity(ctx context.Context) (string, error) {
	p, err := c.get(ctx)
	if err != nil {
		return "", err
	}
	r, err := p.query(ctx, "SELECT system_identifier::text FROM pg_control_system()")
	if err != nil {
		return "", err
	}
	if len(r.rows) != 1 || len(r.rows[0]) != 1 || len(r.rows[0][0]) == 0 {
		return "", errors.New("source returned no cluster system identifier")
	}
	return string(r.rows[0][0]), nil
}

func (c *Connector) VerifyCDCQuiescence(ctx context.Context) error {
	if c.ds.Type != domain.DataSourcePostgreSQL && c.ds.Type != domain.DataSourcePolarDBPostgreSQL {
		return errors.New("write-freeze transaction verification is not qualified for this PostgreSQL derivative")
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	priv, err := p.query(ctx, "SELECT (rolsuper OR pg_has_role(current_user,'pg_read_all_stats','MEMBER'))::text FROM pg_roles WHERE rolname=current_user")
	if err != nil {
		return err
	}
	if len(priv.rows) != 1 || len(priv.rows[0]) == 0 || (string(priv.rows[0][0]) != "true" && string(priv.rows[0][0]) != "t") {
		return errors.New("final freeze verification requires pg_read_all_stats or superuser visibility")
	}
	r, err := p.query(ctx, "SELECT (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_type='client backend' AND backend_xid IS NOT NULL)+(SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database())")
	if err != nil {
		return err
	}
	if len(r.rows) != 1 || len(r.rows[0]) == 0 || string(r.rows[0][0]) != "0" {
		return errors.New("source still has in-flight or prepared write transactions; drain them before confirming write freeze")
	}
	return nil
}
