package mysqlconnector

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func (c *Connector) CDCSourceIdentity(ctx context.Context) (string, error) {
	p, err := c.get(ctx)
	if err != nil {
		return "", err
	}
	r, err := p.query(ctx, "SELECT @@server_uuid")
	if err != nil {
		return "", err
	}
	if len(r.rows) != 1 || len(r.rows[0]) != 1 || len(r.rows[0][0]) == 0 {
		return "", errors.New("source returned no server UUID")
	}
	return string(r.rows[0][0]), nil
}

func (c *Connector) VerifyCDCQuiescence(ctx context.Context) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	priv, err := p.query(ctx, "SELECT count(*), @@version FROM information_schema.user_privileges WHERE GRANTEE=CONCAT(QUOTE(SUBSTRING_INDEX(CURRENT_USER(),'@',1)),'@',QUOTE(SUBSTRING_INDEX(CURRENT_USER(),'@',-1))) AND PRIVILEGE_TYPE='PROCESS'")
	if err != nil {
		return err
	}
	if len(priv.rows) != 1 || len(priv.rows[0]) != 2 || string(priv.rows[0][0]) == "0" {
		return errors.New("final freeze verification requires directly granted PROCESS visibility")
	}
	version := string(priv.rows[0][1])
	major, _, _ := strings.Cut(version, ".")
	v, err := strconv.Atoi(major)
	if err != nil {
		return fmt.Errorf("unknown MySQL version for final freeze verification: %q", version)
	}
	if v >= 8 && !strings.Contains(strings.ToLower(version), "mariadb") {
		// Unlike INNODB_TRX, data_locks reports the current locks. This also
		// catches a writer that started while a cached empty snapshot was visible.
		r, err := p.query(ctx, "SELECT @@performance_schema, EXISTS(SELECT 1 FROM performance_schema.data_locks WHERE ENGINE='INNODB' AND LOCK_MODE REGEXP '^(X|IX|AUTO_INC)(,|$)')")
		if err != nil {
			return fmt.Errorf("inspect live InnoDB write locks: %w", err)
		}
		if len(r.rows) != 1 || len(r.rows[0]) != 2 || string(r.rows[0][0]) != "1" {
			return errors.New("final freeze verification requires live performance_schema.data_locks visibility")
		}
		if string(r.rows[0][1]) != "0" {
			return errors.New("source still holds InnoDB write locks")
		}
	}
	for attempt := 0; ; attempt++ {
		r, err := p.query(ctx, "SELECT count(*) FROM information_schema.innodb_trx WHERE trx_mysql_thread_id<>CONNECTION_ID() AND trx_is_read_only=0")
		if err != nil {
			return err
		}
		if len(r.rows) != 1 || len(r.rows[0]) != 1 {
			return errors.New("missing InnoDB transaction visibility")
		}
		if string(r.rows[0][0]) == "0" {
			break
		}
		if attempt > 0 {
			return errors.New("source still has in-flight or prepared write transactions; drain them before confirming write freeze")
		}
		// InnoDB retains this observation for at least 100ms after a read.
		// A freshly rolled back XA transaction can still be in that snapshot.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
	prepared, err := p.query(ctx, "XA RECOVER")
	if err != nil {
		return err
	}
	if len(prepared.rows) > 0 {
		return errors.New("source has prepared XA transactions; resolve them before confirming write freeze")
	}
	return nil
}
