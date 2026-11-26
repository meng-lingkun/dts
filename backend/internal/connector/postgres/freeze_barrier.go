package postgresconnector

import (
	"context"
	"dts/backend/internal/domain"
	"errors"
)

func (c *Connector) CaptureFreezeBarrier(ctx context.Context) (*domain.CDCPosition, error) {
	if c.ds.Type != domain.DataSourcePostgreSQL && c.ds.Type != domain.DataSourcePolarDBPostgreSQL {
		return nil, errors.New("logical freeze marker is not qualified for this source")
	}
	var lsn string
	err := c.InTransaction(ctx, func(tx *SQLTransaction) error {
		r, err := tx.QuerySQL(ctx, "SELECT pg_logical_emit_message(true,'dts.freeze','final-write-freeze')::text")
		if err != nil {
			return err
		}
		if len(r.Rows) != 1 || len(r.Rows[0]) != 1 {
			return errors.New("logical freeze marker returned no LSN")
		}
		lsn = string(r.Rows[0][0])
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &domain.CDCPosition{DatabaseType: string(c.ds.Type), PositionType: "LSN", PositionValue: lsn}, nil
}
