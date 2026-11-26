package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	pg "dts/backend/internal/connector/postgres"
	"dts/backend/internal/domain"
)

// Each slot opens its wire connection lazily. A transaction owns its slot
// through COMMIT/ROLLBACK, including handling a lost COMMIT response.
type metadataPool struct {
	idle     chan *pg.Connector
	stopping chan struct{}
	closed   sync.Once
	closeErr error
}

func newMetadataPool(ds domain.DataSource, size int) (*metadataPool, error) {
	if size < 1 || size > 128 {
		return nil, fmt.Errorf("metadata pool size must be between 1 and 128")
	}
	p := &metadataPool{idle: make(chan *pg.Connector, size), stopping: make(chan struct{})}
	for i := 0; i < size; i++ {
		conn, err := pg.NewFactory().New(ds)
		if err != nil {
			return nil, err
		}
		p.idle <- conn.(*pg.Connector)
	}
	return p, nil
}

func (p *metadataPool) withConn(ctx context.Context, work func(*pg.Connector) error) error {
	select {
	case <-p.stopping:
		return sql.ErrConnDone
	case <-ctx.Done():
		return ctx.Err()
	case c := <-p.idle:
		defer func() { p.idle <- c }()
		select {
		case <-p.stopping:
			return sql.ErrConnDone
		default:
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return work(c)
	}
}

func (p *metadataPool) ExecSQL(ctx context.Context, query string) error {
	return p.withConn(ctx, func(c *pg.Connector) error { return c.ExecSQL(ctx, query) })
}

func (p *metadataPool) QuerySQL(ctx context.Context, query string) (*pg.RawRows, error) {
	return p.QueryParams(ctx, query)
}

func (p *metadataPool) QueryParams(ctx context.Context, query string, params ...string) (*pg.RawRows, error) {
	var rows *pg.RawRows
	err := p.withConn(ctx, func(c *pg.Connector) error {
		var err error
		rows, err = c.QueryParams(ctx, query, params...)
		return err
	})
	return rows, err
}

func (p *metadataPool) InTransaction(ctx context.Context, work func(*pg.SQLTransaction) error) error {
	return p.withConn(ctx, func(c *pg.Connector) error { return c.InTransaction(ctx, work) })
}

func (p *metadataPool) Close() error {
	p.closed.Do(func() {
		close(p.stopping)
		for i := 0; i < cap(p.idle); i++ {
			c := <-p.idle
			p.closeErr = errors.Join(p.closeErr, c.Close())
		}
	})
	return p.closeErr
}
