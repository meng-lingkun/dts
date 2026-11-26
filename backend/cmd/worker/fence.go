package main

import (
	"context"
	"dts/backend/internal/connector"
	"fmt"
	"time"
)

func fencedBatch(ctx context.Context, dst connector.DataConnector, scope string, generation int64, req connector.WriteBatchRequest) (int64, error) {
	// Zero generations exist only in trusted legacy unit fixtures. Worker main
	// refuses such claims; every production claim takes the fenced path.
	if generation <= 0 {
		return dst.WriteBatch(ctx, req)
	}
	f, ok := dst.(connector.TransactionFencer)
	if !ok {
		return 0, fmt.Errorf("target has no transaction fence")
	}
	tx, ok := dst.(connector.TransactionalCDCApplyConnector)
	if !ok {
		return 0, fmt.Errorf("target has no fenced transaction")
	}
	if err := tx.BeginCDCTransaction(ctx); err != nil {
		return 0, err
	}
	open := true
	defer func() {
		if open {
			rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tx.RollbackCDCTransaction(rctx)
		}
	}()
	if err := f.FenceTransaction(ctx, scope, generation); err != nil {
		return 0, err
	}
	if err := f.VerifyFencedTable(ctx, req.Schema, req.Table); err != nil {
		return 0, err
	}
	n, err := dst.WriteBatch(ctx, req)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	open = false
	if err := tx.CommitCDCTransaction(ctx); err != nil {
		return 0, fmt.Errorf("full-load COMMIT outcome unknown; inspect target before repair: %w", err)
	}
	return n, nil
}
