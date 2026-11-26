package migration

import (
	"context"
	"dts/backend/internal/connector"
	"dts/backend/internal/repository"
	"errors"
)

func prepareTargetFence(ctx context.Context, target connector.Connector) (func() error, error) {
	f, required := repository.CDCFenceFrom(ctx)
	if !required {
		return func() error { return nil }, nil
	}
	fence, ok := target.(connector.TransactionFencer)
	if !ok {
		return nil, errors.New("target connector has no transactional execution fence")
	}
	if _, ok := target.(connector.TransactionalCDCApplyConnector); !ok {
		return nil, errors.New("target connector cannot hold execution fence in a business transaction")
	}
	if err := fence.EnsureExecutionFence(ctx); err != nil {
		return nil, err
	}
	return func() error { return fence.FenceTransaction(ctx, "cdc/"+f.TaskID+"/"+f.Direction, f.Epoch) }, nil
}
