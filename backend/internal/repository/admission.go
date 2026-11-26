package repository

import "context"

type CDCFence struct {
	TaskID, Direction, Owner string
	Epoch                    int64
}
type ManagedFence struct {
	JobID, WorkerID string
	Generation      int64
}
type cdcFenceKey struct{}
type managedFenceKey struct{}

func WithCDCFence(ctx context.Context, f CDCFence) context.Context {
	return context.WithValue(ctx, cdcFenceKey{}, f)
}
func CDCFenceFrom(ctx context.Context) (CDCFence, bool) {
	f, ok := ctx.Value(cdcFenceKey{}).(CDCFence)
	return f, ok
}
func WithManagedFence(ctx context.Context, f ManagedFence) context.Context {
	return context.WithValue(ctx, managedFenceKey{}, f)
}
func ManagedFenceFrom(ctx context.Context) (ManagedFence, bool) {
	f, ok := ctx.Value(managedFenceKey{}).(ManagedFence)
	return f, ok
}
