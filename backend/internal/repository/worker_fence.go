package repository

import (
	"context"
	"time"
)

type workerGenerationKey struct{}

func WithLeaseGeneration(ctx context.Context, generation int64) context.Context {
	return context.WithValue(ctx, workerGenerationKey{}, generation)
}
func LeaseGeneration(ctx context.Context) (int64, bool) {
	v, ok := ctx.Value(workerGenerationKey{}).(int64)
	return v, ok
}
func WorkerLeaseValid(ctx context.Context, generation int64, until time.Time) bool {
	if expected, required := LeaseGeneration(ctx); required {
		return expected > 0 && expected == generation && until.After(time.Now())
	}
	// Legacy zero-generation records are only accessible by trusted in-process
	// callers. HTTP worker APIs always require a positive generation.
	return generation == 0 || until.After(time.Now())
}
