package repository_test

import (
	"context"
	"testing"
	"time"

	"dts/backend/internal/repository"
	"dts/backend/internal/repository/memory"
	"dts/backend/internal/repository/secure"
	"dts/backend/internal/repository/spoolfile"
	"dts/backend/internal/repository/spools3"
	"dts/backend/internal/security"
)

// Exercise the actual production wrapper order. Testing each store separately
// misses optional interfaces hidden by an embedded Repository interface.
func TestControlLeasesSurviveProductionSpoolWrappers(t *testing.T) {
	for _, storage := range []string{"file", "s3"} {
		t.Run(storage, func(t *testing.T) {
			ctx := context.Background()
			base := memory.New()
			var wrapped repository.Repository
			var err error
			if storage == "file" {
				wrapped, err = spoolfile.New(base, spoolfile.Config{Root: t.TempDir()})
			} else {
				wrapped, err = spools3.New(base, spools3.Config{Endpoint: "http://127.0.0.1:1", Bucket: "spool", AccessKey: "test", SecretKey: "test"})
			}
			if err != nil {
				t.Fatal(err)
			}
			cipher, err := security.New("test-master-key")
			if err != nil {
				t.Fatal(err)
			}
			store := secure.New(wrapped, cipher)
			ok, err := store.AcquireControlOperation(ctx, "task", "prepare", "server-a", time.Minute)
			if err != nil || !ok {
				t.Fatalf("acquire through wrappers: ok=%v err=%v", ok, err)
			}
			if ok, err := base.AcquireControlOperation(ctx, "task", "validation", "server-b", time.Minute); err != nil || ok {
				t.Fatalf("underlying lease must exclude another server: ok=%v err=%v", ok, err)
			}
			if err := store.RenewControlOperation(ctx, "task", "prepare", "server-a", time.Minute); err != nil {
				t.Fatal(err)
			}
			if err := store.ReleaseControlOperation(ctx, "task", "prepare", "server-a"); err != nil {
				t.Fatal(err)
			}
			if ok, err := base.AcquireControlOperation(ctx, "task", "validation", "server-b", time.Minute); err != nil || !ok {
				t.Fatalf("release did not reach metadata store: ok=%v err=%v", ok, err)
			}
		})
	}
}
