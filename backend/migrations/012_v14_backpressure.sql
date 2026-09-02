-- V0.14: per-chunk latency telemetry used by adaptive backpressure.
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS last_read_ms bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS last_write_ms bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS last_batch_rows integer NOT NULL DEFAULT 0;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS backpressure_level text NOT NULL DEFAULT '';
