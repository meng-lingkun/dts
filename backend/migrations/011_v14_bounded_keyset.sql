-- V0.14: immutable lower/upper tuple bounds for parallel durable keyset chunks.
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS start_cursor_json text;
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS end_cursor_json text;
