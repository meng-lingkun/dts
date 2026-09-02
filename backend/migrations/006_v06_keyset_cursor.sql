-- V0.6 resumable generic keyset full-load cursor
ALTER TABLE migration_chunks ADD COLUMN IF NOT EXISTS cursor_json text;
