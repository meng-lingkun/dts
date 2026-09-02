-- QMigration V0.5 native CDC, post-load DDL and resource-aware scheduling metadata.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS paused_from_status text;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS post_load_ddl_mode text NOT NULL DEFAULT 'INDEXES';
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS rollback_cdc_engine text;
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS engine text NOT NULL DEFAULT 'native';
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS indexes_json jsonb NOT NULL DEFAULT '[]';
ALTER TABLE migration_tables ADD COLUMN IF NOT EXISTS foreign_keys_json jsonb NOT NULL DEFAULT '[]';
ALTER TABLE workers ADD COLUMN IF NOT EXISTS cpu_usage_pct double precision NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS memory_usage_pct double precision NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS network_rx_bps bigint NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS network_tx_bps bigint NOT NULL DEFAULT 0;
