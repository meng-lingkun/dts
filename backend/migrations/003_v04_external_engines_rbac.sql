-- QMigration V0.4 external engines, CDC replay metadata and JDBC datasource support.
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS jdbc_url text;
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS driver_class text;
ALTER TABLE cdc_positions ADD COLUMN IF NOT EXISTS direction text NOT NULL DEFAULT 'forward';
ALTER TABLE cdc_positions ADD COLUMN IF NOT EXISTS apply_timestamp_ms bigint NOT NULL DEFAULT 0;
ALTER TABLE cdc_positions ADD COLUMN IF NOT EXISTS lag_ms bigint NOT NULL DEFAULT 0;
ALTER TABLE cdc_positions ADD COLUMN IF NOT EXISTS events_total bigint NOT NULL DEFAULT 0;
ALTER TABLE cdc_positions ADD COLUMN IF NOT EXISTS events_pending bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_start_timestamp_ms bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_start_position_type text;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS cdc_start_position_value text;
