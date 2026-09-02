-- RC31: production performance controller, task-global source/target budgets,
-- SLA pacing state and adaptive hotspot split telemetry.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS auto_throughput_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS completion_sla_seconds bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS sla_started_at timestamptz;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS controller_target_bytes_sec bigint NOT NULL DEFAULT 0;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS throughput_controller_reason text;
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS adaptive_hotspot_splits bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc31', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
