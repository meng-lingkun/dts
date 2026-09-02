-- RC30: task-global target throughput. Network/disk chaos features are runtime
-- only and do not need persistent schema beyond this control-plane field.
ALTER TABLE migration_tasks ADD COLUMN IF NOT EXISTS target_throughput_mbps bigint NOT NULL DEFAULT 0;
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc30', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
