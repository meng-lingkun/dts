-- RC41: long-running control-plane metadata retention and aggregate hot paths.
-- These indexes support bounded janitor deletes and task+direction checkpoint head lookup.
CREATE INDEX IF NOT EXISTS idx_task_logs_task_time_id ON task_logs(task_id,created_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_time_id ON audit_events(created_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_cdc_positions_task_direction_time_id ON cdc_positions(task_id,direction,recorded_at DESC,id DESC);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc41', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
