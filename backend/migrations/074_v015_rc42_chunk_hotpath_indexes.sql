-- RC42 bounded chunk hot paths for 10-40TB long-running tasks.
CREATE INDEX IF NOT EXISTS idx_chunks_task_chunk_no_desc
    ON migration_chunks(task_id, chunk_no DESC);
CREATE INDEX IF NOT EXISTS idx_chunks_pending_table_hot
    ON migration_chunks(task_id, table_id, chunk_no, id)
    WHERE status='PENDING';
CREATE INDEX IF NOT EXISTS idx_chunks_running_topology_hot
    ON migration_chunks(task_id, topology_id, started_at, chunk_no, id)
    WHERE status='RUNNING';
CREATE INDEX IF NOT EXISTS idx_chunks_running_fault_rack_hot
    ON migration_chunks(task_id, (fault_domain_json->>'rack'), started_at, chunk_no, id)
    WHERE status='RUNNING';
CREATE INDEX IF NOT EXISTS idx_chunks_running_fault_zone_hot
    ON migration_chunks(task_id, (fault_domain_json->>'zone'), started_at, chunk_no, id)
    WHERE status='RUNNING';
CREATE INDEX IF NOT EXISTS idx_chunks_running_fault_region_hot
    ON migration_chunks(task_id, (fault_domain_json->>'region'), started_at, chunk_no, id)
    WHERE status='RUNNING';
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc42', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
