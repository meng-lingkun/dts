-- QMigration V0.15.0-rc43: bounded validation paging and latest-result coverage.
CREATE INDEX IF NOT EXISTS idx_chunks_validation_table_page
  ON migration_chunks(task_id, table_id, chunk_no, id);
CREATE INDEX IF NOT EXISTS idx_validation_task_chunk_latest
  ON validation_results(task_id, chunk_id, finished_at DESC, id DESC) INCLUDE (status);
CREATE INDEX IF NOT EXISTS idx_validation_task_status_chunk
  ON validation_results(task_id, status, chunk_id, finished_at DESC);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc43', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
