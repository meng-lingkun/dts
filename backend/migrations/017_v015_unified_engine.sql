-- QMigration V0.15 Unified Engine migration.
-- Third-party engine names are retained only in historical release notes; active
-- task/table/runtime metadata is normalized to the single QMigration engine.

UPDATE migration_tasks
SET full_engine = 'qmigration';

UPDATE migration_tasks
SET cdc_engine = CASE
  WHEN mode = 'FULL' THEN NULL
  ELSE 'qmigration'
END;

UPDATE migration_tasks
SET rollback_cdc_engine = CASE
  WHEN mode = 'FULL' THEN NULL
  ELSE 'qmigration'
END;

UPDATE migration_tables
SET engine = 'qmigration';

UPDATE engine_jobs
SET engine = 'qmigration'
WHERE status IN ('PENDING', 'RUNNING', 'STOP_REQUESTED');

INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev1', now())
ON CONFLICT (id) DO UPDATE
SET schema_version = EXCLUDED.schema_version,
    updated_at = EXCLUDED.updated_at;
