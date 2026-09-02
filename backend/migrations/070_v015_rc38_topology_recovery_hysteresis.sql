-- QMigration V0.15.0-rc38: topology recovery hysteresis/staged concurrency.
-- good_streak and recovery_concurrency_cap live inside the existing
-- migration_tables.topology_performance_json document, so no new physical
-- column is required. This marker advances the metadata schema version.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc38', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
