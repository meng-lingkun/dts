-- V0.15.0 RC5 adds the QMigration-owned DB2 LUW DRDA/DDM connector and
-- qualification workflow. No credential-bearing schema change is required.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc5', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
