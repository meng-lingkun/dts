-- V0.15.0 RC2 completes the SQL Server qualification-oriented software path,
-- adds truthful connector maturity/source-CDC advertising, and hardens native
-- SQL-batch numeric values. No persistent columns are required by these changes.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc2', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
