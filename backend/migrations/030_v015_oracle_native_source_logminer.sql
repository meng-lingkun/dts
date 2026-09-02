-- V0.15 unified-dev14 connects the QMigration-owned Oracle TTC query runtime
-- to experimental Oracle Data Dictionary / Full Reader source capabilities and
-- an experimental DBMS_LOGMNR/SCN CDC reader. Oracle target write/schema/DDL
-- capabilities remain gated pending bind + large-LOB DML qualification.
-- No persistent columns are required for this source-side capability increment.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev14', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
