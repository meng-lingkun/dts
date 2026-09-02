-- V0.15 unified-dev10 hardens S3 multipart crash recovery and adds the
-- QMigration-owned Oracle TTC message/state-machine + Data Dictionary plans.
-- No persistent columns are required: multipart recovery is object-store
-- maintenance and Oracle TTC remains capability-gated until real-server auth
-- and SQL execution validation are complete.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-unified-dev10', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
