-- V1.0.0 RC2 release hardening metadata.
-- This table gives operators an explicit durable metadata-schema marker while
-- schema.sql remains idempotent for fresh installs and in-place upgrades.
CREATE TABLE IF NOT EXISTS metadata_schema_state(
  id integer PRIMARY KEY CHECK (id = 1),
  schema_version text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '1.0.0-rc2', now())
ON CONFLICT (id) DO UPDATE
SET schema_version = EXCLUDED.schema_version, updated_at = EXCLUDED.updated_at;
