-- V0.15.0 RC4 consumes the existing non-secret datasources.cdc_url field for
-- OceanBase Binlog Service subscription coordinates. No credential-bearing
-- schema change is required; this migration advances the metadata version only.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc4', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
