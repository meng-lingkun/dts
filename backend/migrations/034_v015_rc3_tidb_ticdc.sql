-- V0.15.0 RC3 adds a non-secret CDC control endpoint descriptor. TiDB uses it
-- for TiCDC OpenAPI + Kafka bootstrap coordinates while keeping SQL and CDC
-- endpoints explicitly separated.
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS cdc_url text;

INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc3', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
