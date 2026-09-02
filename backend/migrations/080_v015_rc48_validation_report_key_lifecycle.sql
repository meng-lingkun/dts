-- QMigration V0.15.0-rc48: validation-report public-signing key lifecycle.
-- Trust stores and transition/revocation certificates are deliberately client-side/public artifacts;
-- no server secret key material is persisted in metadata.
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc48', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
