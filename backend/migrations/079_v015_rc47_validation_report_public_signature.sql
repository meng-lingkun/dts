-- QMigration V0.15.0-rc47: publicly verifiable validation-report delivery.
CREATE TABLE IF NOT EXISTS validation_report_archives(
  task_id text NOT NULL,
  evidence_digest text NOT NULL,
  uri text NOT NULL,
  bucket text,
  prefix text,
  manifest_sha256 text NOT NULL,
  public_signature_algorithm text,
  public_signature_key_id text,
  public_key_ed25519 text,
  public_key_fingerprint_sha256 text,
  object_lock_mode text,
  retain_until text,
  legal_hold boolean NOT NULL DEFAULT false,
  committed_at timestamptz NOT NULL,
  PRIMARY KEY(task_id,evidence_digest)
);
CREATE INDEX IF NOT EXISTS idx_validation_report_archives_committed_at ON validation_report_archives(committed_at DESC);
INSERT INTO metadata_schema_state(id, schema_version, updated_at)
VALUES (1, '0.15.0-rc47', now())
ON CONFLICT (id) DO UPDATE SET schema_version=EXCLUDED.schema_version, updated_at=EXCLUDED.updated_at;
