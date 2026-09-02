# QMigration V0.15.0-unified-dev9 Release Notes

## Scope

This development snapshot continues the single QMigration Unified Engine. It does not add DataX, SeaTunnel, Flink CDC, Debezium, Canal or another migration runtime.

## S3-compatible CDC spool hardening

- Automatic multipart upload for encrypted spool objects over `QMIGRATION_CDC_SPOOL_S3_MULTIPART_THRESHOLD_BYTES` (default 8 MiB).
- Configurable multipart part size via `QMIGRATION_CDC_SPOOL_S3_MULTIPART_PART_BYTES` (default 8 MiB, minimum 5 MiB).
- A failed part/completion aborts the upload; Metadata is never committed and source ACK must not advance.
- New `spools3:v2` references include SHA-256 of the encrypted payload. Hydration verifies the object before secure-repository decrypt/apply.
- `spools3:v1` remains readable so pending dev8 transactions survive an upgrade.
- Fixed missing `ConfigFromEnv` wiring for S3 CA, TLS ServerName and mTLS client certificate/private key.

## Oracle Native foundation

Oracle TNS CONNECT/Redirect/TCPS now has a live post-ACCEPT session handoff. The accepted connection can continue carrying TNS DATA for a future TTC authenticator instead of being discarded by a probe-only flow. TTC authentication, Oracle Data Dictionary, Full Reader/Writer and Redo CDC remain capability-gated; dev9 does not claim they are complete.

## Mandatory archive workflow

Every subsequent development execution must be archived. `deployments/scripts/archive-version.sh` generates and verifies:

- source ZIP;
- previous-version incremental binary patch;
- formal V0.13 cumulative binary patch;
- SHA-256 file;
- JSON archive manifest.

Both patches must be clean-applied, restored trees must equal the current normalized source tree, and backend `go test ./...` plus `go vet ./...` must pass before an archive is accepted.

## Metadata

`025_v015_spool_multipart_integrity.sql` advances the Metadata Schema marker to `0.15.0-unified-dev9`; no new spool table columns are required because object references remain intentionally opaque.
