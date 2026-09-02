# QMigration V0.15.0-unified-dev10 Release Notes

## Scope

This snapshot continues the single QMigration Unified Engine. It does not add an external migration runtime.

## S3-compatible CDC spool crash recovery

- Adds native `ListMultipartUploads` support to the QMigration SigV4 S3 client.
- `Reconcile` aborts only stale multipart uploads beneath the QMigration `pending/` prefix.
- Default stale-upload threshold is 6 hours and is configurable with `QMIGRATION_CDC_SPOOL_S3_MULTIPART_ABORT_AFTER_HOURS`.
- Fresh uploads are never aborted by reconciliation; uploads without a trustworthy `Initiated` timestamp are also left untouched.
- Source ACK semantics do not change: a multipart upload must be completed and its Metadata row committed before the position can be acknowledged.

## Oracle Native TTC foundation

- Adds a QMigration-owned TTC message stream on top of the existing accepted TNS DATA session.
- Adds an explicit TTC session phase machine: transport -> protocol -> data type -> authenticated -> ready.
- Adds native Oracle Data Dictionary query plans for users, tables, columns, primary keys and indexes.
- Adds Oracle type normalization and identifier quoting helpers for the future native metadata/full-load path.
- This is deliberately still capability-gated: validated TTC username/password authentication and SQL execution against a real Oracle instance are not claimed complete in dev10.

## Metadata

`026_v015_ttc_spool_recovery.sql` advances the Metadata Schema marker to `0.15.0-unified-dev10` without adding table columns.

## Archive requirement

The version is accepted only after the mandatory archive flow produces a source ZIP, previous-version incremental patch, formal V0.13 cumulative patch, SHA-256 file and manifest, and verifies both restored trees with `go test ./...` and `go vet ./...`.
