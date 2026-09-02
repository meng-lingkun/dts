# QMigration V0.15.0-rc20 Release Notes

RC20 adds the first qualification-gated **GBase 8s V8.8 source CDC software
path**. Full/target behavior from RC19 is retained. Source capture does not use
ODBC ResultSets: GBase 8s/Informix-style `syscdcv1` sessions expose CDC bytes
through a CSDK smart-large-object stream, so RC20 adds a separate local provider
agent rather than pretending the SQL provider can consume the log.

## Source CDC architecture

- `QMIGRATION_EXPERIMENTAL_GBASE8S_CDC=1` is required in addition to the RC19
  native gate.
- datasource `cdc_url` points to `gbase8scdc://...` or `gbase8scdcs://...`.
- `qmigration-gbase8s-cdc-agent` is bundled and loads a locally built CSDK CDC
  provider plugin.
- QMigration sends no database password to the CDC agent API; source credentials
  stay in the datasource-local provider configuration.
- selected tables receive deterministic IDs used as the CDC `user_data` value.
- unsupported smart BLOB/CLOB/complex types and TRUNCATE fail closed in RC20.

## Crash-safe position model

The durable position type is `GBASE8S_CDC_SEQ` and values are persisted as:

`restart=<earliest-open-BEGIN>;commit=<last-applied-COMMIT>`

This follows the vendor restart model for incomplete transactions. It prevents
loss of a long transaction that began before a later short transaction was
already applied. A restarted reader begins at `restart` and suppresses commits
at or below `commit`.

QMigration still enforces its normal order:

`provider read -> transaction assembly -> durable spool/target apply -> local commit watermark acknowledge`

## Transaction decoding

The provider normalizes BEGIN/COMMIT/ROLLBACK, INSERT/DELETE,
UPDATE_BEFORE/UPDATE_AFTER, DISCARD, TIMEOUT/TABLE_SCHEMA and ERROR records.
QMigration owns transaction buffering, DISCARD rollback effects, 100,000-event /
128 MiB limits, duplicate-commit suppression and restart checkpoint generation.

## Qualification

`qmigration-gbase8s-qualify --cdc` now validates provider health, selected-table
metadata/type prerequisites and creation of a restart checkpoint. Real CSDK
provider build and GBase 8s instance qualification remain required before
production promotion.
