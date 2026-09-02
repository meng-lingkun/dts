# QMigration V0.15.0-rc15 Release Notes

RC15 hardens the experimental GaussDB source-CDC path by replacing the RC14
JSON value envelope with GaussDB's documented binary logical-decoding SQL
functions. The durable position and apply-before-source-ACK contract remain
`GAUSSDB_LSN` based.

## GaussDB byte-safe logical decoding

- Capture now uses `pg_logical_slot_peek_binary_changes` and source ACK uses
  `pg_logical_slot_get_binary_changes`.
- QMigration decodes the documented big-endian B/C/I/U/D frame protocol rather
  than parsing private WAL records.
- The frontend text protocol transports the returned `bytea` as explicit hex
  via `encode(data,'hex')`, so the original binary frame is unambiguous.
- Length-delimited tuple values preserve embedded NUL and non-UTF8 bytes and
  distinguish SQL NULL from a non-NULL zero-length value.
- PostgreSQL-compatible `bytea` OID 17 values are decoded from their `\\x...`
  representation into byte-preserving QMigration CDC fields.
- Other values containing NUL or invalid UTF-8 are transported as base64 CDC
  fields rather than being lossy-coerced to text.

## Transaction and restart semantics

- `pg_logical_slot_peek_binary_changes` remains non-advancing capture.
- QMigration validates BEGIN/COMMIT/XID/LSN relationships and complete
  transaction boundaries before returning a transaction to the runtime.
- Target transaction commit and durable QMigration checkpoint still occur
  before `pg_logical_slot_get_binary_changes(..., commit_lsn, ...)` advances
  the source slot.
- Existing 100,000-event / 128-MiB transaction bounds remain enforced.

## Qualification

`qmigration-gaussdb-qualify --cdc` now creates a temporary LSN-based
`mppdb_decoding` slot and invokes the same binary peek path used by the worker,
then drops the slot. Real-instance qualification is still required before the
experimental gates can be removed.

## Deliberate boundaries

Source DDL decoding remains disabled (`enable-ddl-decoding=false`) in RC15.
GaussDB exposes documented DDL decoding, but QMigration's current target apply
contract deliberately refuses DDL and row events in the same target
transaction. RC15 therefore does not weaken transaction atomicity merely to
advertise DDL support. Multi-primary logical decoding also remains outside the
qualified scope.
