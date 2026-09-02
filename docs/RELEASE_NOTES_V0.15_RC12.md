# QMigration V0.15.0-rc12 Release Notes

RC12 completes the software-side Db2 VECTOR target path while preserving the
serialized-text contract introduced for source Full/CDC in RC11. DB2 remains
experimental until retained real-instance qualification is complete.

## VECTOR metadata and schema

- Detect Db2 VECTOR columns from `SYSCAT.COLUMNS` and preserve `LENGTH` as the
  VECTOR dimension.
- Query `COORDINATETYPE` only when VECTOR is present, so Db2 11.5 catalog reads
  do not depend on the Db2 12.1.2 catalog extension.
- Normalize target metadata to `VECTOR(dimension,FLOAT32|INT8)`.
- Auto-create DB2 target VECTOR columns with the exact dimension and coordinate
  type instead of degrading them to character data.

## Prepared VECTOR target apply

- Continue carrying VECTOR values as `VECTOR_SERIALIZE()` text through the
  QMigration pipeline.
- Reconstruct the native target value with Db2's documented
  `VECTOR(CAST(? AS CLOB), dimension, coordinate-type)` constructor.
- Reuse the existing Prepared SQLDTA path for small serialized vectors and the
  CLOB/EXTDTA path for large serialized vectors.
- Validate bracket syntax, exact coordinate count, INT8 range and finite FLOAT32
  values before sending target data.
- The same prepared writer is used by Full Load and transactional CDC Apply.

## Qualification

- `qmigration-db2-qualify` adds optional `--target-vector`; the shell wrapper
  exposes `DB2_QUALIFY_TARGET_VECTOR=1`.
- The destructive VECTOR qualifier creates FLOAT32 and INT8 VECTOR columns,
  writes through prepared binds and reads them back through `VECTOR_SERIALIZE()`.
- Synthetic tests cover catalog/type shape, schema DDL, prepared SQL generation,
  value validation and large-vector EXTDTA transport.

## Remaining boundaries

- Db2 VECTOR requires a Db2 release/catalog that supports the type; production
  claims still require retained 12.1.2+ target reports and 12.1.4+ source-CDC
  reports.
- Multi-insert with external values required by more than one row remains
  fail-closed because no documented row ordinal exists in the manager records.
- pureScale multi-log-stream ordering/failover remains unclaimed.
