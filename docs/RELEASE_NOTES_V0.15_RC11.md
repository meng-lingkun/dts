# QMigration V0.15.0-rc11 Release Notes

RC11 continues the correctness-first Db2 LUW source-CDC work from RC10. It
extends out-of-row reconstruction into the documented DMS 167 multi-insert path
where ownership is provably unambiguous, and adds a source-side Db2 12.1.4+
VECTOR serialized-value path. DB2 source CDC remains experimental until retained
real-instance qualification is complete.

## Out-of-row multi-insert

- Scan every row description before decoding the multi-insert record.
- Determine which rows declare out-of-row LOB/XML/VECTOR fields from the live
  table descriptor and row headers.
- If exactly one row requires external data, attach the pending Start Out-of-Row
  group only to that row and decode all rows normally.
- If two or more rows require external data, fail closed: the documented manager
  records do not expose a multi-insert row ordinal, so assigning bytes would be
  ambiguous.
- If an external group exists but no row declares an external value, fail closed
  instead of silently discarding the group.
- Preserve per-row RID identity and existing transaction event/byte limits.

## Db2 VECTOR source values

- Recognize Data Manager function 213 VECTOR records introduced for Db2 12.1.4+.
- Decode the documented column id, serialized length and serialized vector text.
- Require VECTOR records to be inside a Start Out-of-Row group for the same
  selected table and byte order.
- Merge the serialized value into the following INSERT/UPDATE or uniquely-owned
  multi-insert row.
- NULL VECTOR values do not require a function-213 record.
- Reject malformed UTF-8/non-bracketed values, conflicting duplicate column
  values, orphan VECTOR records and missing required serialized values.
- Full Read now uses `VECTOR_SERIALIZE(column)` so Full and CDC expose the same
  serialized source representation.

## Target boundary

RC11 does not claim DB2 VECTOR target support. Target table auto-create and
prepared writes fail closed for VECTOR columns until QMigration has retained
metadata for dimension/coordinate type plus native target parameter encoding.
This prevents accidental fallback to VARCHAR semantics.

## Remaining qualification gates

- retained DB2 LUW 11.5/12.1 real-instance Full + CDC reports;
- real out-of-row multi-insert traces, including large LOB/XML combinations;
- Db2 12.1.4+ VECTOR source Full/CDC traces and restart/cutover behavior;
- multi-insert records where more than one row requires external data;
- remaining decomposed-update compensation ordering;
- pureScale multi-log-stream ordering and failover.
