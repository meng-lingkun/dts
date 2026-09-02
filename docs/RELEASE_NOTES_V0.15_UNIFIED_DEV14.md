# QMigration V0.15.0-unified-dev14 Release Notes

## Scope

This snapshot advances the QMigration-owned Oracle Native path from a TTC query/cursor qualification layer into an **explicitly gated source data plane**. It adds Data Dictionary metadata, Full Reader primitives and an experimental DBMS_LOGMNR/SCN CDC reader while keeping Oracle target write/schema/DDL capabilities disabled in the Connector capability matrix. No DataX, SeaTunnel, Flink CDC, Debezium, Canal or JDBC migration runtime is introduced.

## Oracle Native source data plane

- Added a reusable authenticated TNS/TTC session owned by the Oracle connector.
- Data Dictionary execution now provides schema/table discovery, columns, primary keys, indexes, foreign keys, partitions and schema-object discovery.
- Added bounded Full Reader batches with numeric range, composite keyset, partition, hash and custom-filter integration through the existing Unified Engine SPI.
- Added ordered NTILE keyset-boundary discovery for non-integer/composite migration keys.
- Full Reader pagination uses an ordered subquery plus outer `ROWNUM` limit, retaining Oracle 11g-compatible syntax instead of requiring `FETCH FIRST`.
- Metadata size estimates no longer require `V$PARAMETER` access; normal discovery does not unnecessarily depend on dynamic-performance-view privileges.
- Source numeric values rendered into qualification SQL are validated as numeric grammar before use; malformed/injection-shaped numeric text fails closed.

## Experimental LogMiner / SCN CDC

- Added `qmigration-oracle-cdc`, integrated with the shared QMigration Native CDC Runtime (`read -> durable apply/checkpoint -> source ACK`).
- Added current-SCN capture from `V$DATABASE`, bounded SCN polling windows and selected-table filtering.
- Added DBMS_LOGMNR startup with online catalog + committed-data-only semantics and explicit archive/online redo-file fallback when Oracle reports a missing-log window.
- Added transaction grouping by Oracle XID + commit SCN and checkpoint-only progress when a scanned SCN range has no selected-table changes.
- Added full-row reconstruction using Flashback Query at commit SCNs for the current experimental path.
- Added `RS_ID` / `SSN` / `CSF` continuation handling so SQL_REDO/SQL_UNDO fragments larger than one LogMiner row are not silently truncated.
- DDL emission is fail-safe: internal Oracle DDL is filtered and only executable user DDL (`STATUS=0`) can enter the CDC event stream.
- Added Reader tests proving SCN does not advance before QMigration apply acknowledgement.

## Capability boundary

Default Oracle capability remains only `protocol-probe`.

`QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE=1` enables **source-side** experimental capabilities:

- metadata
- full-read
- keyset-boundary
- partition
- runtime-load
- schema-object discovery
- point lookup
- migration prechecks

It intentionally does **not** expose `full-write`, `schema-create`, `post-load-schema`, `cdc-apply`, `cdc-transactional-apply` or `ddl-apply`. Those require native bind execution and large BLOB/CLOB DML qualification before they can be advertised safely.

`QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC=1` additionally requires the native gate and exposes `cdc-position` + `cdc-read` for the experimental LogMiner reader.

## Qualification limits

- Real Oracle 11g/12c/19c/21c/23ai E2E qualification is still required before removing the experimental gates.
- Flashback reconstruction depends on sufficient undo history and has edge cases around row movement/partition movement that require real-instance soak coverage.
- Native BLOB/CLOB source reads remain bounded and need version/charset/large-object qualification.
- Oracle target writes remain unavailable through the production Capability SPI in this release.

## Metadata

`030_v015_oracle_native_source_logminer.sql` advances the Metadata Schema marker to `0.15.0-unified-dev14` without adding table columns.
