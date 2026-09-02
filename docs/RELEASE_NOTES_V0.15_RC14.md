# QMigration V0.15.0-rc14 Release Notes

RC14 moves GaussDB from the probe-only placeholder path into a qualification-gated QMigration data plane and adds a correctness-first SQL logical-decoding CDC reader.

## GaussDB Full / target data plane

- GaussDB now uses QMigration's PostgreSQL frontend/backend wire connector instead of the generic external/JDBC placeholder.
- Metadata, Full Read, numeric/composite keyset, ordered boundary planning, target Full Write, table/PK/index/FK schema creation, point lookup/delete and transactional target CDC Apply use QMigration-owned logic.
- The native/full capability is exposed only with `QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE=1`.
- TLS/CA/ServerName/mTLS use the existing QMigration PostgreSQL-wire transport path.

## GaussDB SQL logical-decoding CDC

- A second gate, `QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC=1`, exposes source CDC.
- Snapshot start position uses `pg_current_xlog_location()` and durable position type `GAUSSDB_LSN`.
- QMigration creates an explicit **LSN-ordered** `mppdb_decoding` slot with `output_order=0`; it does not rely on CN defaults that may create a CSN-ordered slot.
- The reader uses `pg_logical_slot_peek_changes` so source state is not advanced before target commit.
- Complete BEGIN/COMMIT transactions are decoded from the documented JSON DML envelope.
- After QMigration target Apply and durable checkpoint complete, `pg_logical_slot_get_changes(..., commit_lsn, ...)` advances the slot exactly through the committed source transaction.
- Worker restart therefore resumes from the source slot's last acknowledged LSN rather than an in-memory cursor.
- `upto_nchanges` batches remain transaction-complete; partial batches, XID changes or missing COMMIT records fail closed.
- Table selection uses `white-table-list`; UPDATE/DELETE require a primary key.
- JSON logical decoding of binary/NUL-sensitive families (`bytea`/BLOB/binary/RAW) is deliberately rejected in RC14 pending a byte-safe decoder.
- DDL logical decoding is not enabled in RC14; QMigration's CDC DDL policy remains REJECT for this source path.

## Qualification

- Added `qmigration-gaussdb-qualify` and `deployments/scripts/qualify-gaussdb.sh`.
- `--cdc` checks logical-replication GUCs/permissions, captures the current LSN, validates an optional selected table, creates an LSN-based temporary `mppdb_decoding` slot, then drops it.
- Real GaussDB centralized/distributed release, primary-node, TLS, restart/failover, large-transaction and retention qualification reports are still required before removing experimental gates.

## Safety boundaries

RC14 does not claim GaussDB multi-primary logical decoding, binary/NUL-safe JSON CDC, source DDL replay, or production maturity without retained real-instance qualification.
