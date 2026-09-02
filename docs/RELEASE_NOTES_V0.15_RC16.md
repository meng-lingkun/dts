# QMigration V0.15.0-rc16 Release Notes

RC16 extends the experimental GaussDB CDC path with a correctness-first
DDL-only replay mode while retaining RC15's byte-safe binary DML transport.

## GaussDB DDL-only transaction path

- A text logical-decoding pass enables `enable-ddl-decoding=true` only to
  classify transaction shape and extract normalized `TDDL` statements.
- DML values are never taken from that text pass. QMigration peeks the binary
  decoder again up to the exact same commit LSN and uses the RC15 B/C/I/U/D
  decoder for row values.
- DDL-only and DML-only transactions are merged back into source commit order.
- DDL-only ACK uses `pg_logical_slot_get_changes(..., commit_lsn, ...)`; DML ACK
  continues to use `pg_logical_slot_get_binary_changes`.
- Target apply and durable QMigration checkpoint still happen before either
  source ACK path advances the slot.

## Safe DDL subset

RC16 accepts only one normalized statement per event and only when the affected
object can be unambiguously tied to a selected migration table:

- `ALTER TABLE <selected-table> ...`
- `TRUNCATE [TABLE] <selected-table>`
- `CREATE [UNIQUE] INDEX ... ON <selected-table> ...`

DROP/CREATE TABLE, sequence/routine/trigger/schema DDL, CONCURRENTLY, DDL on an
unselected table and all other statement families remain fail-closed.

## Hybrid transaction boundary

GaussDB documents that logical decoding does not reliably preserve hybrid
DDL/DML transactions: DML following a DDL can be absent from the decoded
stream. QMigration therefore cannot reconstruct such a transaction from the
available source information.

DDL replay requires all of the following:

- `cdc_ddl_mode=SAME_FAMILY`;
- GaussDB source and GaussDB target with identity schema/table mappings;
- worker environment `QMIGRATION_GAUSSDB_DDL_ONLY_TRANSACTIONS=1`;
- source `enable_logical_replication_ddl=on`.

The additional worker flag is deliberately not injected automatically by the
planner; it is an operator assertion about the source workload.

## Deliberate boundaries

- hybrid DDL/DML transactions remain unsupported;
- multi-primary logical decoding remains unadvertised;
- real centralized/distributed GaussDB DDL-only qualification is still required
  before experimental gates can be removed.
