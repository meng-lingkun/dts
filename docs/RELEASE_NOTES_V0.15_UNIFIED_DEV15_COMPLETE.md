# QMigration V0.15.0-unified-dev15-complete Release Notes

## Scope

This snapshot closes the **QMigration-owned Oracle Native software data plane** in one development increment instead of splitting target support across later dev releases. Oracle source metadata/full-load, target full-write/schema/DDL, transactional CDC apply and LogMiner CDC are now implemented behind explicit experimental gates. QMigration still does not depend on DataX, SeaTunnel, Flink CDC, Debezium, Canal, JDBC or OCI as a migration runtime.

The code is software-complete for the current Connector SPI, but it is **not claimed as production-qualified against every Oracle release/environment** until the real-instance matrix in `docs/ORACLE_NATIVE_QUALIFICATION.md` is executed.

## Native TTC transport and SQL runtime

- Added native TTC scalar input-bind metadata and value codecs.
- Added exact Oracle NUMBER encoding without float64 precision loss.
- Added OALL8 parse + execute + bind for DML and anonymous PL/SQL.
- Added TTC array binding for homogeneous Full Writer batches.
- Added prepared-cursor re-execution and one-time ORA-01001 invalid-cursor recovery.
- Removed the old literal Full Writer fallback; target row data is no longer concatenated into DML SQL.
- TNS DATA writes now fragment large TTC streams across multiple TNS packets.
- SQL/fetch/LOB response decoders now reassemble a TTC item split across arbitrary TNS DATA packet boundaries while retaining coalesced-message support.
- Logical TTC request/response safety limit is 256 MiB per operation.

## Oracle Native Full Writer

- Keyed rows use bind-safe `MERGE` for idempotent migration/CDC upsert behavior.
- Consecutive compatible scalar rows use array binds, then reuse the parsed cursor on later batches.
- Numeric, boolean, RAW, DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE, strings, BLOB and CLOB write families are supported by the native writer.
- Keyed BLOB/CLOB values use `EMPTY_BLOB()` / `EMPTY_CLOB()` followed by bound `DBMS_LOB.WRITEAPPEND` chunks under `SELECT ... FOR UPDATE`.
- Keyless large BLOB/CLOB rows no longer fail closed: QMigration builds a bound anonymous PL/SQL block, creates a temporary LOB, appends 16 KiB chunks, inserts the row, then frees the temporary LOB.
- CLOB chunk splitting is UTF-8 boundary safe.
- Full Writer commits once per batch unless an outer CDC transaction is active; failures roll back the local batch.

## Oracle target schema and CDC apply

With `QMIGRATION_EXPERIMENTAL_ORACLE_TARGET=1` together with the native gate, Oracle now advertises:

- `full-write`
- `schema-create`
- `post-load-schema`
- `cdc-apply`
- `cdc-transactional-apply`
- `ddl-apply`

Implemented target primitives include table creation with composite primary keys, index creation, foreign-key creation, same-family DDL execution, bind-safe deletes, explicit CDC begin/commit/rollback and point lookup for conflict handling.

## Oracle source and LogMiner

The dev14 source path remains part of this complete snapshot:

- Data Dictionary metadata and schema-object discovery
- Full Reader with range/keyset/hash/partition/custom paths
- Oracle 11g-compatible ordered outer-ROWNUM batching
- BLOB/CLOB locator materialization over TTC
- Current SCN, bounded LogMiner windows, selected-table filtering
- XID + commit-SCN transaction assembly
- `RS_ID` / `SSN` / `CSF` continuation coalescing
- internal-DDL filtering and executable user-DDL handling
- Flashback Query row reconstruction
- durable apply-before-ACK integration through the shared Native CDC Runtime

## Capability gates

Default Oracle remains `protocol-probe` only.

- `QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE=1` enables Oracle source Metadata / Full Reader / keyset / partition / runtime-load / schema-object / point-lookup / migration-precheck capabilities.
- `QMIGRATION_EXPERIMENTAL_ORACLE_TARGET=1` additionally enables Oracle target Full Writer / schema / post-load DDL / transactional CDC apply. It requires the native gate.
- `QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC=1` additionally enables Oracle SCN position + LogMiner CDC Reader. It requires the native gate.

The gates remain intentional until real Oracle qualification is complete; code presence is not treated as evidence that every server patchset, NLS combination or managed-service restriction has been exercised.

## Deliberate safety/policy boundaries

- TTC logical request/response size is bounded at 256 MiB to avoid unbounded worker-memory growth.
- Oracle schema means database user; QMigration validates that the target schema/user exists rather than silently creating database users and privileges.
- Oracle sequence runtime-state synchronization is not auto-advertised: unlike PostgreSQL `setval`, deriving/resetting an Oracle cached sequence exactly can advance or otherwise change source semantics. Sequence/view/trigger/routine handling continues to follow the platform schema-object safety policy.
- Cross-family DDL conversion remains a schema-conversion responsibility; automatic DDL replay is restricted by the existing same-family/policy gates.

## Verification in this snapshot

- Oracle fake-wire tests cover bind metadata/data, exact NUMBER encoding, array bind, prepared re-execute, large keyless LOB PL/SQL, multi-packet outbound TTC, split inbound TTC, fetch continuation, OER summary, ROWID and LOB streams.
- Full backend `go test ./...` passes.
- Full backend `go vet ./...` passes.
- Archive generation must clean-restore both the dev14 incremental patch and formal V0.13 cumulative patch to a byte-equivalent source tree.

## Metadata

`031_v015_oracle_native_complete.sql` advances the Metadata Schema marker to `0.15.0-unified-dev15-complete` without adding persistent columns.
