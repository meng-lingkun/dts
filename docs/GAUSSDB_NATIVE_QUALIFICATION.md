# GaussDB Native Qualification

## RC16 scope

QMigration V0.15.0-rc16 provides a qualification-gated GaussDB data plane over
the PostgreSQL frontend/backend protocol. Metadata, Full Load planning/read/
write, schema creation and target CDC Apply are owned by QMigration.

Source CDC is separately gated and uses GaussDB's documented
`mppdb_decoding` **binary** SQL logical-decoding functions. QMigration does not
parse private WAL formats.

### Gates

```bash
export QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE=1
export QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC=1
```

The second gate requires the first.

## Source CDC contract

RC16 DML uses:

- `pg_current_xlog_location()` -> durable `GAUSSDB_LSN` position;
- `pg_create_logical_replication_slot(slot,'mppdb_decoding',0)` -> explicit
  LSN-ordered durable slot;
- `pg_logical_slot_peek_binary_changes` -> non-advancing transaction capture;
- documented big-endian B/C/I/U/D frames and length-delimited tuples;
- `encode(data,'hex')` over the PostgreSQL text frontend to preserve the exact
  returned `bytea` frame;
- `pg_logical_slot_get_binary_changes(..., commit_lsn, ...)` -> source ACK only
  after target commit and durable QMigration checkpoint;
- default DML capture keeps `enable-ddl-decoding=false`; optional RC16 DDL-only replay adds a second text classification pass while keeping DML values on the binary path.

The binary tuple path preserves SQL NULL versus non-NULL empty values and can
transport embedded NUL/non-UTF8 values without JSON truncation. `bytea` values
are restored from their documented `\\x` hex representation before they enter
QMigration CDC fields.


## RC16 DDL-only qualification boundary

GaussDB documents that hybrid DDL/DML transactions are not fully decodable: a
DML statement following DDL can be absent from the logical stream. Therefore
QMigration does not and cannot infer that a decoded DDL-only transaction was
safe if the source application is allowed to mix DDL and DML explicitly.

Before enabling DDL replay, the operator must establish and retain evidence that
migration-time DDL runs in independent transactions and set:

```bash
export QMIGRATION_GAUSSDB_DDL_ONLY_TRANSACTIONS=1
```

The migration task must also use `cdc_ddl_mode=SAME_FAMILY`, the source and
target must both be GaussDB with identity schema/table mappings, and the source
must report `enable_logical_replication_ddl=on`.

RC16 replays only selected-table `ALTER TABLE`, `TRUNCATE`, and
`CREATE [UNIQUE] INDEX ... ON ...`. Qualification should exercise each enabled
statement class on a disposable selected table and verify:

1. text peek returns normalized `TDDL` inside one BEGIN/COMMIT;
2. binary peek to the same commit LSN contains no row event for that transaction;
3. the target DDL succeeds before source ACK;
4. worker termination after target apply but before ACK is idempotently recoverable;
5. any decoded transaction containing both DDL and DML is rejected without slot advancement.

Do not qualify or enable mixed DDL/DML transactions; this is a source decoder
limitation, not a target apply limitation.

## Prerequisites

- `wal_level=logical`;
- logical replication slot and WAL sender capacity greater than zero;
- user has SYSADMIN, REPLICATION, or inherited `gs_role_replication` permission;
- run against a node/endpoint supporting the documented logical-decoding SQL functions;
- every selected CDC table has a primary key;
- non-multi-primary logical decoding for this qualification release.

## One-command qualification

```bash
export GAUSSDB_HOST=10.0.0.10
export GAUSSDB_PORT=8000
export GAUSSDB_USER=qmigration
export GAUSSDB_PASSWORD='***'
export GAUSSDB_DATABASE=app
export GAUSSDB_SCHEMA=public
export GAUSSDB_TABLE=orders

# Read-only Full/metadata qualification
deployments/scripts/qualify-gaussdb.sh

# Create/drop a temporary LSN-based slot and execute the actual binary peek path
GAUSSDB_QUALIFY_CDC=1 deployments/scripts/qualify-gaussdb.sh
```

Optional TLS settings remain `GAUSSDB_TLS_MODE`, `GAUSSDB_TLS_SERVER_NAME`,
`GAUSSDB_TLS_CA_FILE`, `GAUSSDB_TLS_CERT_FILE`, and `GAUSSDB_TLS_KEY_FILE`.
The JSON report never contains the password. Set `GAUSSDB_QUALIFY_OUTPUT` to
retain it.

## Qualification exit criteria

Before removing experimental gates, retain reports for each supported GaussDB
deployment family and verify:

1. Metadata and Full Read on representative numeric, temporal, text/LOB, binary and composite-key tables.
2. Target prepared Full Write and transactional CDC Apply on supported target types.
3. LSN-based slot create/drop plus binary peek/get functions on the intended endpoint.
4. INSERT/UPDATE/DELETE grouping, multi-row/empty/long transactions, NULL versus empty values, NUL, non-UTF8 and `bytea` payloads.
5. Worker kill/restart between peek, target commit/checkpoint and source slot ACK; no loss and idempotent replay.
6. Slot retention/backpressure during long Full snapshots and target outages.
7. TLS and failover/topology behavior for each supported service release.
8. DDL-only replay is retained-qualified only under an explicit independent-DDL-transaction source policy; hybrid DDL/DML remains unsupported because the source decoder can omit statements.
9. Multi-primary logical decoding remains out of scope until separately qualified.
