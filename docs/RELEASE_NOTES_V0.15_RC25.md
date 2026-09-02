# QMigration V0.15.0-rc25 Release Notes

## Scope

RC25 turns several highest-priority RC24 gaps into executable native paths:
multi-partition TiCDC/Kafka ordering, Kafka transport security, exact historical
validation snapshots, and a qualification-gated DM8 archived-log source CDC
path. It also fixes the validation-state CDC routing race discovered while
auditing RC24. No external migration runtime is introduced.

## TiCDC multi-partition CDC

- Kafka topic metadata now discovers all partitions and leaders.
- Native fetch is partition-aware.
- Durable `TIDB_TSO` checkpoints support per-partition next offsets:
  `tso=<TSO>;kafka=0:<next0>,1:<next1>,...`.
- RC3 single-partition checkpoints remain readable. A non-zero single-partition
  checkpoint fails closed if the topic later expands because offsets for new
  partitions cannot be inferred safely.
- TiCDC Resolved TS is the global merge fence. QMigration emits a candidate
  commitTs only after every active partition has observed a Resolved TS strictly
  greater than that commitTs.
- Prefetched future records keep their earliest unprocessed offset in the
  checkpoint, preventing restart gaps.

## Kafka security

The QMigration native Kafka client now supports:

- TLS 1.2+;
- system or custom CA trust;
- explicit ServerName verification;
- optional mTLS certificate/key;
- SASL/PLAIN;
- SASL/SCRAM-SHA-256;
- SASL/SCRAM-SHA-512.

TiCDC sink creation receives the matching partition/TLS/SASL settings. Kafka
SASL username/password are injected from runtime environment variables rather
than stored in `cdc_url`. Report sink URIs redact password fields. Unsupported
mechanisms fail before migration.

## Exact TSO validation

- `VALIDATING` now freezes forward target CDC apply but keeps capture active.
- New source transactions during validation are acknowledged only after durable
  CDC spool persistence.
- Empty-spool verification, barrier capture and transition to `VALIDATING` are
  serialized with target apply using the same task/direction lock.
- Connector SPI adds `validation-snapshot`.
- TiDB implements it by opening a fresh SQL session, setting
  `SESSION tidb_snapshot='<barrier TSO>'`, verifying the session value, and using
  that connection for every validation read.
- `QMIGRATION_VALIDATION_REQUIRE_EXACT_WATERMARK=1` can make exact snapshots a
  mandatory policy for a task environment.

This removes the RC24 TiDB validation race with concurrent source writes. RC25
also implements the same SPI for Oracle LogMiner positions (`AS OF SCN`) and DM8
LogMiner positions (`AS OF SCN`, with DM LSN/SCN semantics). Other database
families retain the existing stable-watermark validation contract until their
vendor-specific historical snapshot is implemented.

## Oracle exact SCN validation

- Oracle exposes `validation-snapshot` whenever the LogMiner CDC gate is enabled.
- An `ORACLE_SCN` barrier opens an independent native connector whose table and
  point reads use Oracle Flashback Query `AS OF SCN <barrier>`.
- The snapshot connector is read-only. Missing/expired UNDO history fails closed
  instead of falling back to current rows.

## Dameng / DM8 archived-log source CDC

RC25 adds a separate `QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC=1` gate on top of
the existing native DM data plane. The software path is intentionally retained
as EXPERIMENTAL until real DM8 qualification evidence exists.

- capture position: newest fully archived `DM_LSN` from `V$ARCHIVED_LOG`;
- source reader: `DBMS_LOGMNR` with committed archived-log windows;
- transaction identity/order: LogMiner `XID` + `COMMIT_SCN`;
- row identity: LogMiner `ROW_ID`;
- full row reconstruction: `AS OF SCN commitLSN-1` / `AS OF SCN commitLSN`;
- apply ordering: target transaction + durable QMigration checkpoint before the
  reader acknowledges the next DM_LSN;
- intra-transaction repeated updates are coalesced to their net row effect;
- INSERT->DELETE in one transaction becomes a checkpoint-only net no-op;
- long transactions use COMMIT/XA_COMMIT `START_SCN` to rewind the mining window before row decode;
- archive gaps, missing ROW_ID, selected-table DDL/BATCH_UPDATE/UNSUPPORTED,
  missing PK and flashback-history failures stop before checkpoint advancement;
- `qmigration-dameng-cdc` is wired into the Unified Engine;
- `qmigration-dameng-qualify --cdc` validates prerequisites, an archived DM_LSN,
  selected-table PKs and an exact flashback snapshot.

The RC25 contract requires local archived redo, `ARCH_INI=1`,
`RLOG_APPEND_LOGIC=1..3`, `ENABLE_FLASHBACK=1` and
`RLOG_LLOG_COMPRESS=0`. DM MPP/DPC/topology-specific LogMiner behavior, archive
switch latency, LOB flashback matrices and provider TLS remain qualification
gates rather than production claims.

## UI/documentation consistency

- TiDB datasource guidance no longer claims single-partition/plaintext-only
  Kafka.
- GBase 8s datasource guidance now says Agent API/native C ABI v3 with schema
  fence + capture lineage.

## Remaining production gates

- Real TiDB/TiCDC/Kafka multi-broker TLS/SASL/restart/rebalance/GC soak reports.
- Exact-watermark source snapshots for remaining GTID/LSN/LRI database families.
- Retained Oracle SCN and DM_LSN exact-snapshot qualification.
- Existing connector-specific gaps remain explicit: openGauss/Kingbase CDC,
  GBase 8a CDC/transactional target apply, GBase 8s
  complex LOB historical images, DB2 pureScale edge cases and GaussDB
  multi-primary/hybrid DDL+DML boundaries.

- Transactions that share one `COMMIT_SCN` are aggregated across XIDs into one QMigration target transaction so the durable numeric `DM_LSN` cannot suppress a sibling source transaction.
