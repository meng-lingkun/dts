# QMigration V0.15.0-rc29 Support Matrix

| Database | Metadata | Full Read | Full Write / CDC Apply | Source CDC | Schema / DDL | Status |
|---|---:|---:|---:|---|---:|---|
| MySQL / MariaDB / PolarDB MySQL / PolarDB-X | Yes | Yes | Yes | Native MySQL-compatible binlog paths | Yes | NATIVE / prerequisite-specific |
| TiDB | Yes | Yes + exact TSO validation snapshot | Yes | TiCDC + native Kafka Canal-JSON; multi-partition + TLS/mTLS + SASL PLAIN/SCRAM | Yes | EXPERIMENTAL / qualification required |
| OceanBase MySQL | Yes | Yes | Yes | Binlog Service / ODP | Yes | EXPERIMENTAL |
| PostgreSQL / PolarDB PostgreSQL | Yes | Yes | Yes | pgoutput / LSN | Yes | NATIVE |
| **openGauss** | **Yes** | **Yes** | **Yes** | **mppdb_decoding SQL logical CDC / OPENGAUSS_LSN** | Target yes; source DDL not advertised | **EXPERIMENTAL CDC / qualification required** |
| **KingbaseES** | **Yes** | **Yes** | **Yes** | **sys_* logical slots + kboutput / KINGBASE_LSN** | Target yes; publication DML only | **EXPERIMENTAL CDC / kboutput wire qualification required** |
| Oracle | Yes | Yes + exact SCN validation snapshot | Yes | LogMiner / SCN | Yes | EXPERIMENTAL |
| SQL Server | Yes | Yes | Yes | SQL Server CDC / LSN | Yes | EXPERIMENTAL |
| DB2 LUW | Yes | Yes | Yes | QMigration Log Agent + IBM db2ReadLog | Yes | EXPERIMENTAL |
| Dameng / DM8 | Yes | Yes + exact DM_LSN validation snapshot | Yes | DBMS_LOGMNR archived-log CDC / DM_LSN | Table/PK/index/FK target; source DDL fails closed | EXPERIMENTAL / qualification required |
| GaussDB | Yes | Yes | Yes | mppdb_decoding binary DML + optional DDL-only classification / GAUSSDB_LSN | Target yes; selected-table DDL-only same-family replay | EXPERIMENTAL |
| GBase 8a MPP Cluster | Yes | Yes | Full Write + optional retry-idempotent target CDC Apply; no transactional atomicity claim | Not advertised | Table/PK create only | EXPERIMENTAL / target CDC qualification required |
| GBase 8s V8.8 | Yes | Yes | Full Write + transactional target CDC Apply | syscdcv1/CSDK via native C ABI v4; optional event-owned smart BLOB/CLOB proof / GBASE8S_CDC_SEQ | Target owner check + table/PK/index/FK apply | EXPERIMENTAL / qualification required |


## RC29 shared-engine reliability/performance boundary

- Database capability rows are unchanged from RC28; RC29 hardens the shared migration engine rather than claiming new vendor compatibility.
- `qmigration-chaos-qualify` now includes two real child-process SIGKILL scenarios in addition to the eight deterministic RC28 cases.
- Full+CDC task flow control consumes CDC spool backlog bytes, storage level, backlog growth rate and projected critical ETA; it may reduce Full parallelism before storage reaches WARN/CRITICAL.
- Batch feedback is control-plane driven with bounded AIMD-style target changes and remains subordinate to database/Worker/spool WARN/CRITICAL caps.
- file-backed spool I/O faults before write or between payload persistence and Metadata commit fail without source ACK; orphan payloads are reconciled on restart.
- The new controls do not change CDC ordering/checkpoint semantics and do not promote any EXPERIMENTAL connector to NATIVE.

## RC26 openGauss boundary

- Product path: PostgreSQL-wire Full/target + `mppdb_decoding` SQL source CDC.
- Durable position: `OPENGAUSS_LSN`.
- Transaction order: peek complete transaction -> target apply/durable checkpoint -> source slot advance.
- Selected UPDATE/DELETE tables require a deterministic primary key.
- JSON logical decoding rejects binary/NUL-sensitive families and malformed or partial transactions.
- Production maturity requires retained openGauss version/topology/SSL/restart/failover traces.

## RC26 KingbaseES boundary

- Product path: PostgreSQL-wire Full/target + Kingbase `sys_*` logical slot APIs.
- Output plugin: `kboutput`; QMigration intentionally does not create a `pgoutput` slot.
- Durable position: `KINGBASE_LSN`.
- `sys_publication` is used to retain selected-table publication membership.
- Slot plugin identity is checked before every managed stream connection.
- The current strict binary decoder must be retained-qualified against the claimed KingbaseES versions; unknown `kboutput` messages fail closed.
- Source DDL/TRUNCATE and exact-at-LSN validation snapshots are not advertised.

## RC27 GBase 8a target CDC boundary

- Enable with both `QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE=1` and `QMIGRATION_EXPERIMENTAL_GBASE8A_TARGET_CDC=1`.
- INSERT/UPDATE replay uses the existing validated HASH staging table + `MERGE`; DELETE uses the mapped stable key.
- Retry after a lost ACK/checkpoint response is idempotent at the event/position level.
- `cdc-transactional-apply` is intentionally **not** advertised: a multi-event source transaction may be transiently visible event-by-event on GBase 8a MPP.
- Source CDC remains unadvertised: public cluster sync/rsynctool and audit SQL are not treated as a generic exact row-change feed.

## RC27 CDC chaos qualification

- Deterministic failpoints cover spool persist, target apply, spool mark, checkpoint and source-ACK boundaries.
- `qmigration-chaos-qualify` proves replay/duplicate-suppression invariants without an external database.
- Fault injection is off by default and requires explicit `QMIGRATION_ENABLE_FAULT_INJECTION=1`.

## Remaining highest-priority gaps

1. Retained openGauss mppdb_decoding and Kingbase kboutput restart/failover/version qualification; broaden safe binary/value families only with retained evidence.
2. Exact historical validation snapshots for remaining GTID/LSN/LRI families where the vendor can provide a provably equivalent historical read.
3. GBase 8a Source CDC/transactional target atomicity remains unavailable; GBase 8s smart BLOB/CLOB software contract is implemented but still requires retained real CSDK historical-image qualification and bounded-response sizing.
4. DB2 pureScale/multi-log-stream ordering and GaussDB multi-primary/hybrid DDL+DML boundaries.
5. Long-running 10–40 TB external vendor/network/filesystem soak qualification; RC29 adds real local-process SIGKILL and predictive spool-aware Full throttling, but external proxy/ENOSPC qualification remains required.

## RC28 GBase 8s smart-LOB boundary

- Requires Agent API/native provider ABI v4.
- Optional BLOB/CLOB source CDC additionally requires `QMIGRATION_EXPERIMENTAL_GBASE8S_SMART_LOB_CDC=1`.
- Provider must attest `cdc-event-owned-lob-v1` and provide per-field exact length/SHA-256/acquisition proof.
- Current-row SELECT reconstruction, missing/corrupt proof, unsupported type, or oversize complete-record response fails closed before target apply.
- Real CSDK historical-locator qualification is still required before production promotion.

## RC28 generic commit-uncertain boundary

For transactional CDC targets, QMigration persists a durable pre-COMMIT ambiguity fence containing the exact retained source transaction before sending target COMMIT. A COMMIT response error or process death before the durable source checkpoint leaves `COMMIT_UNCERTAIN`, blocks later ordered CDC, forbids automatic replay, and requires explicit COMMITTED/NOT_COMMITTED operator resolution. NOT_COMMITTED enters `REPLAY_REQUIRED`; if its controlled replay fails, later source flow remains blocked until explicit replay succeeds. Prometheus exposes both `qmigration_cdc_commit_uncertain` and `qmigration_cdc_replay_required`.
