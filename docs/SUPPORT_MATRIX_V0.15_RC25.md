# QMigration V0.15.0-rc25 Support Matrix

| Database | Metadata | Full Read | Full Write / CDC Apply | Source CDC | Schema / DDL | Status |
|---|---:|---:|---:|---|---:|---|
| MySQL / MariaDB / PolarDB MySQL / PolarDB-X | Yes | Yes | Yes | Native MySQL-compatible binlog paths | Yes | NATIVE / prerequisite-specific |
| TiDB | Yes | Yes + exact TSO validation snapshot | Yes | TiCDC + native Kafka Canal-JSON; multi-partition + TLS/mTLS + SASL PLAIN/SCRAM | Yes | EXPERIMENTAL / qualification required |
| OceanBase MySQL | Yes | Yes | Yes | Binlog Service / ODP | Yes | EXPERIMENTAL |
| PostgreSQL / PolarDB PostgreSQL | Yes | Yes | Yes | pgoutput | Yes | NATIVE |
| openGauss / Kingbase | Yes | Yes | Yes | Not advertised | Yes | NATIVE_FULL_ONLY |
| Oracle | Yes | Yes + exact SCN validation snapshot | Yes | LogMiner / SCN | Yes | EXPERIMENTAL |
| SQL Server | Yes | Yes | Yes | SQL Server CDC / LSN | Yes | EXPERIMENTAL |
| DB2 LUW | Yes | Yes | Yes | QMigration Log Agent + IBM db2ReadLog | Yes | EXPERIMENTAL |
| Dameng / DM8 | Yes | Yes + exact DM_LSN validation snapshot | Yes | DBMS_LOGMNR archived-log CDC / DM_LSN | Table/PK/index/FK target; source DDL fails closed | EXPERIMENTAL / qualification required |
| GaussDB | Yes | Yes | Yes | mppdb_decoding binary DML + optional DDL-only classification / GAUSSDB_LSN | Target yes; selected-table DDL-only same-family replay | EXPERIMENTAL |
| GBase 8a MPP Cluster | Yes | Yes | Full Write only; validated HASH staging+MERGE; no CDC apply | Not advertised | Table/PK create only | EXPERIMENTAL / FULL_ONLY |
| **GBase 8s V8.8** | **Yes** | **Yes** | **Full Write + transactional target CDC Apply** | **syscdcv1/CSDK smart-LOB via native C ABI v3 provider; GBASE8S_CDC_SEQ** | **Target owner check + table/PK/index/FK apply** | **EXPERIMENTAL / qualification required** |

## TiDB RC25 scope

- `kafka_partitions=N` configures and validates the expected TiCDC topic topology.
- Multi-partition consumption persists one next offset per partition in `TIDB_TSO`.
- A transaction TSO is emitted only after every partition has a Resolved TS strictly greater than it.
- Kafka native client supports TLS 1.2+, custom CA, ServerName verification and optional mTLS.
- SASL supports PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512; username/password are runtime environment secrets.
- Full+CDC validation freezes target apply while capture continues to the Durable CDC Spool.
- TiDB source validation opens an exact historical `tidb_snapshot` session at the barrier TSO.
- Real multi-broker/TiCDC restart/rebalance/security/GC soak evidence is still required before maturity promotion.

## Dameng / DM8 RC25 scope

Behind `QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE=1` and
`QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC=1`:

- captures the newest fully archived `DM_LSN` rather than an online LSN that
  `DBMS_LOGMNR` cannot yet mine;
- validates continuous local archive coverage before each mining window;
- COMMIT/XA_COMMIT `START_SCN` automatically rewinds mining for transactions that began before the durable checkpoint;
- uses committed `V$LOGMNR_CONTENTS` records for XID/commit-LSN/ROWID ordering;
- reconstructs complete row images through DM flashback `AS OF SCN`, avoiding
  SQL_REDO literal parsing;
- uses full primary-key row images for heterogeneous target apply;
- freezes validation at the applied `DM_LSN` and reads the source at the same
  historical LSN;
- selected DDL, BATCH_UPDATE, unsupported log operations, archive gaps, missing
  ROWID/PK and expired flashback history fail closed without checkpoint advance.

Still not promoted: real DM8 version/topology/archive-switch/UNDO/LOB soak,
MPP/DPC-specific semantics and provider TLS qualification.

## GBase 8s retained RC24 scope

Behind `QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE=1` and
`QMIGRATION_EXPERIMENTAL_GBASE8S_CDC=1`:

- RC19 CSDK/ODBC Full + target data plane remains unchanged;
- RC20 transaction-aware source CDC remains apply-before-ACK with
  `restart=<earliest-open-BEGIN>;commit=<last-applied-COMMIT>`;
- RC24 requires a Linux native C ABI v3 `.so` (or updated legacy provider) loaded by
  `qmigration-gbase8s-cdc-agent`;
- legacy Go provider plugin remains compatibility-only;
- optional SHA-256 pinning protects the native provider artifact;
- provider calls are serialized and remote agent traffic requires TLS + token;
- provider row images are checked for complete selected columns, order,
  NULL/encoding correctness and response limits;
- empty committed transactions emit CHECKPOINT events so the durable watermark
  continues to advance;
- RC22 supports documented transactional source TRUNCATE and GBase 8s transactional target TRUNCATE replay.
- RC23 schema fingerprints remain mandatory; RC24 additionally requires Agent API v3 plus a 64-hex capture lineage persisted in every durable GBASE8S_CDC_SEQ checkpoint and verified on every read. `/v1/status` and `/metrics` observability are restored.

Fail-closed/not advertised:

- production maturity without retained real GBase 8s/CSDK/syscdcv1 evidence;
- smart BLOB/CLOB source values and unsupported complex/opaque/collection types;
- keyless CDC tables;
- QMigration SQL TLS `PREFERRED/REQUIRED` until CSDK SQL SSL mapping is qualified;
- quoted/case-sensitive identifier behavior outside the safe subset.

- Same-`COMMIT_SCN` source XIDs are aggregated into one target transaction/checkpoint to keep `DM_LSN` replay and duplicate suppression unambiguous.

## Remaining highest-priority gaps

1. Retained TiDB/TiCDC/Kafka multi-partition + TLS/SASL + restart/failover/GC qualification.
2. Add exact historical validation snapshot implementations for the remaining GTID/LSN/LRI families; retain Oracle/DM exact-snapshot qualification.
3. openGauss/Kingbase product-specific CDC; GBase 8a Source CDC/target transactional apply.
4. Retain DM8 archived-log CDC qualification across version/topology/UNDO/LOB cases.
5. GBase 8s smart BLOB/CLOB historical image retrieval; DB2 pureScale/multi-stream; GaussDB multi-primary/hybrid DDL+DML boundaries.
