# QMigration Unified Engine Feature Matrix

| 能力 | V0.15.0-rc29 | 实现方式 |
|---|---|---|
| 单一迁移内核 | DONE | Server 只注册 `qmigration` |
| 第三方迁移 Runtime | REMOVED | 不执行 DataX/SeaTunnel/Flink CDC/Debezium/Canal |
| Full Pipeline | DONE | Reader → bounded channel → Transform → Writer → durable checkpoint |
| Range / Keyset / HASH / PARTITION / CUSTOM | DONE | QMigration Planner + Connector SPI |
| Dynamic Chunk / Backpressure | DONE | pending refinement + control-plane AIMD batch target + adaptive parallelism + CDC-spool predictive throttling |
| Durable CDC Spool | DONE | encrypted file/shared-fs/S3 + integrity/reconcile/lease |
| Validation / Repair | DONE | atomic target-freeze barrier + rowcount/checksum + chunk repair; TiDB TSO + Oracle SCN + DM_LSN exact snapshots |
| Cutover / Reverse / Rollback | DONE | unified state machine |
| MySQL / MariaDB Full + Binlog CDC | DONE | QMigration MySQL wire + native Binlog reader |
| PolarDB MySQL Full + Binlog CDC | DONE / PRECHECK | MySQL-compatible wire/binlog with runtime prerequisite checks |
| PolarDB-X Full + global Binlog CDC | DONE / PRECHECK | MySQL-compatible global Binlog path |
| TiDB Full + TiCDC | SOFTWARE COMPLETE / EXPERIMENTAL | MySQL-wire Full + TiCDC OpenAPI + native Kafka Canal-JSON; multi-partition Resolved-TS merge, per-partition durable offsets, Kafka TLS/mTLS + PLAIN/SCRAM SASL, exact TSO validation snapshot; qualification required |
| OceanBase MySQL Full + CDC | EXPERIMENTAL | MySQL-wire Full + explicit tenant ODP/Binlog Service `cdc_url`, MySQL Binlog V4/GTID; real qualification required |
| PostgreSQL / PolarDB PG Full + pgoutput | DONE | QMigration PostgreSQL wire + logical replication |
| openGauss Source CDC | EXPERIMENTAL | PostgreSQL-wire Full/target + mppdb_decoding SQL logical DML / OPENGAUSS_LSN; binary/DDL not advertised |
| KingbaseES Source CDC | EXPERIMENTAL | PostgreSQL-wire Full/target + sys_* logical slots + kboutput / KINGBASE_LSN; retained kboutput wire qualification required |
| Oracle Native | SOFTWARE COMPLETE / EXPERIMENTAL | TNS/TCPS/TTC Full + target + LogMiner + exact `AS OF SCN` validation snapshot; qualification gate retained |
| SQL Server Native | SOFTWARE COMPLETE / EXPERIMENTAL | TDS/TLS Full + target + CDC/LSN + schema objects/IDENTITY; qualification gate retained |
| DB2 LUW Native | FULL + SOURCE CDC EXPERIMENTAL | DRDA/DDM Full/target + source-side QMigration Log Agent calling IBM `db2ReadLog`; DB2_LRI checkpoint; ordinary/value-compressed/LOB/XML row DML plus multi-insert, row compensation, indirect/decomposed updates; RC11 adds uniquely-owned out-of-row multi-insert and VECTOR source serialization; RC12 adds native VECTOR target metadata/DDL/prepared apply; ambiguous multi-row ownership/pureScale fail closed |
| Dameng / DM8 | FULL + SOURCE CDC EXPERIMENTAL | QMigration catalog/keyset/schema/prepared DML + vendor SQL transport; RC25 DBMS_LOGMNR archived-log CDC / DM_LSN + flashback full-row reconstruction + exact DM_LSN validation snapshot |
| GaussDB | FULL + SOURCE CDC EXPERIMENTAL | PostgreSQL-wire Full/target + mppdb_decoding binary DML / GAUSSDB_LSN; RC16 optional safe DDL-only same-family replay with explicit source-policy gate; hybrid DDL/DML and multi-primary not advertised |
| GBase 8a MPP | FULL + TARGET CDC APPLY EXPERIMENTAL | native packet Metadata/Full Read + EXPRESS HASH target Full Write; RC27 optionally exposes retry-idempotent HASH staging+MERGE/delete target CDC apply via `QMIGRATION_EXPERIMENTAL_GBASE8A_TARGET_CDC=1`; source CDC and transactional target apply remain unadvertised |
| GBase 8s V8.8 | FULL + SOURCE CDC EXPERIMENTAL | CSDK/ODBC Full+target plus datasource-local syscdcv1 smart-LOB CDC; RC28 requires Agent API/native C ABI v4; RC23 schema-fence + RC24 capture-lineage remain mandatory, and optional smart BLOB/CLOB CDC requires `cdc-event-owned-lob-v1` proof (length + SHA-256 + acquisition) on every non-NULL LOB image; TRUNCATE supported; unproved/oversize/complex LOB values fail closed |
| CDC Chaos Qualification | DONE | deterministic error failpoints + RC29 real child-process SIGKILL at spool-before-ACK and target-COMMIT-before-checkpoint boundaries; file-spool I/O crash windows; disabled by default |
| Connector support API | DONE | `/api/v1/connectors`: capabilities + maturity + qualification flag |

`DONE` means the path is executed by QMigration-owned code. Experimental means the software path exists but must not be promoted to a production compatibility claim until real-instance qualification is retained.

See `SUPPORT_MATRIX_V0.15_RC29.md` for the database-by-database matrix.
