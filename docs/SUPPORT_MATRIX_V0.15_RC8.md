# QMigration V0.15.0-rc8 Support Matrix

This matrix describes QMigration-owned migration/runtime support. SQL-wire
compatibility or a protocol probe alone does not imply source CDC support.
Vendor client libraries may be required only where the database exposes a
vendor API rather than a SQL/wire log API; RC8 uses this model for Db2
`db2ReadLog` on the source-side QMigration Log Agent host.

| Database | Metadata | Full Read | Full Write / CDC Apply | Source CDC | Schema / DDL | Status |
|---|---:|---:|---:|---|---:|---|
| MySQL | Yes | Yes | Yes | Native Binlog/GTID | Yes | NATIVE |
| MariaDB | Yes | Yes | Yes | Native Binlog | Yes | NATIVE |
| PolarDB MySQL | Yes | Yes | Yes | MySQL-compatible Binlog + precheck | Yes | NATIVE |
| PolarDB-X | Yes | Yes | Yes | global Binlog + precheck | Yes | NATIVE |
| TiDB | Yes | Yes | Yes | TiCDC + native Kafka Canal-JSON / TIDB_TSO | Yes | EXPERIMENTAL; qualification required |
| OceanBase MySQL | Yes | Yes | Yes | Binlog Service via tenant ODP + Binlog V4/GTID | Yes | EXPERIMENTAL; qualification required |
| PostgreSQL | Yes | Yes | Yes | pgoutput + durable slot checkpoint | Yes | NATIVE |
| PolarDB PostgreSQL | Yes | Yes | Yes | pgoutput + durable slot checkpoint | Yes | NATIVE |
| openGauss | Yes | Yes | Yes | Not advertised | Yes | NATIVE_FULL_ONLY |
| Kingbase | Yes | Yes | Yes | Not advertised | Yes | NATIVE_FULL_ONLY |
| Oracle | Yes | Yes | Yes | LogMiner / SCN | Yes | EXPERIMENTAL; qualification required |
| SQL Server | Yes | Yes | Yes | SQL Server CDC / LSN | Yes | EXPERIMENTAL; qualification required |
| **DB2 LUW** | **Yes** | **Yes, EXTDTA LOB** | **Yes, Prepared SQLDTA/EXTDTA** | **QMigration Log Agent + IBM db2ReadLog / DB2_LRI** | **Yes** | **EXPERIMENTAL; qualification required** |
| Dameng / DM | No | No | No | No | No | PROBE_ONLY |
| GaussDB | No | No | No | No | No | PROBE_ONLY |
| GBase | No | No | No | No | No | PROBE_ONLY |

## DB2 RC8 source CDC scope

Implemented:

- QMigration-owned `qmigration-db2-log-agent` on the source-side host;
- a small QMigration C provider calling IBM's supported `db2ReadLog` API;
- `DB2READLOG_FILTER_ON`, so only documented propagatable records are consumed;
- durable `DB2_LRI` capture/resume using `nextStartLRI`;
- `DATA CAPTURE CHANGES` and primary-key prechecks;
- source-local Initialize Table descriptor bootstrap before the migration LRI;
- INSERT / DELETE / UPDATE for ordinary row images;
- documented VALUE COMPRESSION full-row images, including NULL and `COMPRESS SYSTEM DEFAULT`;
- logged out-of-row BLOB/CLOB/DBCLOB and varying data, including chunked and consolidated-value reconstruction;
- Db2 11.5.8+ serialized XML CSL records when `DB2_DCC_XML_SERIALIZE=YES`;
- transaction TID aggregation, child/subtransaction merge, COMMIT and ABORT;
- target Apply -> durable checkpoint -> reader ACK ordering;
- checkpoint-only safe advancement for windows with no selected open transaction;
- 100,000 events / 128 MiB per transaction and 10,000 open-transaction limits;
- Log Agent HTTP/TLS and optional Bearer token.

Fail-closed boundaries in RC8:

- partial/ambiguous VALUE COMPRESSION row images not proven to be complete full-row images;
- `NOT LOGGED` LOB columns (rejected before capture) and LOB operations that cannot yield a complete after-image;
- multi-insert log records;
- committed transactions requiring compensation/savepoint/undo net-effect reconstruction;
- selected-table Data Manager actions not explicitly decoded;
- non-UTF8 row character bytes that cannot be losslessly interpreted by the current decoder;
- Db2 pureScale multi-log-stream ordering until retained qualification proves the ordering model.

Operational dependency: the Go Server/Worker does **not** link IBM libraries. The
source-side DB2 Log Agent host needs IBM Data Server Client/Runtime headers and
`libdb2` to build/run `qmigration-db2-readlog-provider`.

## Remaining highest-priority gaps

1. Retained DB2 LUW 11.5 / 12.1 real-instance source-CDC qualification.
2. DB2 multi-insert and compensation/savepoint/undo net-effect reconstruction.
3. Dameng Native Connector.
4. GaussDB Native Connector.
5. GBase Native Connector.
