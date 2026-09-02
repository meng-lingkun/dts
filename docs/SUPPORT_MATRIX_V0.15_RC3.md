# QMigration V0.15.0-rc3 Support Matrix

This matrix describes QMigration-owned runtime support. SQL wire compatibility
alone does not imply source CDC support.

## Maturity levels

- `NATIVE`: enabled QMigration-native path for advertised capabilities.
- `NATIVE_FULL_ONLY`: Full Load/target apply exists; source CDC is not advertised.
- `EXPERIMENTAL`: software path exists but requires retained real-instance qualification.
- `PROBE_ONLY`: migration planning must reject the datasource.

## Database matrix

| Database | Metadata | Full Read | Full Write / CDC Apply | Source CDC | Schema / DDL | Status |
|---|---:|---:|---:|---|---:|---|
| MySQL | Yes | Yes | Yes | Native Binlog/GTID | Yes | NATIVE |
| MariaDB | Yes | Yes | Yes | Native Binlog | Yes | NATIVE |
| PolarDB MySQL | Yes | Yes | Yes | MySQL-compatible Binlog + precheck | Yes | NATIVE |
| PolarDB-X | Yes | Yes | Yes | MySQL-compatible global Binlog + precheck | Yes | NATIVE |
| TiDB | Yes | Yes | Yes | **TiCDC OpenAPI + QMigration Native Kafka Canal-JSON / TIDB_TSO** | Yes | EXPERIMENTAL; qualification required |
| OceanBase MySQL | Yes | Yes | Yes | Not advertised on SQL endpoint; Binlog Service adapter pending | Yes | NATIVE_FULL_ONLY |
| PostgreSQL | Yes | Yes | Yes | pgoutput + durable slot checkpoint | Yes | NATIVE |
| PolarDB PostgreSQL | Yes | Yes | Yes | pgoutput + durable slot checkpoint + precheck | Yes | NATIVE |
| openGauss | Yes | Yes | Yes | Not advertised | Yes | NATIVE_FULL_ONLY |
| Kingbase | Yes | Yes | Yes | Not advertised | Yes | NATIVE_FULL_ONLY |
| Oracle | Yes | Yes | Yes | LogMiner / SCN | Yes | EXPERIMENTAL; software-complete, qualification required |
| SQL Server | Yes | Yes | Yes | SQL Server CDC / LSN | Yes | EXPERIMENTAL; software-complete, qualification required |
| DB2 | No | No | No | No | No | PROBE_ONLY |
| Dameng / DM | No | No | No | No | No | PROBE_ONLY |
| GaussDB | No | No | No | No | No | PROBE_ONLY |
| GBase | No | No | No | No | No | PROBE_ONLY |

## TiDB RC3 details

- SQL endpoint: QMigration MySQL-wire Metadata / Full Reader / Full Writer / Schema / CDC target apply.
- CDC endpoint: `cdc_url=ticdc://<ticdc>:8300?brokers=<kafka>:9092,...`.
- Capture position: TiDB current TSO.
- Transport: TiCDC -> Kafka Canal-JSON with TiDB extension -> QMigration native Kafka consumer.
- Ordering: one partition in RC3.
- Transaction boundary: consecutive identical `commitTs`.
- Durable checkpoint: `tso=<TSO>;kafka=<nextOffset>`.
- ACK order: target atomic apply + durable checkpoint, then reader ACK.
- Qualification: `qmigration-tidb-qualify` / `TIDB_TICDC_QUALIFICATION.md`.

## Shared native engine capabilities

- RANGE / bounded composite KEYSET / HASH / PARTITION / controlled CUSTOM predicates.
- Dynamic chunk refinement, Worker lease/failover, retry/resume, durable cursors.
- Bounded pipeline, adaptive batch/parallelism and database/Worker backpressure.
- Transform-policy DSL and connector-neutral lossless row images.
- Encrypted CDC spool: file/shared-fs/S3, integrity/reconciliation/multipart/drain lease.
- Transactional CDC apply, apply-before-source-ACK, checkpoint-only progress, DDL policy and DLQ/conflict handling.
- Validation watermark barrier, row-count/checksum, repair, catch-up, cutover, reverse sync and rollback.
- Universal Schema Model, PK/index/FK post-load and safe same-family schema objects.
- TLS/mTLS where each native protocol path advertises it, encrypted credentials, RBAC/audit/Prometheus/WebSocket/Kubernetes.

## Remaining GA gaps

1. TiDB/TiCDC/Kafka real-instance qualification; Kafka TLS/SASL and multi-partition remain unclaimed in RC3.
2. OceanBase Binlog Service Native source adapter.
3. Oracle real-instance version/NLS/TCPS/RAC qualification.
4. SQL Server version/encryption/CDC retention/failover qualification.
5. Native DB2, Dameng, GaussDB and GBase connectors.
6. Cross-family stored procedure/function/trigger conversion remains explicit/manual.
