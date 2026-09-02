# QMigration V0.15.0-rc2 Support Matrix

This matrix describes **QMigration-owned runtime support**, not generic SQL/protocol
compatibility. A connector is marked source-CDC capable only when QMigration can
capture a durable source position and stream changes from the configured source
endpoint without falling back to DataX, SeaTunnel, Flink CDC, Debezium or Canal.

## Maturity levels

- `NATIVE`: QMigration native software path is enabled by default for the advertised capabilities.
- `NATIVE_FULL_ONLY`: native Full Load / target apply exists, but source CDC is intentionally not advertised for that product endpoint.
- `EXPERIMENTAL`: software path exists behind an explicit gate and requires real-instance qualification before production claims.
- `PROBE_ONLY`: connection/probe surface only; migration planning must reject the datasource.

## Database matrix

| Database | Protocol | Metadata | Full Read | Full Write / CDC Apply | Source CDC | Schema / DDL | Status |
|---|---|---:|---:|---:|---:|---:|---|
| MySQL | MySQL | Yes | Yes | Yes | Native Binlog/GTID | Yes | NATIVE |
| MariaDB | MySQL | Yes | Yes | Yes | Native Binlog | Yes | NATIVE |
| PolarDB MySQL | MySQL | Yes | Yes | Yes | MySQL-compatible Binlog, precheck-gated | Yes | NATIVE |
| PolarDB-X | MySQL + global Binlog | Yes | Yes | Yes | Native MySQL-compatible global Binlog; version/privilege prechecks apply | Yes | NATIVE |
| TiDB | MySQL SQL protocol | Yes | Yes | Yes | **Not advertised**; dedicated TiCDC adapter required | Yes | NATIVE_FULL_ONLY |
| OceanBase MySQL | MySQL SQL protocol | Yes | Yes | Yes | **Not advertised on SQL endpoint**; separate OceanBase Binlog Service endpoint must be modeled | Yes | NATIVE_FULL_ONLY |
| PostgreSQL | PostgreSQL | Yes | Yes | Yes | Native pgoutput + slot checkpoint | Yes | NATIVE |
| PolarDB PostgreSQL | PostgreSQL | Yes | Yes | Yes | Native pgoutput + slot checkpoint, precheck-gated | Yes | NATIVE |
| openGauss | PostgreSQL wire | Yes | Yes | Yes | Not advertised | Yes | NATIVE_FULL_ONLY |
| Kingbase | PostgreSQL wire | Yes | Yes | Yes | Not advertised | Yes | NATIVE_FULL_ONLY |
| Oracle | TNS/TCPS/TTC | Yes | Yes | Yes | LogMiner / SCN | Yes | EXPERIMENTAL; software-complete, qualification required |
| SQL Server | TDS/TLS | Yes | Yes | Yes | SQL Server CDC / LSN | Yes, including schema objects + IDENTITY | EXPERIMENTAL; qualification required |
| DB2 | TCP probe | No | No | No | No | No | PROBE_ONLY |
| Dameng / DM | TCP probe | No | No | No | No | No | PROBE_ONLY |
| GaussDB | TCP probe | No | No | No | No | No | PROBE_ONLY |
| GBase | TCP probe | No | No | No | No | No | PROBE_ONLY |

## Unified migration capabilities already implemented

The following are shared by supported native connectors rather than implemented
as separate third-party engines:

- Full snapshot: RANGE, bounded composite KEYSET, HASH, PARTITION and controlled CUSTOM predicates.
- Dynamic chunk refinement, Worker lease/failover, retry/resume and durable cursors.
- Bounded pipeline, adaptive batch size, source/target pressure sampling and two-level backpressure.
- Connector-neutral row image plus transform-policy DSL and fail-closed incompatible values.
- Durable CDC spool with encrypted file/shared-fs/S3 backends, integrity checks, multipart upload, reconciliation and drain leases.
- Apply-before-source-ACK ordering, transactional CDC apply, checkpoint-only progress, DDL policy, DLQ/conflict handling.
- Validation watermark barrier, row-count/checksum validation, chunk repair, catch-up, cutover, reverse sync and rollback state machine.
- Universal Schema Model, PK/index/FK post-load operations, safe same-family schema-object handling and manual policy for unsafe procedural cross-family objects.
- TLS/mTLS, encrypted credentials, audit/RBAC, Prometheus metrics, WebSocket status and Kubernetes deployment assets.

## Remaining GA gaps

1. Oracle real-instance qualification matrix: 11gR2/12c/19c/21c/23ai, NLS/charset, TCPS, RAC/SCAN, reconnect and long LOB/CDC soak.
2. SQL Server real-instance qualification matrix: supported server releases, encryption modes, CDC retention/restart, IDENTITY and large-value soak.
3. Dedicated source CDC adapters/endpoints for TiDB (TiCDC) and OceanBase MySQL (Binlog Service). PolarDB-X uses its MySQL-compatible global Binlog path.
4. Native DB2, Dameng, GaussDB and GBase connectors.
5. Cross-family stored procedure/function/trigger semantic conversion remains explicit/manual rather than guessed.
