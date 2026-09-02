# QMigration V0.15.0-rc6 Support Matrix

This matrix describes QMigration-owned runtime support. SQL-wire compatibility
or a successful protocol probe alone does not imply source CDC support.

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
| **DB2 LUW** | **Yes** | **Yes, EXTDTA LOB** | **Yes, Prepared SQLDTA/EXTDTA** | **Not advertised** | **Yes** | **EXPERIMENTAL; qualification required** |
| Dameng / DM | No | No | No | No | No | PROBE_ONLY |
| GaussDB | No | No | No | No | No | PROBE_ONLY |
| GBase | No | No | No | No | No | PROBE_ONLY |

## DB2 RC6 scope

Implemented in QMigration-owned Go code:

- DRDA/DDM authenticated session, TLS/mTLS and SECMEC 9 / TLS-protected 3;
- SYSCAT metadata, PK/index/FK and schema objects;
- Full Read with generic multi-segment QRYDTA/EXTDTA reassembly;
- composite keyset and ordered boundaries;
- target schema/table/index/FK;
- Prepared INSERT/MERGE target DML using EXCSQLSTT + SQLDTA;
- exact packed DECIMAL parameters;
- out-of-line EXTDTA BLOB/CLOB target values over 32 KiB;
- transactional target CDC Apply, point lookup/delete and identity-state sync;
- DB2 qualification tool with large-LOB target round trip.

Not claimed:

- DB2 source log CDC / IIDR / Q Replication reader;
- every DRDA security mechanism;
- production support before DB2 11.5/12.1 real-instance qualification;
- single logical LOB values above the current 256 MiB safety bound.

## Remaining highest-priority gaps

1. DB2 11.5 / 12.1 real-instance qualification of DRDA/TLS/types/LOBs.
2. DB2 native source CDC architecture/implementation.
3. Dameng Native Connector.
4. GaussDB Native Connector.
5. GBase Native Connector.
