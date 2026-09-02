# QMigration V0.15.0-rc5 Support Matrix

This matrix describes QMigration-owned runtime support. SQL-wire compatibility
or a successful protocol probe alone does not imply source CDC support.

## Database matrix

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
| **DB2 LUW** | **Yes** | **Yes** | **Yes, experimental bounded DML** | **Not advertised** | **Yes** | **EXPERIMENTAL; qualification required** |
| Dameng / DM | No | No | No | No | No | PROBE_ONLY |
| GaussDB | No | No | No | No | No | PROBE_ONLY |
| GBase | No | No | No | No | No | PROBE_ONLY |

## DB2 RC5 scope

Implemented by QMigration-owned Go code:

- DRDA/DDM protocol probe and authenticated database session;
- direct TLS/mTLS;
- SECMEC 9 encrypted credentials and TLS-only fallback for SECMEC 3;
- SYSCAT Metadata / PK / index / FK;
- Full Read including EXTDTA LOB payloads;
- numeric RANGE metadata and composite KEYSET boundary planning;
- target table/schema/index/FK creation;
- target MERGE/INSERT/delete/point lookup and transactional CDC apply SPI;
- schema-object catalog discovery;
- qualification binary and one-command wrapper.

Not claimed in RC5:

- DB2 source log CDC / IIDR / Q Replication reader;
- all DRDA security mechanisms beyond SECMEC 9 and TLS-protected SECMEC 3;
- very-large target BLOB/CLOB prepared-parameter/EXTDTA streaming;
- production compatibility before retained real-instance qualification.

## Remaining highest-priority gaps

1. DB2 11.5 / 12.1 real-instance DRDA/TLS/type/LOB qualification and target
   prepared-LOB streaming hardening.
2. DB2 source CDC architecture and implementation.
3. Dameng Native Connector.
4. GaussDB Native Connector.
5. GBase Native Connector.
6. Cross-family stored procedure/function/trigger conversion remains explicit/manual.
