# QMigration V0.15.0-rc16 Support Matrix

| Database | Metadata | Full Read | Full Write / CDC Apply | Source CDC | Schema / DDL | Status |
|---|---:|---:|---:|---|---:|---|
| MySQL / MariaDB / PolarDB MySQL / PolarDB-X | Yes | Yes | Yes | Native MySQL-compatible binlog paths | Yes | NATIVE / prerequisite-specific |
| TiDB | Yes | Yes | Yes | TiCDC + native Kafka Canal-JSON | Yes | EXPERIMENTAL |
| OceanBase MySQL | Yes | Yes | Yes | Binlog Service / ODP | Yes | EXPERIMENTAL |
| PostgreSQL / PolarDB PostgreSQL | Yes | Yes | Yes | pgoutput | Yes | NATIVE |
| openGauss / Kingbase | Yes | Yes | Yes | Not advertised | Yes | NATIVE_FULL_ONLY |
| Oracle | Yes | Yes | Yes | LogMiner / SCN | Yes | EXPERIMENTAL |
| SQL Server | Yes | Yes | Yes | SQL Server CDC / LSN | Yes | EXPERIMENTAL |
| DB2 LUW | Yes | Yes | Yes | QMigration Log Agent + IBM db2ReadLog | Yes | EXPERIMENTAL |
| Dameng / DM8 | Yes | Yes | Yes | Not advertised | Table/PK/index/FK target | EXPERIMENTAL |
| **GaussDB** | **Yes** | **Yes** | **Yes** | **mppdb_decoding binary DML + optional DDL-only classification / GAUSSDB_LSN** | **Target yes; selected-table DDL-only same-family replay** | **EXPERIMENTAL** |
| GBase | No | No | No | No | No | PROBE_ONLY |

## GaussDB RC16 scope

Behind `QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE=1` and
`QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC=1`:

- PostgreSQL-wire Metadata, Full Read/Write, schema and transactional target
  apply;
- explicit LSN-based `mppdb_decoding` slot and durable `GAUSSDB_LSN`;
- byte-safe binary DML peek/get with NULL/empty/NUL/non-UTF8/bytea handling;
- optional text DDL classification pass merged with binary DML at one commit-LSN
  boundary;
- DDL-only source replay only for GaussDB -> GaussDB identity mappings,
  `cdc_ddl_mode=SAME_FAMILY`, explicit
  `QMIGRATION_GAUSSDB_DDL_ONLY_TRANSACTIONS=1`, and
  `enable_logical_replication_ddl=on`;
- safe DDL subset: selected-table ALTER TABLE, TRUNCATE and CREATE [UNIQUE]
  INDEX.

Fail-closed/not advertised: hybrid DDL/DML transactions, unsupported DDL
families, multi-primary logical decoding, and production maturity without
retained centralized/distributed qualification reports.

## Remaining highest-priority gaps

1. Retained GaussDB Full/binary CDC/DDL-only restart and retention qualification.
2. GaussDB multi-primary qualification; hybrid DDL/DML remains blocked by source-decoder limitations.
3. Retained Dameng DM8 qualification and supported source CDC API.
4. Retained DB2 11.5/12.1/pureScale qualification.
5. GBase Native Connector.
