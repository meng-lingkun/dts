# QMigration V0.15.0-rc17 Support Matrix

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
| GaussDB | Yes | Yes | Yes | mppdb_decoding binary DML + optional DDL-only classification / GAUSSDB_LSN | Target yes; selected-table DDL-only same-family replay | EXPERIMENTAL |
| **GBase 8a MPP Cluster** | **Yes** | **Yes** | **Full Write only; no CDC apply** | **Not advertised** | **Table/PK create only; no FK/post-load replay** | **EXPERIMENTAL / FULL_ONLY** |

## GBase 8a RC17 scope

Behind `QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE=1`:

- distinct GBase 8a Connector descriptor and Unified Engine routing;
- QMigration native packet transport, `information_schema` metadata and Full
  Read;
- stable numeric/composite migration-key keyset reads and ordered boundaries;
- GBase-specific target type conversion;
- `ENGINE=EXPRESS` table creation with random distribution by default;
- staging-table + `MERGE` keyed Full Write for retry-safe logical upsert;
- migration prechecks and retained qualification workflow.

Fail-closed/not advertised:

- keyless Full Write;
- source CDC and durable source log positions;
- target transactional CDC Apply;
- foreign-key replay/enforcement;
- automatic workload-aware HASH/REPLICATED placement;
- automatic `AUTO_INCREMENT` generator restoration;
- GBase 8s / GBase 8c;
- production maturity without retained GBase 8a version/topology/charset/auth/TLS
  qualification reports.

## Remaining highest-priority gaps

1. Retained GBase 8a real-instance Full source/target and staging+MERGE restart qualification.
2. Workload-aware/pre-created HASH/REPLICATED target qualification and performance tuning.
3. Retained GaussDB Full/binary CDC/DDL-only restart and retention qualification.
4. Retained Dameng DM8 qualification and supported source CDC API.
5. Retained DB2 11.5/12.1/pureScale qualification.
