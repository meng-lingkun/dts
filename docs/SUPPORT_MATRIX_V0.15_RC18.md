# QMigration V0.15.0-rc18 Support Matrix

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
| **GBase 8a MPP Cluster** | **Yes** | **Yes** | **Full Write only; validated HASH staging+MERGE; no CDC apply** | **Not advertised** | **Table/PK create only; no FK/post-load replay** | **EXPERIMENTAL / FULL_ONLY** |

## GBase 8a RC18 scope

Behind `QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE=1`:

- distinct GBase 8a Connector and native packet Metadata/Full Read;
- stable numeric/composite migration-key keyset reads;
- GBase-specific target type conversion;
- automatically created `ENGINE=EXPRESS` target uses a HASH-eligible stable migration-key column;
- actual target `SHOW CREATE TABLE` distribution is validated before staging/MERGE;
- random/REPLICATED target is rejected for the replay-safe MERGE path;
- every pre-created HASH distribution column must belong to the migration/MERGE key;
- per-batch staging + MERGE remains the retry-convergent Full Write path.

Fail-closed/not advertised:

- keyless Full Write;
- automatic target when no migration-key column is HASH-eligible;
- source CDC/durable source positions;
- transactional target CDC Apply;
- FK replay/enforcement;
- automatic workload/cardinality-based alternative distribution key selection;
- GBase 8s / GBase 8c;
- production maturity without retained GBase 8a qualification reports.

## Remaining highest-priority gaps

1. Retained GBase 8a V9.5.x Full/HASH-MERGE restart/failure-window qualification.
2. Workload skew/cardinality qualification for auto-selected HASH keys and pre-created layouts.
3. Retained GaussDB Full/binary CDC/DDL-only qualification and multi-primary.
4. Retained Dameng DM8 qualification and supported source CDC API.
5. Retained DB2 11.5/12.1/pureScale qualification.
