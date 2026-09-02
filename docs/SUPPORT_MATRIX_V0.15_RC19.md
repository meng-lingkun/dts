# QMigration V0.15.0-rc19 Support Matrix

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
| GBase 8a MPP Cluster | Yes | Yes | Full Write only; validated HASH staging+MERGE; no CDC apply | Not advertised | Table/PK create only | EXPERIMENTAL / FULL_ONLY |
| **GBase 8s V8.8** | **Yes** | **Yes** | **Full Write + transactional target CDC Apply** | **Not advertised** | **Target owner check + table/PK/index/FK apply** | **EXPERIMENTAL / qualification required** |

## GBase 8s RC19 scope

Behind `QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE=1`:

- distinct `gbase8s` datasource/Connector family; it is not routed through GBase
  8a;
- vendor GBase Client-SDK ODBC is only the SQL transport provider;
- QMigration-owned catalog Metadata and numeric/composite keyset Full Read;
- optional ordered `NTILE` boundary planning;
- conservative GBase 8s target type conversion;
- pre-existing target owner + table/PK/index/FK creation;
- key-required prepared UPDATE/existence/INSERT Full Write;
- exact database/sql byte binding for BLOB/binary values;
- point lookup/delete and transactional target CDC Apply;
- non-secret ODBC DSN plus encrypted datasource credentials;
- qualification/precheck tooling.

Fail-closed/not advertised:

- GBase 8s source CDC or a durable source-log position;
- keyless target Full Write;
- implicit target user/owner creation;
- quoted/case-sensitive identifiers outside the RC19 safe identifier subset;
- QMigration TLS `PREFERRED/REQUIRED` until CSDK SSL parameters are retained-qualified;
- production maturity without retained GBase 8s V8.8/CSDK/unixODBC/topology/
  charset/failover reports.

## GBase product-family split

`gbase` continues to mean **GBase 8a MPP Cluster** and uses the existing native
packet/EXPRESS HASH-MERGE Full path. `gbase8s` means **GBase 8s V8.8** and uses
its own CSDK/ODBC transactional SQL path. GBase 8c is not implied by either
connector.

## Remaining highest-priority gaps

1. Retained GBase 8s V8.8/CSDK Full + target-apply qualification and failure-window tests.
2. Determine whether a supported GBase 8s source CDC API can satisfy durable position + transaction + apply-before-ACK requirements; keep source CDC disabled until then.
3. Retained GBase 8a Full/HASH-MERGE qualification and workload skew/cardinality tuning.
4. Retained GaussDB Full/binary CDC/DDL-only qualification and multi-primary.
5. Retained Dameng and DB2 qualification work.
