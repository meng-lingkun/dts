# QMigration V0.15.0-rc20 Support Matrix

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
| **GBase 8s V8.8** | **Yes** | **Yes** | **Full Write + transactional target CDC Apply** | **syscdcv1/CSDK smart-LOB provider / GBASE8S_CDC_SEQ** | **Target owner check + table/PK/index/FK apply** | **EXPERIMENTAL / qualification required** |

## GBase 8s RC20 scope

Behind `QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE=1`; source CDC additionally requires `QMIGRATION_EXPERIMENTAL_GBASE8S_CDC=1`:

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
- qualification/precheck tooling;
- source CDC through a datasource-local CSDK smart-LOB provider;
- durable `GBASE8S_CDC_SEQ` restart/commit watermarks with long-transaction rewind;
- apply-before-ACK through the shared QMigration CDC runtime.

Fail-closed/not advertised:

- production-qualified GBase 8s source CDC without retained CSDK/provider reports;
- RC20 smart BLOB/CLOB/complex-type source CDC and TRUNCATE replay;
- keyless target Full Write;
- implicit target user/owner creation;
- quoted/case-sensitive identifiers outside the RC20 safe identifier subset;
- QMigration TLS `PREFERRED/REQUIRED` until CSDK SSL parameters are retained-qualified;
- production maturity without retained GBase 8s V8.8/CSDK/unixODBC/topology/
  charset/failover reports.

## GBase product-family split

`gbase` continues to mean **GBase 8a MPP Cluster** and uses the existing native
packet/EXPRESS HASH-MERGE Full path. `gbase8s` means **GBase 8s V8.8** and uses
its own CSDK/ODBC transactional SQL path. GBase 8c is not implied by either
connector.

## Remaining highest-priority gaps

1. Retained GBase 8s V8.8/CSDK Full + target-apply + syscdcv1 CDC restart/failure-window qualification.
2. Qualify smart BLOB/CLOB handling and additional CDC record families before broadening RC20 fail-closed boundaries.
3. Retained GBase 8a Full/HASH-MERGE qualification and workload skew/cardinality tuning.
4. Retained GaussDB Full/binary CDC/DDL-only qualification and multi-primary.
5. Retained Dameng and DB2 qualification work.
