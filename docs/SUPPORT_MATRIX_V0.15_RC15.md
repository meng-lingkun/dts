# QMigration V0.15.0-rc15 Support Matrix

This matrix describes QMigration-owned migration/runtime support. SQL-wire
compatibility or a protocol probe alone does not imply source CDC support.
Vendor client libraries may be required only where the database exposes a
vendor API rather than a SQL/wire log API. Db2 uses IBM `db2ReadLog` on the
source Log Agent host; Dameng uses its `database/sql` driver strictly as SQL
transport while QMigration owns migration semantics.

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
| **DB2 LUW** | **Yes** | **Yes, EXTDTA LOB + VECTOR_SERIALIZE source** | **Yes, Prepared SQLDTA/EXTDTA + VECTOR target** | **QMigration Log Agent + IBM db2ReadLog / DB2_LRI** | **Yes** | **EXPERIMENTAL; qualification required** |
| **Dameng / DM** | **Yes** | **Yes** | **Yes, prepared INSERT/MERGE + transactional CDC Apply** | **Not advertised** | **Table/PK/index/FK** | **EXPERIMENTAL; qualification required** |
| **GaussDB** | **Yes** | **Yes** | **Yes** | **mppdb_decoding binary SQL API / GAUSSDB_LSN** | **Target schema yes; source DDL not advertised** | **EXPERIMENTAL; qualification required** |
| GBase | No | No | No | No | No | PROBE_ONLY |

## GaussDB RC15 scope

Implemented behind `QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE=1` plus
`QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC=1` for source CDC:

- QMigration PostgreSQL-wire Metadata, Full Read/Write, keyset/boundary, schema
  and target transactional apply;
- durable current position as `GAUSSDB_LSN` and explicit LSN-based
  `mppdb_decoding` slot (`output_order=0`);
- non-advancing `pg_logical_slot_peek_binary_changes` capture and
  post-target-commit `pg_logical_slot_get_binary_changes` ACK;
- documented B/C/I/U/D binary frame and tuple decoder with strict lengths,
  delimiters, XID and LSN validation;
- byte-preserving NUL/non-UTF8 handling, SQL NULL versus empty-value
  distinction, and OID 17 `bytea` conversion;
- selected-table `white-table-list`, primary-key validation, transaction
  event/byte safety bounds and QMigration TLS settings;
- `qmigration-gaussdb-qualify` validates the actual temporary binary-peek path.

Fail-closed/not advertised in RC15: source DDL replay, multi-primary logical
CDC, malformed/unknown binary frame variants, and production maturity without
retained centralized/distributed real-instance reports.

## Dameng RC13 scope

Implemented behind `QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE=1`: QMigration-owned
catalog, Full Load, schema and target apply with a replaceable vendor
`database/sql` transport provider. Source CDC remains unadvertised pending a
supported and retained-qualified source-log API.

## DB2 RC12 scope

The existing qualification-gated DB2 LUW DRDA/DDM Full/target and source-side
QMigration Log Agent + IBM `db2ReadLog` path remains unchanged in RC15,
including DB2_LRI durability, value compression, logged LOB/XML, multi-insert,
row compensation, relocation/decomposed updates and VECTOR source/target
software paths. Real DB2 11.5/12.1 and pureScale qualification remains a
release gate.

## Remaining highest-priority gaps

1. Retained GaussDB centralized/distributed Full + binary CDC restart/retention qualification.
2. GaussDB source DDL transaction-model implementation and multi-primary qualification.
3. Retained Dameng DM8 qualification, provider TLS qualification and supported source CDC API.
4. Retained DB2 LUW 11.5/12.1/pureScale qualification.
5. GBase Native Connector.
