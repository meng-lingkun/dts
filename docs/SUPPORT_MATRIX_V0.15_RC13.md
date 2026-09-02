# QMigration V0.15.0-rc13 Support Matrix

This matrix describes QMigration-owned migration/runtime support. SQL-wire
compatibility or a protocol probe alone does not imply source CDC support.
Vendor client libraries may be required only where the database exposes a
vendor API rather than a SQL/wire log API. RC13 uses this model for Db2
`db2ReadLog` and uses DM's `database/sql` Go driver strictly as the Dameng SQL transport; QMigration still owns migration semantics.

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
| GaussDB | No | No | No | No | No | PROBE_ONLY |
| GBase | No | No | No | No | No | PROBE_ONLY |

## Dameng RC13 scope

Implemented behind `QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE=1`:

- QMigration-owned catalog, Full Load, schema and target-apply logic;
- DM `database/sql` driver is a replaceable SQL transport provider, not a migration runtime;
- Linux runtime provider plugin loading through `QMIGRATION_DAMENG_DRIVER_PLUGIN`;
- schema/table/column/PK/index/FK metadata;
- numeric/composite keyset Full Read and NTILE boundaries;
- prepared keyless INSERT / keyed MERGE, BLOB/binary parameters, point lookup/delete;
- explicit target transactions for CDC Apply;
- fail-closed TLS handling until provider TLS settings are qualified;
- `qmigration-dameng-qualify` + one-command wrapper.

Not advertised: Dameng source CDC. A supported and retained-qualified source-log API is required before that capability is exposed.

## DB2 RC12 scope

Implemented:

- QMigration-owned `qmigration-db2-log-agent` on the source-side host;
- a small QMigration C provider calling IBM's supported `db2ReadLog` API;
- `DB2READLOG_FILTER_ON`, so only documented propagatable records are consumed;
- durable `DB2_LRI` capture/resume using `nextStartLRI`;
- `DATA CAPTURE CHANGES` and primary-key prechecks;
- source-local Initialize Table descriptor bootstrap before the migration LRI;
- INSERT / DELETE / UPDATE for complete ordinary row images;
- documented VALUE COMPRESSION full-row images, including NULL and `COMPRESS SYSTEM DEFAULT`;
- logged out-of-row BLOB/CLOB/DBCLOB and varying data, including chunked and consolidated-value reconstruction;
- Db2 11.5.8+ serialized XML CSL records when `DB2_DCC_XML_SERIALIZE=YES`;
- DMS 167 inline multi-insert expansion with per-row RID identity and bounded description validation;
- 40/56/64-byte normal/compensation/propagatable-compensation log-manager header handling;
- row-level compensation/SAVEPOINT net-effect reconstruction for undo insert/delete/update, empty-page variants and undo multi-insert;
- indirect UPDATE (`0x02`) after-image linkage to the immediately preceding same-table outer-`0x04` INSERT, with transaction-wide stale-candidate invalidation;
- `lrIUDflags=0x8000` decomposed DELETE+INSERT pairing emitted as one logical CDC UPDATE;
- out-of-row DMS 167 multi-insert reconstruction when exactly one row can own the pending external-value group;
- Db2 12.1.4+ DMS 213 VECTOR serialized source values scoped by Start Out-of-Row;
- source Full Read `VECTOR_SERIALIZE()` for representation consistency with CDC;
- VECTOR metadata preserves catalog dimension + `COORDINATETYPE`;
- DB2 target VECTOR schema restore plus Prepared `VECTOR(CAST(? AS CLOB),dimension,coordinate-type)` apply;
- large serialized VECTOR values reuse CLOB/EXTDTA while Full and CDC share the same prepared writer;
- transaction TID aggregation, child/subtransaction merge, COMMIT and ABORT;
- target Apply -> durable checkpoint -> reader ACK ordering;
- checkpoint-only safe advancement for windows with no selected open transaction;
- 100,000 events / 128 MiB per transaction and 10,000 open-transaction limits;
- Log Agent HTTP/TLS and optional Bearer token.

Fail-closed boundaries in RC12:

- partial/ambiguous VALUE COMPRESSION row images not proven to be complete full-row images;
- `NOT LOGGED` LOB columns and LOB operations that cannot yield a complete after-image;
- multi-insert records where more than one row requires external data, because manager records provide no row ordinal;
- indirect UPDATE linkage when the preceding INSERT candidate is missing, stale, cross-table or separated by another transaction row mutation;
- incomplete/interrupted decomposed-update pairs and decomposed-update compensation orders not yet retained-qualified;
- compensation records whose RID cannot be matched to a buffered selected-table mutation, or whose DMS function is outside the qualified row-undo set;
- malformed VECTOR metadata/value strings, dimension mismatches, unsupported coordinate types, or VECTOR use on a Db2 target release that does not support the type;
- selected-table Data Manager actions not explicitly decoded;
- non-UTF8 row character bytes that cannot be losslessly interpreted by the current decoder;
- Db2 pureScale multi-log-stream ordering until retained qualification proves the ordering model.

Operational dependency: the Go Server/Worker does **not** link IBM libraries. The
source-side DB2 Log Agent host needs IBM Data Server Client/Runtime headers and
`libdb2` to build/run `qmigration-db2-readlog-provider`.

## Remaining highest-priority gaps

1. Retained DB2 LUW 11.5 / 12.1 real-instance source-CDC qualification.
2. DB2 real-instance out-of-row multi-insert/VECTOR source + VECTOR target qualification, pureScale qualification and retained decomposed-update rollback traces.
3. Retained Dameng DM8 real-instance qualification, provider TLS qualification and a supported source-CDC/log API.
4. GaussDB Native Connector.
5. GBase Native Connector.
