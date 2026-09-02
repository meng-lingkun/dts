# QMigration V0.15.0-rc7 Release Notes

RC7 adds an experimental, correctness-first DB2 LUW source CDC path. DB2 Full
Migration/target apply remains the pure-Go DRDA/DDM implementation from RC5/RC6;
source CDC is isolated behind a QMigration source-side Log Agent because IBM
exposes asynchronous row-log access through the `db2ReadLog` API rather than
through DRDA SQL.

## Added

- `qmigration-db2-log-agent` HTTP/TLS service.
- `qmigration-db2-cdc` Unified Engine reader process.
- QMigration native provider source `backend/native/db2readlog/qmigration_db2readlog.c`.
- `deployments/scripts/build-db2-readlog-provider.sh` for IBM SDK/libdb2 hosts.
- `DB2_LRI` durable source position and resume contract.
- Db2 `DATA CAPTURE CHANGES`, recoverability and primary-key prechecks.
- Source-local Initialize Table descriptor bootstrap.
- ordinary INSERT/UPDATE/DELETE Data Manager row decode.
- transaction TID grouping, subtransaction merge, COMMIT/ABORT handling.
- source ACK only after QMigration target Apply + durable checkpoint.
- Log Agent TLS/server-name/CA/Bearer-token client support.
- `qmigration-db2-qualify --cdc` checks Agent health, current LRI and descriptor bootstrap.
- metadata marker `038_v015_rc7_db2_readlog_cdc.sql`.

## Safety / fail-closed behavior

RC7 rejects a selected table/transaction instead of emitting uncertain data when
it sees value-compressed row images, out-of-row LOB log reconstruction,
multi-insert, compensation/savepoint/undo net effects, an unknown selected-table
Data Manager function, or an unqualified pureScale multi-log-stream ordering
case. Limits are 100,000 events / 128 MiB per transaction and 10,000 open
transactions.

The Agent validates that normalized envelope metadata matches the raw log header
(length, log type, flags and TID) before row decoding.

## Runtime boundary

The default QMigration backend binaries remain Go builds and do not link IBM
libraries. Only the source-side provider is compiled on a DB2 host against IBM
Data Server Client/Runtime headers and `libdb2`. RC7 does not use IIDR, Q
Replication, Debezium, Flink CDC, DataX or SeaTunnel at runtime.

## Qualification status

DB2 source CDC remains `EXPERIMENTAL` and requires both:

- `QMIGRATION_EXPERIMENTAL_DB2_NATIVE=1`
- `QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC=1`

No production certification is claimed without retained real-instance reports.
The current build environment can run Go/unit/fake-log tests and C ABI-shape
syntax tests, but it does not contain a real IBM SDK/libdb2 or DB2 server.
