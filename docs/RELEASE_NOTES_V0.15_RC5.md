# QMigration V0.15.0-rc5 Release Notes

## Release purpose

RC5 moves DB2 LUW from an external/JDBC placeholder to a QMigration-owned
native DRDA/DDM data plane. The new path is intentionally `EXPERIMENTAL` until
real DB2 qualification reports are retained. DB2 source CDC is not advertised.

## DB2 Native DRDA

- Pure Go DRDA/DDM implementation; no IBM CLI, JDBC, Python or third-party
  migration runtime is loaded.
- Native DRDA probe plus authenticated session establishment.
- DRDA flow covers EXCSAT / ACCSEC / SECCHK / ACCRDB and dynamic SQL query /
  execute / commit / rollback.
- SECMEC 9 encrypted user/password authentication is implemented.
- SECMEC 3 is accepted only on an already-established TLS session; QMigration
  refuses to send plaintext credentials over a non-TLS DRDA connection.
- Direct TLS, CA verification, server-name verification and optional mTLS use
  the common datasource TLS policy.
- Native QRYDSC/QRYDTA decoding for integer, decimal, floating point, character,
  date/time/timestamp, binary, boolean and LOB families.
- EXTDTA LOB payload association for source Full Read.
- DB2 timestamp values are normalized to QMigration's canonical
  `YYYY-MM-DD HH:MM:SS[.fraction]` representation before heterogeneous writes.

## Metadata and Full Load

- SYSCAT schema/table/column discovery.
- Primary key, secondary/unique index and foreign-key discovery through DB2
  catalogs.
- DB2 identity mode plus `START WITH` / `INCREMENT BY` discovery through `SYSCAT.COLIDENTATTRIBUTES`; target DDL preserves `GENERATED ALWAYS` versus `BY DEFAULT`.
- Full-load completion synchronizes DB2 identity `RESTART WITH` state; committed target CDC transactions also advance identity state before the transaction is considered durably applied.
- Generated-expression columns are excluded from target DML and DB2 target auto-create fails closed when their source expression is unavailable, rather than silently converting them into ordinary columns.
- Numeric single-key MIN/MAX discovery.
- Composite bounded keyset ReadBatch.
- Ordered `NTILE` keyset boundary planning for parallel Full Load.
- Target schema/table/PK/index/FK creation.
- Heterogeneous target type compiler for common MySQL/PostgreSQL/Oracle/SQL
  Server types into DB2 LUW types.
- Transactional target MERGE/INSERT, delete, point lookup and DDL apply. `GENERATED ALWAYS` identity row images use `OVERRIDING SYSTEM VALUE` so source identity values are preserved.
- View/sequence/trigger/routine catalog discovery; objects with unresolved
  dependency semantics remain manual through the existing schema-object planner.

## Safety boundaries

- Numeric values rendered into the current dynamic SQL writer use the shared
  arbitrary-precision fail-closed numeric validator.
- Binary values use DB2 hex literals and strings use DB2 SQL escaping.
- DRDA packet/field/read sizes are bounded.
- RC5 target DML is still the bounded dynamic-SQL implementation. Very large
  target BLOB/CLOB values that exceed the DRDA SQLSTT request limit fail closed;
  prepared/EXTDTA target parameter streaming remains a post-RC5 hardening item.
- DB2 source CDC is not implemented or advertised. `FULL+CDC` with DB2 as the
  source therefore remains rejected by the planner.

## Qualification

New artifacts:

- `qmigration-db2-qualify`
- `deployments/scripts/qualify-db2.sh`
- `docs/DB2_NATIVE_QUALIFICATION.md`

The qualifier is read-only by default. `--target-write` explicitly creates and
removes one temporary `QMQUAL_*` table to exercise create, MERGE, binary value,
point lookup, identity `START/INCREMENT`, full-load identity restart, transactional CDC identity restart, rollback and secondary-index operations.

## Verification

RC5 release verification requires:

- `go test ./...`
- `go vet ./...`
- `make backend-build`
- RC4 -> RC5 clean patch restore
- formal V0.13 -> RC5 cumulative clean patch restore
- byte-for-byte equality of both restored source trees

Web build status is recorded separately in the archive manifest and is never
inferred from backend success.
