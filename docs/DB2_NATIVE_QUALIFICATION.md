# DB2 Native DRDA + db2ReadLog Qualification

RC12 DB2 support remains experimental until retained reports exist for every DB2
release/deployment class QMigration claims. Full/target data uses QMigration's
pure-Go DRDA/DDM stack. Source CDC uses a QMigration Log Agent and IBM's
`db2ReadLog` API on the source host.

## Full / target qualification

```bash
export DB2_HOST=db2.example.internal
export DB2_PORT=50000
export DB2_DATABASE=SAMPLE
export DB2_USER=qmigration
export DB2_PASSWORD='***'
export DB2_SCHEMA=APP

deployments/scripts/qualify-db2.sh
```

Optional source sample:

```bash
DB2_TABLE=ORDERS deployments/scripts/qualify-db2.sh
```

Explicit destructive target test:

```bash
DB2_QUALIFY_TARGET_WRITE=1 deployments/scripts/qualify-db2.sh
```

The target test includes exact DECIMAL, transaction rollback, identity lifecycle,
and a 2 MiB BLOB plus large UTF-8 CLOB round trip through Prepared
SQLDTA/EXTDTA.

On Db2 12.1.2+ explicitly add VECTOR target qualification:

```bash
DB2_QUALIFY_TARGET_WRITE=1 DB2_QUALIFY_TARGET_VECTOR=1 deployments/scripts/qualify-db2.sh
```

The VECTOR check creates `VECTOR(3,FLOAT32)` and `VECTOR(4,INT8)`, writes both
through the QMigration prepared path, and reads them back with
`VECTOR_SERIALIZE()`. Source CDC VECTOR qualification still requires Db2
12.1.4+ because DMS function 213 is the source log representation used by RC12.

## Source CDC prerequisites

The database must be recoverable (archive logging enabled) and the source account
used by the provider must have the authority IBM requires for `db2ReadLog`
(DBADM or SYSADM). Every selected table must have a deterministic primary key and
`DATA CAPTURE CHANGES` enabled. RC12 also rejects selected LOB columns declared
`NOT LOGGED`; selected XML tables require Db2 11.5.8+ and the live
`DB2_DCC_XML_SERIALIZE=YES` registry setting. For example:

```sql
ALTER TABLE APP.ORDERS DATA CAPTURE CHANGES;
```

The Agent host requires IBM Data Server Client/Runtime development headers and
`libdb2`. Build only this provider on that host:

```bash
export DB2_HOME=/opt/ibm/db2/V11.5
deployments/scripts/build-db2-readlog-provider.sh
```

The QMigration Go Server/Worker does not link `libdb2`.

## Run the DB2 Log Agent

The provider reuses the DB2 connection environment:

```bash
export QMIGRATION_DB2_DATABASE=SAMPLE
export QMIGRATION_DB2_USER=qmigration
export QMIGRATION_DB2_PASSWORD='***'
export QMIGRATION_DB2_READLOG_PROVIDER=/opt/qmigration/bin/qmigration-db2-readlog-provider
export QMIGRATION_DB2_LOG_LISTEN=:8787
export QMIGRATION_DB2_LOG_TOKEN='replace-with-a-secret'

bin/qmigration-db2-log-agent
```

For TLS:

```bash
export QMIGRATION_DB2_LOG_TLS_CERT_FILE=/etc/qmigration/db2-log-agent.crt
export QMIGRATION_DB2_LOG_TLS_KEY_FILE=/etc/qmigration/db2-log-agent.key
bin/qmigration-db2-log-agent
```

Do not place the bearer token in `cdc_url`; inject it by environment/secret.

## CDC qualification

```bash
export DB2_CDC_URL='db2logs://db2-agent.example.internal:8787?server_name=db2-agent.example.internal'
export DB2_CDC_TOKEN_ENV=QMIGRATION_DB2_LOG_TOKEN
export QMIGRATION_DB2_LOG_TOKEN='replace-with-a-secret'
export DB2_QUALIFY_CDC=1
export DB2_TABLE=ORDERS

deployments/scripts/qualify-db2.sh
```

Optional Log Agent CA:

```bash
export DB2_CDC_CA_FILE=/etc/qmigration/db2-log-agent-ca.pem
export DB2_CDC_SERVER_NAME=db2-agent.example.internal
```

The CDC qualifier verifies:

- DB2 catalog Full/metadata access;
- `DATA CAPTURE CHANGES` and PK;
- no selected LOB column has `SYSCAT.COLUMNS.LOGGED='N'`;
- for selected XML tables, the live `DB2_DCC_XML_SERIALIZE` registry value is enabled;
- Log Agent health;
- recoverable database/current `DB2_LRI`;
- Initialize Table descriptor bootstrap before the captured start LRI;
- descriptor/catalog column-count agreement.

## RC12 qualification matrix

Retain reports for at least:

- DB2 LUW 11.5 and 12.1 environments actually deployed by customers;
- same-host and remote Log Agent deployments;
- Agent TLS + CA verification and, where used, bearer token;
- INSERT / UPDATE / DELETE / multi-insert / large transactions;
- indirect/relocated UPDATE where the second UPDATE image carries outer `0x02` and the immediately preceding same-transaction INSERT carries outer `0x04`;
- interleaving an unselected DCC-table mutation between an outer-`0x04` INSERT and indirect UPDATE, proving the stale candidate is rejected;
- decomposed UPDATE represented as DELETE then INSERT with `lrIUDflags=0x8000`, including cases where the RID changes;
- COMMIT / ABORT, subtransactions and SAVEPOINT-style partial rollback with insert/delete/update undo compensation;
- ordinary `DMSUndoUpdate` compensation of an indirect UPDATE after it has been reconstructed as one logical UPDATE;
- retained real-log traces for rollback of decomposed DELETE+INSERT updates before certifying arbitrary decomposed compensation ordering;
- reader restart from durable `DB2_LRI`;
- Full+CDC snapshot-to-catch-up transition;
- archive-log rotation/retention pressure;
- UTF-8 text, exact DECIMAL, date/time/timestamp and binary row values;
- ordinary inline BLOB/CLOB plus logged out-of-row BLOB/CLOB/DBCLOB and varying values;
- DMS 167 multi-insert where exactly one row owns an out-of-row LOB/XML/VECTOR group, plus a retained ambiguous multi-row case proving fail-closed behavior;
- Db2 12.1.4+ VECTOR INSERT/UPDATE/NULL source values, comparing Full `VECTOR_SERIALIZE()` with CDC function-213 output;
- Db2 12.1.2+ VECTOR target create/prepared-write/readback with both FLOAT32 and INT8, including a large serialized vector that exercises EXTDTA;
- VALUE COMPRESSION rows with NULL and `COMPRESS SYSTEM DEFAULT`;
- Db2 11.5.8+ serialized XML INSERT/UPDATE with `DB2_DCC_XML_SERIALIZE=YES`;
- cutover final drain and target identity restoration.

## Explicit fail-closed cases in RC12

Do not certify a table/workload yet if qualification reaches:

- partial/ambiguous VALUE COMPRESSION row images that cannot be proven complete;
- `NOT LOGGED` LOB columns or LOB operation sequences that do not provide complete bytes;
- multi-insert records where more than one row requires out-of-row data and no documented row-ownership signal exists;
- indirect outer-`0x02` UPDATE records whose required preceding outer-`0x04` INSERT is missing, stale, cross-table or separated by another row mutation;
- incomplete/interrupted decomposed `lrIUDflags=0x8000` DELETE+INSERT pairs, or decomposed compensation orders not covered by retained qualification traces;
- compensation whose RID cannot be matched, or selected-table compensation functions outside the qualified row-undo set;
- malformed/missing VECTOR dimension or coordinate metadata, serialized value dimension mismatch, out-of-range INT8 or non-finite FLOAT32 coordinates, or a target Db2 release/catalog that does not support VECTOR;
- unknown selected-table Data Manager actions;
- non-UTF8 row text requiring an unimplemented legacy log-codepage conversion;
- pureScale multi-log-stream ordering without retained ordering/failover evidence.

These conditions stop CDC rather than advancing the durable LRI with uncertain
row data.

## TLS for the normal DB2 DRDA data plane

Use one of `DISABLE`, `PREFERRED`, or `REQUIRED`:

```bash
export DB2_TLS_MODE=REQUIRED
export DB2_TLS_SERVER_NAME=db2.example.internal
export DB2_TLS_CA_FILE=/etc/qmigration/db2-ca.pem
```

Optional mTLS:

```bash
export DB2_TLS_CERT_FILE=/etc/qmigration/db2-client.pem
export DB2_TLS_KEY_FILE=/etc/qmigration/db2-client-key.pem
```

QMigration never emits passwords, bearer-token values or private-key contents in
the JSON qualification report.

## Exit criteria

Remove the DB2 experimental gates only after retained reports show:

- zero FAIL checks for the claimed Full/CDC matrix;
- no precision/timestamp/LOB corruption;
- restart does not acknowledge an incomplete target transaction;
- `db2ReadLog` retention exhaustion fails visibly and does not skip forward;
- all claimed VALUE COMPRESSION/LOB/XML/multi-insert/indirect-update/decomposed-update/row-compensation cases are implemented and qualified; Db2 12.1.2+ VECTOR target and 12.1.4+ VECTOR source paths have retained reports; pureScale remains unclaimed until its ordering matrix passes;
- privileges, archive-log sizing, TLS and Agent deployment requirements are documented.
