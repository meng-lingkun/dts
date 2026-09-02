# GBase 8s V8.8 Native Qualification

RC24+ exposes the GBase 8s Full/target + experimental source-CDC data plane behind:

```bash
export QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE=1
```

The SQL transport must be a matching **GBase Client-SDK ODBC** environment. The
QMigration source archive does not contain GBase CSDK libraries or proprietary
vendor driver source.

## Runtime prerequisites

Install and configure on every Server/Worker that can open a GBase 8s
connection:

1. the GBase Client-SDK matching the database family/version;
2. unixODBC (or the ODBC manager required by the selected Go wrapper);
3. a Go `database/sql` ODBC wrapper registered as `odbc` or another explicit
   `QMIGRATION_GBASE8S_SQL_DRIVER` name;
4. the required CSDK shared-library search path (`LD_LIBRARY_PATH` or an
   equivalent system loader configuration);
5. a non-secret ODBC DSN/connection-property string.

To build the runtime provider plugin from an already obtained local Go ODBC
wrapper source tree:

```bash
export GBASE8S_GO_ODBC_DRIVER_DIR=/opt/src/go-odbc-wrapper
deployments/scripts/build-gbase8s-driver-plugin.sh
export QMIGRATION_GBASE8S_DRIVER_PLUGIN=$PWD/bin/qmigration-gbase8s-driver.so
```

Use the same Go toolchain for the plugin and QMigration Server/Worker binaries.

## Credential rule

Do **not** place credentials in `GBASE8S_ODBC_DSN` or datasource `jdbc_url`.
RC22 rejects `UID=`, `USER=`, `PWD=` and `PASSWORD=` there. Configure normal
QMigration datasource username/password; QMigration injects those credentials
into the in-memory ODBC connection string.

## One-command qualification

```bash
export GBASE8S_HOST=10.0.0.10
export GBASE8S_PORT=9088
export GBASE8S_USER=qmigration
export GBASE8S_PASSWORD='***'
export GBASE8S_DATABASE=appdb
export GBASE8S_SCHEMA=qmigration
export GBASE8S_ODBC_DSN=GBASE8S_APP

# Read-only connection/catalog/full qualification
deployments/scripts/qualify-gbase8s.sh

# Inspect a representative table
export GBASE8S_TABLE=orders
deployments/scripts/qualify-gbase8s.sh

# Destructive temporary target qualification
GBASE8S_QUALIFY_TARGET_WRITE=1 deployments/scripts/qualify-gbase8s.sh
```

Optional controls:

```bash
export GBASE8S_SQL_DRIVER=odbc
export GBASE8S_SAMPLE_ROWS=16
export GBASE8S_QUALIFY_TIMEOUT=90s
export GBASE8S_QUALIFY_OUTPUT=/secure/path/gbase8s-qualification.json
```

The report does not contain the password or ODBC DSN string.


For source CDC additionally configure:

```bash
export QMIGRATION_EXPERIMENTAL_GBASE8S_CDC=1
export GBASE8S_CDC_URL=gbase8scdc://127.0.0.1:9188
export GBASE8S_QUALIFY_CDC=1
deployments/scripts/qualify-gbase8s.sh
```

The retained CDC qualification report records Agent API version, provider kind, provider ABI and whether SHA-256 pinning is active.

The bundled `qmigration-gbase8s-cdc-agent` should load a locally built native C ABI v4 CSDK provider. The legacy Go provider plugin remains compatibility-only. Example native configuration:

```bash
export QMIGRATION_GBASE8S_CDC_PROVIDER_LIBRARY=/opt/qmigration/lib/qm-gbase8s-cdc-provider.so
export QMIGRATION_GBASE8S_CDC_PROVIDER_SHA256=<exact-sha256>
export QMIGRATION_GBASE8S_CDC_PROVIDER_CONFIG_FILE=/etc/qmigration/gbase8s-cdc-provider.json
```

The config file must not be accessible by `other` users. For a non-loopback Agent listener configure TLS certificate/key and Bearer token; remote plaintext is rejected. See `GBASE8S_CDC_PROVIDER_PROTOCOL.md`. The Agent API receives no database password from QMigration.

## What the qualifier verifies

Read-only checks:

1. provider/plugin registration and CSDK ODBC authentication;
2. `DBINFO('version','full')` server version;
3. database logging/precheck visibility from `sysmaster:sysdatabases`;
4. visible owners;
5. optional `systables`/`syscolumnsext`/`sysconstraints`/`sysindexes` metadata;
6. optional bounded keyset Full Read;
7. optional `NTILE`/`ROW_NUMBER` keyset boundary query.

`--target-write` additionally:

1. verifies the selected target owner already exists;
2. creates a temporary table with primary key and BLOB column;
3. writes one key through prepared UPDATE/existence/INSERT logic;
4. replays the same key with an updated non-key value;
5. requires one logical row and an exact binary BLOB round trip;
6. begins an explicit QMigration target CDC transaction;
7. deletes the row by key and commits;
8. drops the temporary table.

## Required retained evidence before promotion

Retain the JSON report plus operational notes for every supported combination:

1. exact GBase 8s V8.8 build and deployment topology;
2. exact Client-SDK/ODBC library build and Go ODBC wrapper;
3. Linux distribution, unixODBC and dynamic-library configuration;
4. authentication and endpoint failover behavior;
5. database locale/DB_LOCALE/CLIENT_LOCALE and representative UTF-8/GBK/
   GB18030 workloads as applicable;
6. INTEGER/BIGINT/DECIMAL/FLOAT/BOOLEAN/DATE/DATETIME/VARCHAR/LVARCHAR/CLOB/BLOB
   boundary values;
7. composite primary/unique migration keys and long-running keyset resume;
8. Worker kill/restart during Full Write before/after UPDATE/existence/INSERT;
9. target CDC transaction rollback/commit behavior from a supported source CDC;
10. index/foreign-key target DDL on the exact supported build;
11. quoted/case-sensitive identifiers before expanding the RC22 identifier
    subset;
12. CSDK SSL/TLS configuration before enabling QMigration TLS modes for this
    connector.

## Source CDC release gate

RC22 implements the software/provider contract but it remains experimental. Retain evidence for:

- `cdc_opensess/startcapture/activatesess` checkpoint creation before Full;
- smart-LOB record stream parsing for the exact GBase 8s/CSDK build;
- long transactions where an older BEGIN remains open across later committed transactions;
- Worker/agent kill before and after QMigration durable apply;
- restart from `restart=<min open BEGIN>;commit=<last applied COMMIT>` without loss;
- ROLLBACK/DISCARD and update before/after pairing;
- source log retention/backpressure while Full is running.

RC22 supports the documented CDC_REC_TRUNCATE path for selected tables. Retained qualification must cover DML-before-TRUNCATE followed immediately by COMMIT/ROLLBACK and target rollback on apply failure. Selected tables containing smart BLOB/CLOB/complex CDC columns remain rejected until their separate retrieval path is implemented and retained-qualified.

## RC24 schema-fence + capture-lineage qualification

With `--cdc`, retain evidence that the Agent reports API v4/provider ABI v4 and that checkpoint/read responses return one live-validated schema fence per selected table. Qualification must exercise a controlled mismatch (for example a provider test table created with a different type/nullability/PK layout) and demonstrate fail-closed behavior before DML apply.

Because capture-active ALTER is rejected by the CDC source, also test stop/recreate capture after an intentional schema change and verify that the new TABLE_SCHEMA/catalog fingerprint is rejected against the persisted migration plan.

Retain a restart matrix for `capture_lineage`: same logical capture resume must return the same lineage and continue from the durable restart position; deliberate capture recreation must return a different lineage and QMigration must fail before applying a row. Also verify authenticated `/v1/status` and `/metrics`, and confirm lineage/sequence values are absent from Prometheus metrics.
## RC28 smart BLOB/CLOB qualification

Enable the additional gate and use a selected table that contains BLOB or CLOB:

```bash
export QMIGRATION_EXPERIMENTAL_GBASE8S_SMART_LOB_CDC=1
./bin/qmigration-gbase8s-qualify --cdc-smart-lob ...
```

The Agent/provider must declare `smart_lob_image_contract=cdc-event-owned-lob-v1`. Retained real-instance evidence must go beyond contract echo: create event A with known LOB bytes, let a later committed event B replace the same row's LOB while CDC consumption is delayed, then prove event A still delivers A's exact bytes/length/SHA-256. A current-row SELECT implementation will return B and must fail qualification.

Qualification must also cover NULL, empty LOB, binary BLOB bytes, multibyte CLOB text, UPDATE before/after images, provider restart with unchanged capture lineage, and values near the configured bounded response limit. Values exceeding the configured complete-record response budget must fail closed in RC28.
