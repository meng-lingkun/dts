# SQL Server Native Qualification

QMigration V0.15.0-rc2 contains an experimental native SQL Server software data
plane. The experimental gate is removed only after the exact server/deployment
class has a retained qualification report.

## Gates

```bash
export QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE=1
export QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC=1   # only when source CDC is required
```

## One-command qualification

Read-only by default:

```bash
SQLSERVER_HOST=sqlserver.example.internal \
SQLSERVER_DATABASE=app \
SQLSERVER_USER=qmigration \
SQLSERVER_PASSWORD='***' \
  deployments/scripts/qualify-sqlserver.sh
```

Include CDC prerequisites/current LSN:

```bash
SQLSERVER_QUALIFY_CDC=1 deployments/scripts/qualify-sqlserver.sh
```

Explicit destructive target-write qualification creates and drops a temporary
`QMQUAL_*` table:

```bash
SQLSERVER_QUALIFY_TARGET_WRITE=1 deployments/scripts/qualify-sqlserver.sh
```

Optional JSON report:

```bash
SQLSERVER_QUALIFY_OUTPUT=/tmp/sqlserver-qualification.json \
  deployments/scripts/qualify-sqlserver.sh
```

The report contains PASS / FAIL / SKIP checks and non-secret endpoint metadata.
Passwords and TLS private-key material are not emitted.

## Automated checks

The qualifier covers:

- TDS LOGIN7 connection and server version;
- datasource migration prechecks;
- schema/table/column/PK metadata;
- bounded Full Reader sample;
- partition discovery and runtime load;
- View / Sequence / Trigger / Function / Procedure discovery and dependency metadata;
- optional SQL Server CDC current LSN and cleanup-retention check;
- optional target table creation with `IDENTITY(seed,increment)`;
- Full Writer + `IDENTITY_INSERT` lifecycle;
- exact DECIMAL values, large Unicode text and large VARBINARY round-trip;
- target transactional apply rollback and commit;
- delete and post-load index creation;
- automatic cleanup of the qualification table.

## Server matrix

Run source-only, CDC and target-write scopes as applicable across the SQL Server
versions QMigration intends to claim. Record:

- exact product version / edition;
- database compatibility level;
- encryption/TLS mode and certificate validation behavior;
- collation;
- CDC enabled state and cleanup retention;
- Always On / listener topology when used;
- reconnect/failover behavior;
- row counts and LOB sizes used in soak tests.

## Exit criteria

- zero FAIL checks for every required scope;
- no partial transaction acknowledged after target failure;
- `IDENTITY_INSERT` is always released, including error paths;
- large NVARCHAR/VARBINARY and DECIMAL remain byte/value correct;
- source CDC resumes from a durable LSN without duplicate-loss correctness failures;
- schema-object dependency ordering is deterministic; unsupported procedural conversion is manual, not guessed;
- qualification JSON reports are retained for every advertised deployment class.
