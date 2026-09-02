# QMigration V1.0.0 RC2 Build Verification

## Passed in the release workspace

```text
go test ./...                 PASS
go vet ./...                  PASS
Go binary build               PASS
Local Server + Worker smoke   PASS
SIGTERM graceful shutdown     PASS
Docker Compose YAML parse     PASS
Kubernetes YAML parse         PASS
Prometheus rules YAML parse   PASS
Shell syntax checks           PASS
```

Built Go programs:

- `qmigration-server`
- `qmigration-worker`
- `qmigration-cdc-bridge`
- `qmigration-binlog-inspect`
- `qmigration-postgres-cdc`
- `qmigration-mysql-cdc`
- `qmigrationctl`

The smoke test verifies the health/version response, datasource and migration APIs, Worker registration, Dashboard data, and the engine registry including `debezium` and `canal`.

## Push CDC regression gates

RC2 contains dedicated tests proving:

- malformed Debezium events without a durable position are rejected;
- a full-load `425 Too Early` response does not create a CDC checkpoint;
- retrying that same held record after the apply gate writes it exactly once and then checkpoints it;
- an incremental task's first pushed Canal record is applied before its checkpoint is stored;
- retry of an already applied durable position is deduplicated without a second target write.

## Environment-limited checks

### Vue production build

The release workspace cannot resolve the npm registry and contains no pre-existing `node_modules` or npm cache. Therefore a real `npm install/npm ci + npm run build` could not be completed and is **not** marked PASS. `deployments/Dockerfile.web` prefers `npm ci` when a lockfile is available and otherwise falls back to `npm install`.

### Docker database E2E

`scripts-e2e.sh` is present and uses the real database integration test suite, but the release workspace does not provide Docker/Podman. The Docker E2E suite is therefore **not** marked PASS in this environment.

### PostgreSQL migration execution

The ordered migration helper and SQL are syntax/static-reviewed, but this workspace does not provide `psql`/PostgreSQL. Production/staging release validation should execute `deployments/scripts/migrate-metadata.sh` against a PostgreSQL copy of metadata before rollout.
