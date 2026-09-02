# GBase 8a Native Qualification

RC18 exposes the GBase 8a Full data plane only behind:

```bash
export QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE=1
```

This document applies to **GBase 8a MPP Cluster only**. Do not use an RC18
qualification result to claim GBase 8s or GBase 8c support.

## One-command qualification

```bash
export GBASE_HOST=10.0.0.10
export GBASE_PORT=5258
export GBASE_USER=qmigration
export GBASE_PASSWORD='***'
export GBASE_DATABASE=app
export GBASE_TABLE=orders

deployments/scripts/qualify-gbase.sh
```

Optional destructive target qualification:

```bash
GBASE_QUALIFY_TARGET_WRITE=1 deployments/scripts/qualify-gbase.sh
```

Optional controls:

```bash
export GBASE_SAMPLE_ROWS=16
export GBASE_QUALIFY_TIMEOUT=90s
export GBASE_QUALIFY_OUTPUT=/secure/path/gbase8a-qualification.json
```

The report does not include the password.

## What the tool verifies

Read-only mode checks:

1. native packet authentication and `SELECT 1`;
2. `SELECT VERSION()`;
3. GBase-specific migration prechecks and character-set variables;
4. visible application schemas;
5. optional table metadata and migration key;
6. optional bounded Full Read;
7. optional keyset boundary planning.

`--target-write` additionally:

1. creates an `ENGINE=EXPRESS` HASH-distributed qualification table using the stable key;
2. validates the real `SHOW CREATE TABLE` distribution and writes a row through staging+MERGE;
3. replays the same migration key with an updated value;
4. reads the table back and requires one logical row with the updated value;
5. validates a binary payload round-trip;
6. drops the qualification table.

## Required retained evidence before production promotion

Retain JSON reports and operational notes for every supported deployment family:

1. exact GBase 8a version/build and gcluster/gnode topology;
2. authentication mode and SQL endpoint failover behavior;
3. UTF-8/UTF8MB4 and any required GBK/GB18030 workload;
4. metadata against representative EXPRESS tables and composite keys;
5. long-running Full Read with worker restart/resume;
6. target staging+MERGE under worker kill/retry around stage load and MERGE;
7. BLOB/LONGBLOB, LONGTEXT, DECIMAL boundary and temporal values;
8. pre-created workload-specific HASH targets whose distribution columns are contained in the stable migration key;
9. concurrent application workload effects and MERGE performance;
10. TLS behavior if the deployment requires encrypted SQL transport.

## Deliberate RC18 exclusions

- no GBase 8a source CDC or source-log checkpoint API is claimed;
- no target transactional CDC Apply is claimed;
- no FK replay/enforcement is claimed;
- automatic targets choose only a HASH-eligible stable migration-key member; workload-specific alternative HASH keys still require a pre-created target;
- no implicit AUTO_INCREMENT runtime-state restoration is performed;
- no GBase 8s / GBase 8c compatibility claim is made.

For production migration, pre-create the target table when a workload-specific HASH layout is required. RC18 validates the real target with `SHOW CREATE TABLE`; every HASH distribution column must also be part of QMigration's stable migration key. Random and REPLICATED targets are rejected for retryable MERGE Full Write because the GBase MERGE path requires HASH distribution.
