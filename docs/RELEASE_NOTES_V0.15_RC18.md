# QMigration V0.15.0-rc18 Release Notes

RC18 is a GBase 8a MPP Full-migration correctness release. It fixes the RC17 auto-target layout so QMigration never pairs retry-safe `MERGE` with an unqualified random/replicated target. GBase 8s and GBase 8c remain separate unsupported families.

## HASH distribution is now part of the Full Write contract

GBase 8a documentation and current application guidance constrain `MERGE` to HASH-distributed tables and require the MERGE condition to contain the distribution column. RC18 therefore treats the distribution layout as a correctness prerequisite rather than a performance hint.

For automatically created targets QMigration now:

1. requires a stable migration key;
2. scans migration-key columns in order;
3. chooses the first GBase HASH-eligible target type (integer, DECIMAL/NUMERIC, VARCHAR family);
4. creates `ENGINE=EXPRESS DISTRIBUTED BY('<key>')`;
5. immediately validates the actual DDL returned by `SHOW CREATE TABLE`.

Temporal, LOB/LONGTEXT and other non-HASH-compatible migration-key members are skipped. If a composite migration key has no eligible member, automatic target creation fails closed and requires a pre-created compatible HASH table.

## Pre-created target validation

Before the first Full Write performed by each fresh Connector/Worker, RC18 runs `SHOW CREATE TABLE` and parses the target distribution. Full Write is rejected before a staging table is created when:

- the target is random distribution;
- the target is `REPLICATED`;
- no HASH columns can be determined;
- any HASH distribution column is not included in the stable migration/MERGE key.

A successful layout check is cached only inside the live connector. A restarted Worker revalidates the target.

## Staging + MERGE semantics retained

After layout validation RC18 retains RC17's replay-safe batch path:

1. `CREATE TABLE stage LIKE target`;
2. load the batch into the private stage;
3. `MERGE INTO target USING stage` with the full migration key;
4. update only non-key columns;
5. insert missing rows;
6. drop the stage.

The selected HASH key is a subset of the full MERGE predicate, so RC18 never updates the distribution key.

## Qualification

`qmigration-gbase-qualify --target-write` now exercises HASH target creation, `SHOW CREATE TABLE` validation, replay MERGE and exact binary round trip. Real GBase 8a V9.5.x/topology/TLS/failure-window evidence is still required before production promotion.

## Deliberate boundaries

RC18 still does not advertise GBase 8a source CDC, durable source log positions, transactional target CDC apply, FK replay, AUTO_INCREMENT state restoration, GBase 8s or GBase 8c. This release hardens deterministic Full migration rather than broadening CDC claims.
