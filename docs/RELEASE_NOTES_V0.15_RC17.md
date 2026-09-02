# QMigration V0.15.0-rc17 Release Notes

RC17 adds the first qualification-gated QMigration Native data plane for
**GBase 8a MPP Cluster**. The scope is deliberately limited to deterministic
Full migration. GBase 8s and GBase 8c are different product families and are
not covered by this connector.

## GBase 8a connector

- Added a distinct `gbase8a` Connector Factory rather than routing GBase through
  the former generic/external-JDBC placeholder.
- QMigration owns catalog discovery, Full Read/Write, keyset planning, schema
  conversion, retry semantics and qualification. No DataX/SeaTunnel/Flink/JDBC
  migration runtime is launched.
- The connector reuses QMigration's audited MySQL/GBase-compatible packet
  transport because GBase 8a application drivers use the GBase/MySQL-style SQL
  protocol family; it does not inherit MySQL Binlog CDC capability.
- Native capabilities are behind `QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE=1` and
  remain `EXPERIMENTAL / QualificationRequired`.

## Metadata and source Full Read

RC17 provides:

- schema/table/column/primary-key/index metadata through `information_schema`;
- numeric and composite migration-key Full Read;
- ordered bounded-keyset reads and NTILE boundary planning;
- utf8mb4 session setup over the packet transport;
- migration prechecks for server version, character set and unsupported CDC.

The source still requires the QMigration-wide stable migration-key rule: a
primary key or UNIQUE NOT NULL key is needed for resumable Full migration.

## Target schema and idempotent Full Write

GBase 8a is columnar/MPP and must not inherit MySQL target assumptions.
RC17 therefore adds a GBase-specific target path:

- auto-created tables use `ENGINE=EXPRESS DEFAULT CHARSET=utf8mb4`;
- no `DISTRIBUTED BY` clause is guessed, so an auto-created table uses GBase 8a
  random distribution;
- production HASH/REPLICATED placement should be pre-created when workload-aware
  distribution is required;
- source `AUTO_INCREMENT` is not copied to an automatically created target,
  because Full migration writes explicit source values and GBase 8a deployments
  can reject explicit writes to auto-increment columns unless separately
  configured;
- VARCHAR/DECIMAL/BLOB/TEXT mappings respect the RC17 GBase 8a target limits.

Full Write does **not** use MySQL `ON DUPLICATE KEY UPDATE`. GBase 8a primary-key
syntax is not treated as an enforced uniqueness contract by QMigration. Every
batch instead uses a private staging table and GBase `MERGE` keyed by the stable
migration key:

1. `CREATE TABLE stage LIKE target`;
2. load the batch into `stage`;
3. `MERGE INTO target USING stage` with key equality;
4. drop `stage`.

A keyless GBase target write is rejected rather than accepting non-idempotent
lease retry behavior.

## Deliberate capability boundaries

RC17 does not advertise:

- GBase 8a source CDC;
- transactional target CDC apply;
- foreign-key replay/enforcement;
- post-load schema-object replay;
- GBase 8s or GBase 8c compatibility;
- production TLS/topology compatibility before retained real-instance reports.

`FULL+CDC` and `CDC` plans with GBase 8a as the source or CDC target therefore
fail capability validation rather than falling back to another migration
runtime.

## Qualification workflow

Added:

- `qmigration-gbase-qualify`;
- `deployments/scripts/qualify-gbase.sh`;
- `docs/GBASE8A_NATIVE_QUALIFICATION.md`.

Read-only qualification verifies packet authentication, server version,
character set, metadata, sample Full Read and keyset boundaries. Optional
`--target-write` creates an EXPRESS table, writes the same migration key twice
through staging+MERGE, verifies one updated logical row plus binary round-trip,
and drops the qualification table.
