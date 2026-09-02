# QMigration V0.15.0-rc26 Release Notes

## Scope

RC26 closes the previous PostgreSQL-wire derivative Full-only gap without
pretending that every PostgreSQL-compatible database uses PostgreSQL pgoutput.
It adds separate, qualification-gated source CDC paths for openGauss and
KingbaseES, each with a product-specific durable position type and source
prechecks.

## openGauss source CDC

- `QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC=1` exposes source CDC only
  after explicit qualification.
- Creates a native `mppdb_decoding` logical slot and persists
  `OPENGAUSS_LSN`.
- Uses `pg_logical_slot_peek_changes` to read complete selected-table
  transactions without advancing the slot.
- Parses BEGIN/COMMIT/XID and JSON INSERT/UPDATE/DELETE row changes.
- Target transaction + QMigration durable checkpoint complete before
  `pg_logical_slot_get_changes(... up_to_lsn ...)` advances the source slot.
- Primary keys are required for deterministic UPDATE/DELETE apply.
- Binary/NUL-sensitive values, malformed/partial transactions and unsupported
  logical records fail closed.
- Prechecks cover `wal_level=logical`, replication slots/senders, replication
  privilege and the openGauss SSL prerequisite.
- Adds `qmigration-opengauss-cdc` and `qmigration-opengauss-qualify`.

## KingbaseES source CDC

- `QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC=1` exposes source CDC only
  after explicit qualification.
- Uses Kingbase `sys_current_wal_lsn`, `sys_create_logical_replication_slot`,
  `sys_drop_replication_slot`, `sys_replication_slots` and `sys_publication`.
- Creates the vendor `kboutput` output plugin, not PostgreSQL `pgoutput`.
- Persists `KINGBASE_LSN`; event IDs use an independent `kingbase:` namespace.
- Managed CDC creates/updates a selected-table publication and streams through
  the PostgreSQL-compatible replication transport.
- Before opening a stream, QMigration proves that the durable slot still uses
  `kboutput`; a replaced/mismatched slot fails closed.
- The decoder is strict: unknown or incompatible `kboutput` wire records fail
  rather than being silently coerced. Real-version `kboutput` wire conformance
  remains a retained qualification gate.
- Adds `qmigration-kingbase-qualify`; the existing PostgreSQL CDC worker is
  reused only as transport/runtime, with explicit Kingbase source dialect.

## Safety and compatibility boundary

- openGauss and Kingbase remain independent connector products and position
  types; neither is relabeled as PostgreSQL CDC.
- RC26 does not claim openGauss binary-value logical decoding, source DDL, or
  multi-primary behavior.
- RC26 does not claim Kingbase source DDL/TRUNCATE or production `kboutput`
  compatibility without retained real-instance wire traces and restart/failover
  qualification.
- Exact historical validation remains available for TiDB/Oracle/DM8 from RC25;
  openGauss/Kingbase exact-at-LSN historical snapshot validation is not claimed.
