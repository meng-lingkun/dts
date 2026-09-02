# QMigration V0.13.0 Release Notes

## UNIQUE NOT NULL Native Migration Key

V0.13 allows resumable Native full-load migration for tables without a primary key when a safe UNIQUE NOT NULL key exists.

### Added

- Primary-key-first stable Migration Key selection.
- UNIQUE NOT NULL fallback, including composite unique indexes.
- Durable Generic Keyset Cursor for unique-key tables.
- Column-mapped target Migration Keys.
- Pre-load UNIQUE creation for auto-created target tables.
- Existing-target constraint verification to preserve idempotent retry semantics.

### Safety

Nullable or generated-column unique indexes are never used as Native resume keys. Tables with no safe primary/unique key remain external-engine workloads; QMigration does not fall back to OFFSET-based pseudo-checkpointing.
