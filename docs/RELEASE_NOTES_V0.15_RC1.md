# QMigration V0.15.0-rc1 Release Notes

## Release purpose

V0.15.0-rc1 is the release-candidate packaging and qualification layer on top of
`0.15.0-unified-dev15-complete`. It does not add another Oracle migration data
path; instead it turns the completed native Oracle software path into a repeatable,
auditable real-instance qualification workflow.

Oracle production gates remain in place until representative customer Oracle
instances pass the matrix in `docs/ORACLE_NATIVE_QUALIFICATION.md`.

## Oracle qualification executable

Added `qmigration-oracle-qualify` and `deployments/scripts/qualify-oracle.sh`.

The tool produces a structured JSON report with PASS / FAIL / SKIP results and
never includes the Oracle password or private-key contents. By default the tool
is source/read-only and checks:

- Oracle Net/TNS/TTC connection and authentication;
- server/native protocol version;
- migration prechecks and database/NLS character-set visibility;
- schema/table/column metadata;
- bounded Full Reader sample;
- partition discovery;
- runtime-load sampling;
- schema-object discovery;
- optional LogMiner prerequisites and current SCN.

`--target-write` is explicitly destructive and creates a short-lived
`QMQUAL_*` table in the selected schema. It qualifies native bound/array-bound
Full Writer, prepared DML behavior, exact Oracle NUMBER round trip, large CLOB
and BLOB write/read, transactional rollback/commit, bound delete and post-load
index creation. The table is dropped with PURGE when the test finishes.

The qualification process enables experimental Oracle gates only inside its own
process; it does not edit QMigration service configuration.

## Release-build integration

`make backend-build` now builds `bin/qmigration-oracle-qualify` together with the
server, worker, CDC binaries and `qmigrationctl`.

## Archive verification correctness

`deployments/scripts/archive-version.sh` now distinguishes three Go verification
modes:

- default: tests/vet run again on both clean restored trees;
- `--preverified-go`: source was already fully tested/vetted and both restored
  trees are byte-for-byte equal to that source;
- `--no-go-verify`: Go verification is not claimed.

The manifest no longer reports `go_test=true` or `go_vet=true` when tests were
skipped without an explicit preverified-source assertion.

## Metadata

`032_v015_rc1_qualification.sql` advances the metadata schema marker to
`0.15.0-rc1`; no new persistent columns are required.
