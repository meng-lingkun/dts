# QMigration V1.0.0 RC2 Release Notes

V1.0.0 RC2 hardens the feature-complete RC1 line for production-style deployment and upgrade testing. The product model remains QMigration Control Plane + distributed Workers + pluggable Native/DataX/SeaTunnel/Flink/Debezium/Canal execution paths.

## CDC integration hardening

- Added built-in raw Debezium and Canal push-ingress endpoints.
- Added Debezium/Canal JSON normalization into the common `CDCEvent` model.
- Reused atomic target apply, durable checkpoint, DLQ, replay and conflict policy for pushed events.
- Added HTTP `425 Too Early` retry semantics so upstream capture does not ACK events before QMigration reaches a safe apply state.
- Fixed the first-push state transition so an unapplied Debezium/Canal record is never persisted as the CDC checkpoint; the state gate opens with a nil position and the normal apply path checkpoints only after target success.
- Debezium/Canal no longer depend on nonexistent local `qmigration-*-runner` binaries.

## Repository and mTLS correctness

- Fixed PostgreSQL `GetDataSource` projection drift that could misalign mTLS client certificate/private-key fields.
- Centralized the datasource SELECT projection and added a regression test to keep parser/columns aligned.
- Added `metadata_schema_state` and migration `016_v1_rc2_release_hardening.sql`.
- `/readyz` now checks the metadata repository and, for PostgreSQL repositories, verifies the durable metadata schema version matches the running binary.

## Graceful deployment lifecycle

- Server handles SIGINT/SIGTERM and drains HTTP requests with a 20-second shutdown window.
- Worker stops claiming new work on SIGINT/SIGTERM, cancels managed external CDC processes, and relies on durable cursor + lease recovery for interrupted chunks.
- Added configurable `QMIGRATION_WORKER_SHUTDOWN_GRACE_SECONDS`.
- Docker Compose and Kubernetes manifests now use `/readyz`, termination grace periods and Worker/Server PDBs.
- Added ordered transactional PostgreSQL metadata migration script: `deployments/scripts/migrate-metadata.sh`.

## Validation boundary

Backend unit/integration tests, Go vet, all Go binaries and the local API/Worker smoke test are release gates. The current build environment cannot resolve the npm registry, so a real Vue dependency install/production build cannot be claimed as passed until run in a network-enabled environment. No frontend compile error was observed; dependency acquisition is the blocker.
