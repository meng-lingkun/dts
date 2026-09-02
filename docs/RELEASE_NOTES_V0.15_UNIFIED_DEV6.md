# QMigration V0.15.0-unified-dev6 Release Notes

## Theme

**Durable CDC staging during Full Snapshot.** QMigration now owns the long-running Snapshot+CDC bridge instead of relying on source log retention for the complete snapshot duration.

## Durable CDC Spool

```text
Source CDC -> QMigration transaction -> compress -> encrypt -> durable spool -> source ACK
                                                           |
Full Snapshot ---------------------------------------------+
                                                           ↓
                                                   CDC catch-up
                                                           ↓
                                               ordered target apply
                                                           ↓
                                                target checkpoint
```

- Native CDC capture starts while Full Load is running.
- A source transaction is acknowledged only after durable spool persistence.
- Spool payloads are gzip-compressed and AES-256-GCM encrypted through the existing secure repository.
- Target CDC checkpoint remains strictly apply-after-commit; staging never pretends target apply has happened.
- Reclaimed CDC Workers resume from the newest pending spool position.
- Backlog drains in monotonic sequence and live CDC cannot overtake it.
- Duplicate source-position retries reuse the same deterministic spool record.

## Capacity / Operations

- `QMIGRATION_CDC_SPOOL_MAX_TRANSACTION_BYTES` — default `16777216` (16 MiB).
- `QMIGRATION_CDC_SPOOL_MAX_PENDING_BYTES` — default `68719476736` (64 GiB).
- `QMIGRATION_CDC_SPOOL_DRAIN_PER_REQUEST` — default `1000`.
- `QMIGRATION_CDC_SPOOL_KEEP_APPLIED` — default `1000`.
- `QMIGRATION_VALIDATION_MAX_CDC_LAG_MS` — default `5000`.

If a spool capacity limit would be exceeded, QMigration fails the staging request and **does not ACK the source position**.

## Lifecycle correction

Full+CDC now follows:

```text
FULL_MIGRATING + CDC capture/staging
        ↓
FULL_FINISHED
        ↓
CDC_CATCHING_UP / spool drain
        ↓ empty backlog + lag gate
VALIDATING
        ↓
CDC_CATCHING_UP
        ↓
READY_FOR_CUTOVER
```

This prevents automatic validation while the target is knowingly behind a durable CDC backlog. Exact transaction-watermark online validation remains database-specific future work when source writes continue during validation.

## SQL Server retention semantics

With durable staging active, SQL Server CDC cleanup retention no longer needs to cover the entire Full Snapshot. It remains an operational safety window for CDC reader outages and spool backpressure. The existing configurable minimum-retention precheck remains enabled.

## Metadata

- Added `cdc_spool` metadata table.
- Added migration `022_v015_durable_cdc_spool.sql`.
- Runtime version: `0.15.0-unified-dev6`.
- Metadata schema version: `0.15.0-unified-dev6`.
