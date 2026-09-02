# Exact Watermark Validation

QMigration RC25 adds a connector-level exact historical validation snapshot SPI
and implements it for TiDB, Oracle and Dameng/DM8.

## Problem solved

The previous validation barrier waited for an empty CDC spool and a stable
applied checkpoint, but target CDC apply was still considered runnable while
`VALIDATING`. That created two problems:

1. the scheduler could route a transaction to target apply while the apply API
   itself rejected the `VALIDATING` state;
2. even if it succeeded, source and target scans could observe different online
   write points.

RC25 changes the lifecycle to:

```text
CDC_CATCHING_UP
  -> acquire forward CDC spool/apply lock
  -> verify spool empty + lag/quiet-window gates
  -> capture durable apply barrier
  -> transition VALIDATING while the same lock is held
  -> release lock

VALIDATING:
  source CDC capture -> Durable CDC Spool -> ACK source
  target CDC apply   -> FROZEN
  validation target  -> barrier state
  exact source reads -> TiDB TSO / Oracle SCN / DM_LSN snapshot

validation complete:
  -> CDC_CATCHING_UP
  -> drain validation-window spool in durable order
```

This makes target freezing atomic with barrier capture and prevents a CDC
transaction from slipping between the two operations.

## Connector SPI

A source can implement `connector.ValidationSnapshotConnector` and advertise
`validation-snapshot`. The connector receives the durable `CDCPosition` and
must return a fresh `DataConnector` whose subsequent reads are pinned to that
exact historical position. If it cannot prove that contract it must fail.

Set:

```bash
QMIGRATION_VALIDATION_REQUIRE_EXACT_WATERMARK=1
```

to reject validation for connectors that do not implement this capability.
Without this flag, connectors without an exact snapshot retain the existing empty-spool + lag +
stable-window barrier and post-scan barrier-drift protection.

## TiDB implementation

For a `TIDB_TSO` barrier QMigration parses the TSO from the durable position,
opens an independent TiDB SQL connection, executes:

```sql
SET SESSION tidb_snapshot='<TSO>';
```

and verifies `SELECT @@tidb_snapshot` returns the same TSO. All validation
`SELECT`s then reuse that session. An expired/GC-collected historical version,
wrong position type or mismatched session value fails validation closed.

## Implemented exact snapshot families

### TiDB — `TIDB_TSO`

A fresh SQL connection executes `SET SESSION tidb_snapshot='<barrier TSO>'`,
verifies `@@tidb_snapshot`, and all validation reads stay on that connection.

### Oracle — `ORACLE_SCN`

With the Oracle LogMiner gate enabled, a fresh native connector renders table
references with `AS OF SCN <barrier>`. The connector is read-only; expired or
unavailable Flashback/UNDO history aborts validation.

### Dameng / DM8 — `DM_LSN`

With the DM LogMiner CDC gate enabled, a fresh connector renders source reads as
`AS OF SCN <barrier LSN>`. DM documents SCN/LSN operands for flashback queries.
The same mechanism is also used by the DM source CDC reader to reconstruct full
row before/after images around a committed LogMiner LSN. Expired UNDO, MPP/DPC
limitations or any unsupported flashback case fail closed.

## Remaining exact-snapshot families

MySQL/PolarDB-X GTID, PostgreSQL-family LSN, SQL Server LSN, DB2 LRI, GaussDB
LSN, OceanBase GTID and GBase 8s sequence positions still use the stable barrier
contract unless/until a source-native historical snapshot can be tied to the
exact durable CDC watermark. `QMIGRATION_VALIDATION_REQUIRE_EXACT_WATERMARK=1`
continues to reject those paths rather than silently weakening the guarantee.
