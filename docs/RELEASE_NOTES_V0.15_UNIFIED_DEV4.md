# QMigration V0.15.0-unified-dev4 Release Notes

## Focus

Dev4 continues the single QMigration Unified Engine direction. No DataX,
SeaTunnel, Flink CDC, Debezium or Canal runtime is reintroduced.

## SQL Server Native TDS/TLS

- Added TDS 7.x TLS negotiation using PRELOGIN-wrapped TLS handshake records.
- `REQUIRED` never downgrades when the server cannot negotiate encryption.
- `PREFERRED` implements SQL Server login-only TLS when the server returns
  `ENCRYPT_OFF`, then returns to plaintext exactly after encrypted LOGIN7.
- Full-connection TLS is used for server `ENCRYPT_ON/ENCRYPT_REQ`.
- Added custom CA, server-name verification and optional mTLS client cert/key.
- SQL Server datasource TLS now defaults to `PREFERRED`.

## SQL Server Native CDC/LSN (experimental gate)

Enable with:

```bash
QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC=1
```

Implemented inside QMigration:

- captures `sys.fn_cdc_get_max_lsn()` before Full Load;
- discovers `cdc.change_tables` capture instances for selected tables;
- validates every selected table has a capture instance before snapshot copy;
- reads bounded transaction windows from `cdc.lsn_time_mapping`;
- calls native `cdc.fn_cdc_get_all_changes_<capture_instance>` functions;
- normalizes INSERT / DELETE / UPDATE old+new images into `CDCEvent`;
- groups changes by `__$start_lsn` as the SQL Server transaction checkpoint;
- preserves binary values using base64 CDC fields;
- validates minimum retained LSN to detect CDC retention gaps early;
- feeds the shared QMigration `cdc/runtime.Runner`;
- source cursor advances only after target Apply + durable QMigration checkpoint.

Added built-in binary:

```text
qmigration-sqlserver-cdc
```

No Debezium, SSIS or external migration runtime is required.

## Worker / managed CDC fix

Managed CDC subprocesses now receive `QMIGRATION_TASK_ID`. This fixes a latent
issue where the existing MySQL/PostgreSQL managed CDC binaries could be claimed
by a Worker but fail immediately because the task ID was not injected.

## Oracle Native TNS

- Native TNS probe now follows listener `REDIRECT` responses (RAC/SCAN style)
  with a bounded redirect count.
- ACCEPT protocol version is surfaced when present.
- Oracle authentication/full-load/Redo CDC remain intentionally gated.

## Metadata

New migration:

```text
020_v015_sqlserver_native_cdc.sql
```

Metadata schema version:

```text
0.15.0-unified-dev4
```

## Safety boundary

SQL Server Full and source CDC remain experimental until qualified against real
SQL Server versions and long-running workloads. Oracle remains protocol-probe
only. Capability SPI prevents either path from being silently advertised beyond
what is implemented.
