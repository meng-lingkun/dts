# OceanBase Binlog Service Native Qualification

## Scope

RC4 adds QMigration-owned OceanBase MySQL source CDC by combining the existing
native MySQL Binlog V4/GTID decoder with an explicit OceanBase Binlog Service
subscription endpoint routed through ODP.

The SQL/full-load endpoint and CDC endpoint are deliberately separate:

```text
OceanBase datasource host:port
        -> Metadata / Full Read / target apply

cdc_url=obbinlog://odp1:2883?fallback=odp2:2883
        -> tenant ODP -> OceanBase Binlog Service -> MySQL Binlog V4/GTID
```

Do **not** put credentials in `cdc_url`; QMigration reuses the encrypted tenant
datasource credential. `obbinlogs://` forces TLS for the CDC path and may carry a
non-secret shared certificate name using `server_name=`.

## Why ODP is the subscription endpoint

OceanBase documents downstream subscription as a MySQL Binlog Dump command sent
through ODP for a specific tenant, with ODP forwarding the request to Binlog
Service. Binlog Server's commonly configured `2983` port is a service/management
listener and must not be assumed to be the tenant subscription endpoint.

## Supported software path

- MySQL Binlog V4 event framing.
- RBR Write/Update/Delete row events.
- Format Description / Previous GTIDs / Rotate / GTID / Query / Table Map.
- MySQL-native DDL Query events emitted by OceanBase Binlog Service.
- GTID preferred durable restart; file/position fallback.
- apply-before-ACK via QMigration Unified CDC Runtime.
- reconnect from the last acknowledged durable position.
- up to 8 ODP subscription endpoints (primary + 7 fallbacks).
- plaintext `obbinlog://` or TLS `obbinlogs://`.

## One-command qualification

Build binaries:

```bash
make backend-build
```

Run:

```bash
export OCEANBASE_HOST=obproxy-sql.example
export OCEANBASE_PORT=2883
export OCEANBASE_DATABASE=app
export OCEANBASE_USER='root@app_tenant'
export OCEANBASE_PASSWORD='***'
export OCEANBASE_BINLOG_URL='obbinlog://odp1.example:2883?fallback=odp2.example:2883'

deployments/scripts/qualify-oceanbase.sh
```

The tool is read-only. It validates:

1. OceanBase SQL connection/version;
2. table metadata and an optional Full Read sample;
3. `cdc_url` syntax;
4. ODP subscription authentication;
5. `SHOW MASTER STATUS` / GTID or file-position capture;
6. `SHOW BINARY LOGS` visibility;
7. connector capability/maturity output.

Optional JSON output:

```bash
OCEANBASE_QUALIFY_OUTPUT=/tmp/oceanbase-binlog-qualification.json \
  deployments/scripts/qualify-oceanbase.sh
```

## Real migration qualification matrix

Retain evidence for every claimed OceanBase/Binlog/ODP combination:

- OceanBase CE/EE versions used by customers;
- Binlog Service release and obcdc compatibility;
- ODP release;
- plain/TLS subscription;
- GTID restart across Rotate;
- primary ODP disconnect and fallback ODP reconnect;
- Binlog instance failover behind ODP;
- long transaction and large row-event soak;
- DDL during Full+CDC;
- source restart / Binlog Service restart / Worker restart;
- cutover, reverse sync and rollback;
- binlog retention gap / purged GTID failure behavior.

## Fail-closed boundaries

- Missing/invalid `cdc_url` prevents CDC planning/precheck.
- QMigration never guesses the Binlog Service subscription address from the SQL endpoint.
- Credentials embedded in `cdc_url` are rejected.
- If all configured ODP endpoints are unavailable, the reader stops at the last durable checkpoint.
- Unsupported row/event types fail before QMigration advances the source checkpoint.
- OceanBase-only MySQL-incompatible data types must be qualified explicitly before production CDC.

## Exit criteria for removing EXPERIMENTAL maturity

- Retained qualification reports for the supported OceanBase/Binlog/ODP matrix.
- No duplicate/lost transaction across ODP and Binlog instance failover.
- GTID and file-position restart behavior verified against retention/purge scenarios.
- DDL and supported MySQL-compatible data types verified end to end.
- TLS certificate/hostname validation and reconnect verified when TLS is claimed.
