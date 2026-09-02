# GBase 8s CSDK CDC provider

QMigration RC28 supports two datasource-local provider mechanisms for the
`syscdcv1` smart-LOB CDC stream:

1. **Native C ABI v4 (recommended)** — build a normal Linux `.so` with the local
   GBase 8s Client-SDK / ESQL-C toolchain. The QMigration agent loads it with
   `dlopen`, so the provider does not need the same Go toolchain as QMigration.
2. Legacy Go `buildmode=plugin` — retained for RC20 compatibility only.

The vendor CSDK itself is not shipped by QMigration.

## Native C ABI

Implement the symbols in:

- `qmigration_gbase8s_cdc_provider.h`

A compileable stub is provided as `native_provider.c.example`. Verify the ABI
skeleton on the target Linux toolchain with:

```bash
./deployments/scripts/build-gbase8s-cdc-native-provider-example.sh
```

Then compile your real provider with the matching GBase CSDK. The provider owns:

- connection credentials and Client-SDK session state;
- `cdc_opensess()` / `cdc_startcapture()` / `cdc_activatesess()`;
- `ifx_lo_read()` or the supported equivalent smart-LOB read call;
- fragmented smart-LOB read reassembly into complete CDC records;
- exact record sequence / transaction / `user_data` preservation;
- lossless conversion of CDC row values into the QMigration `CDCField` JSON
  contract (`base64` for arbitrary bytes).

QMigration still owns transaction assembly, long-transaction restart
watermarks, duplicate suppression, apply/checkpoint order and target apply.

Configure the agent:

```bash
export QMIGRATION_GBASE8S_CDC_PROVIDER_LIBRARY=/opt/qmigration/lib/qm-gbase8s-cdc-provider.so
# Strongly recommended: pin the exact native provider artifact.
export QMIGRATION_GBASE8S_CDC_PROVIDER_SHA256='<64-hex-sha256>'

# Optional local provider configuration. Use either direct JSON or a config file.
# A config file must not be accessible by "other" users.
export QMIGRATION_GBASE8S_CDC_PROVIDER_CONFIG_FILE=/etc/qmigration/gbase8s-cdc-provider.json

export QMIGRATION_GBASE8S_CDC_AGENT_TOKEN='...'
./bin/qmigration-gbase8s-cdc-agent
```

`QMIGRATION_GBASE8S_CDC_PROVIDER_LIBRARY` must be an absolute, regular,
non-world-writable file. If SHA-256 pinning is configured, a mismatch fails
before `dlopen`.

The agent serializes Health/Checkpoint/Read calls for a datasource-local
provider handle. A single CSDK smart-LOB session is therefore never read
concurrently by multiple HTTP requests.


### RC28 schema-fence + capture-lineage + smart-LOB image requirement

The provider must validate the initial `CDC_REC_TABSCHEMA` plus current catalog metadata and return one matching `schema_fences` entry for every requested table in both checkpoint and read responses. The selection request carries `schema_columns`, `primary_keys`, and the QMigration canonical `schema_fingerprint`. Do not merely echo the requested fingerprint: the provider is responsible for proving that the live source schema matches it.

Native ABI v1/v2/v3 providers from RC21-RC24 are intentionally rejected and must be rebuilt against the ABI v4 header.

The provider must also return a 64-hex `capture_lineage` at checkpoint and on every read. It must validate `expected_capture_lineage` before returning records. Resume of the same logical capture may keep the lineage; recreation/replacement must generate a different lineage so QMigration fails closed rather than attaching an old checkpoint to a new stream.

## Legacy Go plugin

The legacy Go interface remains available through:

```bash
export GBASE8S_CDC_PROVIDER_DIR=/opt/src/qmigration-gbase8s-csdk-provider
./deployments/scripts/build-gbase8s-cdc-provider-plugin.sh
export QMIGRATION_GBASE8S_CDC_PROVIDER_PLUGIN=$PWD/bin/qmigration-gbase8s-cdc-provider.so
```

Do not configure both the native C ABI library and the legacy Go plugin. The Go
plugin requires the exact compatible Go toolchain and is no longer the preferred
provider integration path.

Keep the agent loopback-local or use its TLS certificate/key options when it
crosses a host boundary. Do not put source database credentials into the
QMigration datasource `cdc_url`.
### RC28 event-owned smart BLOB/CLOB contract

When `schema_columns[].smart_lob` is `blob` or `clob`, checkpoint/read responses must declare `smart_lob_image_contract=cdc-event-owned-lob-v1`. Every non-NULL LOB field in a normalized row image must include one `smart_lob_proofs` entry carrying column, kind, exact byte length, SHA-256 and `acquisition=cdc-event-owned-lob-v1`. The bytes must come from the CDC event-owned CSDK locator/stream; querying the current table row by primary key is forbidden because it cannot prove the historical image under CDC lag.

RC28 still uses bounded complete normalized records. Values larger than the configured provider/Agent response budget must fail closed; this ABI does not claim unlimited LOB streaming.
