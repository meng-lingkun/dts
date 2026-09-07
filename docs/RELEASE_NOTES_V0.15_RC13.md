# DTS V0.15.0-rc13 Release Notes

RC13 adds the first DTS Dameng / DM8 migration data plane while preserving the project's correctness-first capability contract.

## Dameng Metadata and Full Load

- Add a DTS Dameng Connector behind `DTS_EXPERIMENTAL_DAMENG_NATIVE=1`.
- Discover schemas, tables, columns, primary keys, indexes and foreign keys through DM catalog views.
- Add numeric/composite keyset reads and ordered NTILE keyset-boundary planning.
- Remove Dameng from the old generic external-JDBC placeholder path; GaussDB and GBase remain unchanged.

## Dameng target path

- Create schema/table/composite-PK/index/FK objects.
- Use prepared INSERT for keyless tables and prepared MERGE for keyed idempotent Full/CDC target writes.
- Keep row values out of generated SQL and validate numeric values before binding.
- Add BLOB/binary parameters, point lookup/delete and explicit target transactions.

## Driver-provider boundary

- DTS does not vendor the proprietary DM Go wire driver.
- Linux Server/Worker can load a Go provider plugin through `DTS_DAMENG_DRIVER_PLUGIN`; the provider's init registers the DM `database/sql` driver.
- Missing provider registration fails explicitly rather than silently reverting to a generic connector.
- `PREFERRED`/`REQUIRED` Dameng TLS modes fail closed in RC13 until provider-specific TLS properties are qualified.

## Qualification

- Add `dts-dameng-qualify`, `qualify-dameng.sh` and a provider-plugin build helper.
- Read-only qualification covers connection/version/catalog/full-read sampling.
- Optional target qualification covers temporary table creation, prepared MERGE/BLOB round trip and delete/drop cleanup.
- Dameng source CDC is **not advertised** in RC13.
