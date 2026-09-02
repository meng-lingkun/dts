# openGauss / KingbaseES Source CDC Qualification

RC26 adds software paths but keeps both product CDC claims qualification-gated.
A retained report must be produced for every server release/topology QMigration
claims.

## openGauss

```bash
export OPENGAUSS_PASSWORD='***'
./bin/qmigration-opengauss-qualify \
  --host 10.0.0.10 --port 5432 --user repl --database app \
  --schema public --table orders --cdc --tls-mode REQUIRED \
  --output opengauss-rc26.json
```

The qualifier checks connection/version, logical-replication settings,
replication privilege, SSL, selected-table PK, current `OPENGAUSS_LSN`, a
temporary `mppdb_decoding` slot and a non-advancing selected-table logical peek.

Retain separate restart/failover evidence proving that target apply/checkpoint
occurs before source slot advance and that a Worker restart resumes from the
same durable slot without gaps.

## KingbaseES

```bash
export KINGBASE_PASSWORD='***'
./bin/qmigration-kingbase-qualify \
  --host 10.0.0.20 --port 54321 --user repl --database app \
  --schema public --table orders --cdc --tls-mode REQUIRED \
  --output kingbase-rc26.json
```

The qualifier checks connection/version, `wal_level`, sender/slot capacity,
replication privilege, selected-table PK, `KINGBASE_LSN`, a temporary
`kboutput` slot and a temporary selected-table publication. Cleanup removes
only qualifier-owned objects.

A real CDC qualification must additionally retain the actual `kboutput` stream
trace and prove that every message used by QMigration conforms to the strict
RC26 decoder. A slot whose plugin is not `kboutput` is rejected before stream
startup. Unknown/incompatible wire records must fail closed.

## Promotion criteria

Do not remove the experimental gates until:

- no FAIL checks exist in the retained release/topology matrix;
- restart, source primary failover and network interruption are exercised;
- UPDATE/DELETE key identity and large/NULL/text values round-trip correctly;
- unsupported binary/DDL/complex cases fail before durable checkpoint advance;
- source log retention gaps are detected rather than silently skipped;
- repeated reconnect/ACK tests show no committed source transaction is lost.
