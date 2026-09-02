# QMigration V0.15.0-rc24 Support Matrix

| Database | Metadata | Full Read | Full Write / CDC Apply | Source CDC | Schema / DDL | Status |
|---|---:|---:|---:|---|---:|---|
| MySQL / MariaDB / PolarDB MySQL / PolarDB-X | Yes | Yes | Yes | Native MySQL-compatible binlog paths | Yes | NATIVE / prerequisite-specific |
| TiDB | Yes | Yes | Yes | TiCDC + native Kafka Canal-JSON | Yes | EXPERIMENTAL |
| OceanBase MySQL | Yes | Yes | Yes | Binlog Service / ODP | Yes | EXPERIMENTAL |
| PostgreSQL / PolarDB PostgreSQL | Yes | Yes | Yes | pgoutput | Yes | NATIVE |
| openGauss / Kingbase | Yes | Yes | Yes | Not advertised | Yes | NATIVE_FULL_ONLY |
| Oracle | Yes | Yes | Yes | LogMiner / SCN | Yes | EXPERIMENTAL |
| SQL Server | Yes | Yes | Yes | SQL Server CDC / LSN | Yes | EXPERIMENTAL |
| DB2 LUW | Yes | Yes | Yes | QMigration Log Agent + IBM db2ReadLog | Yes | EXPERIMENTAL |
| Dameng / DM8 | Yes | Yes | Yes | Not advertised | Table/PK/index/FK target | EXPERIMENTAL |
| GaussDB | Yes | Yes | Yes | mppdb_decoding binary DML + optional DDL-only classification / GAUSSDB_LSN | Target yes; selected-table DDL-only same-family replay | EXPERIMENTAL |
| GBase 8a MPP Cluster | Yes | Yes | Full Write only; validated HASH staging+MERGE; no CDC apply | Not advertised | Table/PK create only | EXPERIMENTAL / FULL_ONLY |
| **GBase 8s V8.8** | **Yes** | **Yes** | **Full Write + transactional target CDC Apply** | **syscdcv1/CSDK smart-LOB via native C ABI v3 provider; GBASE8S_CDC_SEQ** | **Target owner check + table/PK/index/FK apply** | **EXPERIMENTAL / qualification required** |

## GBase 8s RC24 scope

Behind `QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE=1` and
`QMIGRATION_EXPERIMENTAL_GBASE8S_CDC=1`:

- RC19 CSDK/ODBC Full + target data plane remains unchanged;
- RC20 transaction-aware source CDC remains apply-before-ACK with
  `restart=<earliest-open-BEGIN>;commit=<last-applied-COMMIT>`;
- RC24 requires a Linux native C ABI v3 `.so` (or updated legacy provider) loaded by
  `qmigration-gbase8s-cdc-agent`;
- legacy Go provider plugin remains compatibility-only;
- optional SHA-256 pinning protects the native provider artifact;
- provider calls are serialized and remote agent traffic requires TLS + token;
- provider row images are checked for complete selected columns, order,
  NULL/encoding correctness and response limits;
- empty committed transactions emit CHECKPOINT events so the durable watermark
  continues to advance;
- RC22 supports documented transactional source TRUNCATE and GBase 8s transactional target TRUNCATE replay.
- RC23 schema fingerprints remain mandatory; RC24 additionally requires Agent API v3 plus a 64-hex capture lineage persisted in every durable GBASE8S_CDC_SEQ checkpoint and verified on every read. `/v1/status` and `/metrics` observability are restored.

Fail-closed/not advertised:

- production maturity without retained real GBase 8s/CSDK/syscdcv1 evidence;
- smart BLOB/CLOB source values and unsupported complex/opaque/collection types;
- keyless CDC tables;
- QMigration SQL TLS `PREFERRED/REQUIRED` until CSDK SQL SSL mapping is qualified;
- quoted/case-sensitive identifier behavior outside the safe subset.

## Remaining highest-priority gaps

1. Build the native C ABI v3 provider against a real GBase 8s V8.8 Client-SDK and retain schema-fence + long-transaction/restart/failover qualification.
2. Retain source TRUNCATE commit/rollback/restart qualification; smart BLOB/CLOB retrieval remains a separate future path.
3. Retained GBase 8a Full/HASH-MERGE workload qualification.
4. Retained GaussDB Full/binary CDC/DDL-only qualification and multi-primary.
5. Retained Dameng and DB2 qualification work.
