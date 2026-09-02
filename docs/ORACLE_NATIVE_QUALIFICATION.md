# Oracle Native real-instance qualification matrix

`0.15.0-rc1` contains the complete QMigration-owned Oracle software data plane
plus a repeatable qualification executable. Experimental gates remain until this
matrix is executed against representative real Oracle instances.

## Automated qualification command

Build everything:

```bash
make backend-build
```

Provide the password through the environment instead of a command-line argument:

```bash
export ORACLE_HOST=10.0.0.10
export ORACLE_PORT=1521
export ORACLE_SERVICE=ORCLPDB1
export ORACLE_USER=QMIGRATION_TEST
export ORACLE_PASSWORD='...'
export ORACLE_SCHEMA=QMIGRATION_TEST
export ORACLE_TABLE=QUAL_SOURCE

# Read-only/source qualification.
deployments/scripts/qualify-oracle.sh

# Add LogMiner precheck/current-SCN qualification.
ORACLE_QUALIFY_CDC=1 deployments/scripts/qualify-oracle.sh

# Explicitly destructive target qualification. A QMQUAL_* table is created and
# dropped in ORACLE_SCHEMA.
ORACLE_QUALIFY_TARGET_WRITE=1 deployments/scripts/qualify-oracle.sh
```

Optional report file:

```bash
ORACLE_QUALIFY_OUTPUT=/tmp/oracle-qualification.json \
  deployments/scripts/qualify-oracle.sh
```

The JSON report contains PASS / FAIL / SKIP checks, server version, advertised
capabilities and non-secret endpoint metadata. Passwords, TLS private keys and
certificate private material are never emitted.

## Server matrix

Qualify at minimum:

- Oracle 11gR2
- Oracle 12c
- Oracle 19c
- Oracle 21c
- Oracle 23ai
- TCP and TCPS
- password verifier families observed in supported estates
- AL32UTF8 plus at least one legacy database character set
- single-instance and RAC/SCAN listener redirect paths where used by customers

## Full-load matrix

For each supported server family verify:

1. schema/table/column/PK/index/FK/partition discovery;
2. numeric range, composite keyset, HASH, PARTITION and CUSTOM read paths;
3. NULL/empty-string behavior and exact NUMBER precision;
4. DATE, TIMESTAMP and TIMESTAMP WITH TIME ZONE values;
5. RAW, BLOB and CLOB including values crossing TNS packet boundaries;
6. keyed MERGE, keyless INSERT, array bind and prepared re-execute;
7. keyed and keyless large-LOB paths;
8. reconnect/ORA-01001 cursor-aging recovery;
9. table creation, composite PK, secondary index and foreign key post-load operations;
10. cancellation/deadline and rollback under mid-batch errors.

The automated `--target-write` path directly exercises items 3, 5, 6, 7, 9
(index) and transaction rollback/commit behavior. Broader source-data and
schema-layout cases remain matrix fixtures rather than synthetic one-table tests.

## CDC matrix

Verify:

1. ARCHIVELOG + supplemental logging prechecks;
2. online-redo and archived-redo LogMiner windows;
3. insert/update/delete multi-row transactions;
4. `RS_ID`/`SSN` ordering and `CSF` long SQL continuation;
5. empty windows/checkpoint-only progress;
6. Flashback Query reconstruction within UNDO retention;
7. row movement/partition movement behavior;
8. DDL policy and internal-DDL filtering;
9. durable spool persistence before source ACK;
10. target apply transaction atomicity and restart from checkpoint;
11. cutover, reverse sync and rollback workflows through the Unified Engine.

`--cdc` validates prerequisites and current SCN. Long-running LogMiner, restart,
spool, cutover and rollback cases must be executed through a real migration task.

## TCPS/RAC matrix

For TCPS record:

- TLS mode;
- CA chain;
- server-name verification target;
- optional client certificate usage;
- listener redirect target and final protocol.

For RAC/SCAN repeat qualification across listener redirects and node/service
failover. A successful single connection is not sufficient evidence for
multi-node failover qualification.

## Qualification report acceptance

A single run is accepted only when:

- process exit code is zero;
- JSON field `qualified` is `true`;
- there are zero `FAIL` checks;
- every intentionally required scope (source, CDC, target write) was requested,
  so required checks are not merely `SKIP`;
- server version / NLS / TLS / topology metadata are attached to the environment
  qualification record.

## Exit criteria for removing experimental gates

- No correctness regression across the supported matrix.
- No silent character-set corruption.
- No source checkpoint advancement before durable persistence/apply acknowledgement.
- No partial target transaction acknowledged as committed.
- Large LOB and split/coalesced TTC packet soak tests remain stable under reconnect and backpressure.
- Operational privilege requirements are documented for self-managed and managed Oracle services.
- Qualification JSON reports are retained for every supported Oracle release and
  deployment class that QMigration claims.
