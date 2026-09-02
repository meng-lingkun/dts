# QMigration CDC Chaos Qualification (RC29)

RC29 extends the deterministic RC27/RC28 failpoint framework with **real process-death qualification**. Fault injection remains disabled unless `QMIGRATION_ENABLE_FAULT_INJECTION=1` is explicitly set.

## Built-in qualification

Run:

```bash
./deployments/scripts/qualify-chaos.sh
```

or:

```bash
bin/qmigration-chaos-qualify
```

RC29 executes ten self-contained scenarios. Eight preserve the RC28 deterministic durability/recovery cases; two launch a real child QMigration process and terminate it with SIGKILL at exact crash windows:

1. `process-sigkill-target-commit-before-checkpoint`: target transaction is durably committed, a pre-COMMIT ambiguity fence exists, then the child process is killed before the source checkpoint. Restart must find `COMMIT_UNCERTAIN`, block automatic replay, and avoid a second target write.
2. `process-sigkill-spool-persist-before-source-ack`: the source transaction is durably staged, then the child process is killed before source ACK. Restart/source redelivery must reuse one spool record rather than create a second copy.

The other eight checks cover spool persistence/apply/mark windows, checkpoint-before-source-ACK, target COMMIT response uncertainty, controlled NOT_COMMITTED replay, durable-spool COMMIT uncertainty, and REPLAY_REQUIRED recovery.

A successful run returns JSON with `qualified=true` and all ten checks `PASS`.

## Failpoint syntax

Deterministic error injection remains:

```bash
export QMIGRATION_ENABLE_FAULT_INJECTION=1
export QMIGRATION_FAULT_PLAN='cdc.spool.after_persist_before_ack=1'
```

RC29 additionally supports a process-kill action:

```bash
export QMIGRATION_ENABLE_FAULT_INJECTION=1
export QMIGRATION_FAULT_PLAN='cdc.apply.after_target_before_checkpoint=1@SIGKILL'
```

`SIGKILL` terminates the current process at the exact occurrence. Use it only in isolated qualification processes. Unknown actions and malformed enabled plans fail closed.

## File-backed spool I/O windows

RC29 adds repository-level I/O failpoints:

- `cdc.spool.file.before_write`
- `cdc.spool.file.after_persist_before_metadata`

The second simulates a crash after atomic payload persistence but before Metadata commit. On restart, `spoolfile.Reconcile` must identify the unreferenced pending file and move it under `applied/recovered-orphans`; source progress was never acknowledged.

Never enable fault injection on a production Server/Worker.
