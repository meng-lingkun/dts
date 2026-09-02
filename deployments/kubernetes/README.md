# Kubernetes deployment

`qmigration.yaml` provides a runnable development/small-production topology:

- 2 QMigration servers behind a Service
- 3 workers with HPA up to 50
- 2 web replicas
- PostgreSQL StatefulSet for metadata
- shared RWX PVC for the encrypted CDC file spool
- Server and Worker PodDisruptionBudgets
- `/healthz` liveness and metadata/spool-aware `/readyz` readiness
- SIGTERM-aware server/worker graceful termination

For production, replace the bundled single-node PostgreSQL with an externally managed HA PostgreSQL service and point `QMIGRATION_METADATA_HOST` at it. Replace every placeholder in `qmigration-secrets` before deployment. The `QMIGRATION_MASTER_KEY` must be backed up independently; losing it makes encrypted database credentials, CDC DLQ payloads and CDC spool payloads unrecoverable.

The two Server replicas must see the **same** `QMIGRATION_CDC_SPOOL_DIR`. The example PVC therefore requests `ReadWriteMany`; use an RWX-capable StorageClass such as NFS or CephFS. Do not replace it with one `emptyDir` per Server pod: Metadata may point to a payload written by the other Server. PostgreSQL `cdc_spool_drain_leases` prevents two Server replicas from draining the same task/direction concurrently.

```bash
kubectl apply -f deployments/kubernetes/qmigration.yaml
kubectl -n qmigration port-forward svc/web 8088:80
```

## Unified-dev8 upgrade sequence

```bash
# 1. Back up metadata before changing the control plane.
QMIGRATION_METADATA_PASSWORD='...' deployments/scripts/backup.sh

# 2. Apply ordered idempotent migrations, including
#    024_v015_spool_s3_object_store.sql.
QMIGRATION_METADATA_PASSWORD='...' deployments/scripts/migrate-metadata.sh

# 3. Ensure the shared spool PVC is Bound and writable by every Server pod.
kubectl -n qmigration get pvc qmigration-cdc-spool

# 4. Deploy the unified-dev8 images.
kubectl apply -f deployments/kubernetes/qmigration.yaml

# 5. Wait for metadata/schema/spool-aware readiness.
kubectl -n qmigration rollout status deploy/server
kubectl -n qmigration rollout status deploy/worker
kubectl -n qmigration get pods
```

The Server returns `503` from `/readyz` when the PostgreSQL repository is unavailable, `metadata_schema_state.schema_version` does not match the binary, the spool filesystem is not writable, or its CRITICAL disk watermark is reached.

Workers stop claiming new Chunk/CDC work after SIGTERM. Unfinished Native chunks recover through durable cursor + lease; CDC source ACK is safe because an acknowledged transaction is either applied to the target or durably present in the encrypted spool.


## S3-compatible spool option

For multi-Server deployments you may replace the RWX filesystem spool with `QMIGRATION_CDC_SPOOL_STORAGE=s3`. In that mode the Server does not need the shared spool PVC. Put S3 access/secret/session credentials in a Kubernetes Secret, not the ConfigMap. See `docs/SPOOL_STORAGE.md` for the full environment contract. QMigration performs application-level AES-256-GCM encryption before uploading any CDC payload.
