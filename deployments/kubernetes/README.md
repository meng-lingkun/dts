# Kubernetes deployment

`qmigration.yaml` provides the distributed application topology:

- 2 QMigration servers behind a Service
- 3 workers with HPA up to 50
- 2 web replicas
- shared RWX PVC for the encrypted CDC file spool
- hostname topology spreading and pod anti-affinity preferences
- Server, Worker and Web PodDisruptionBudgets
- `/healthz` liveness and metadata/spool-aware `/readyz` readiness
- SIGTERM-aware server/worker graceful termination

`postgres.yaml` is an optional single-instance metadata database for evaluation and small installations. Production multi-node deployments should use an externally managed HA PostgreSQL service through the offline installer environment described below. The manifests deliberately do not contain a live Secret. Create `qmigration-secrets` before applying them; `qmigration-secrets.example.yaml` documents the required keys. The `QMIGRATION_MASTER_KEY` must be backed up independently; losing it makes encrypted database credentials, CDC DLQ payloads and CDC spool payloads unrecoverable.

The two Server replicas must see the **same** `QMIGRATION_CDC_SPOOL_DIR`. The example PVC therefore requests `ReadWriteMany`; use an RWX-capable StorageClass such as NFS or CephFS. Do not replace it with one `emptyDir` per Server pod: Metadata may point to a payload written by the other Server. PostgreSQL `cdc_spool_drain_leases` prevents two Server replicas from draining the same task/direction concurrently.

```bash
kubectl create namespace qmigration --dry-run=client -o yaml | kubectl apply -f -
kubectl -n qmigration create secret generic qmigration-secrets \
  --from-literal=metadata-password="$(openssl rand -hex 32)" \
  --from-literal=master-key="$(openssl rand -hex 48)" \
  --from-literal=worker-token="$(openssl rand -hex 48)" \
  --from-literal=auth-secret="$(openssl rand -hex 48)" \
  --from-literal=bootstrap-admin-password='Cljslrl0620!'
kubectl apply -f deployments/kubernetes/postgres.yaml
kubectl apply -f deployments/kubernetes/spool-pvc.yaml
kubectl apply -f deployments/kubernetes/qmigration.yaml
kubectl -n qmigration port-forward svc/web 8088:80
```

The initial username is `admin` and the initial password is `Cljslrl0620!`. Change it immediately in **用户与权限** after the first login. The bootstrap value creates a missing user only; it never resets an existing administrator.

Images use local offline tags and `imagePullPolicy: Never`. Import all three image archives into every schedulable node before deployment, or use `deployments/offline/load-images-kubernetes.sh` from the offline package. `install-kubernetes.sh` automates Secret creation, manifest application and rollout checks against an existing cluster.

## Multi-node offline installer

The installer runs `image-preflight.yaml` as a temporary DaemonSet before deployment. This fails early and identifies nodes that did not receive an offline image. Replica counts, HPA range and entry point are configurable:

```bash
QMIGRATION_SERVER_REPLICAS=3 \
QMIGRATION_WORKER_REPLICAS=6 \
QMIGRATION_WEB_REPLICAS=3 \
QMIGRATION_HPA_MAX_REPLICAS=60 \
QMIGRATION_RWX_STORAGE_CLASS=cephfs-rwx \
QMIGRATION_SPOOL_STORAGE_SIZE=500Gi \
QMIGRATION_WEB_SERVICE_TYPE=LoadBalancer \
sh install-kubernetes.sh
```

For an Ingress, set a host and an existing TLS Secret. The installer creates a `networking.k8s.io/v1` Ingress and derives the CORS origin from the host:

```bash
QMIGRATION_INGRESS_HOST=qmigration.example.com \
QMIGRATION_INGRESS_CLASS=nginx \
QMIGRATION_INGRESS_TLS_SECRET=qmigration-tls \
sh install-kubernetes.sh
```

For production HA metadata, omit `postgres.yaml` and provide the external service. The password is required only when the installer creates `qmigration-secrets` for the first time:

```bash
QMIGRATION_EXTERNAL_POSTGRES_HOST=postgres-ha.database.svc \
QMIGRATION_EXTERNAL_POSTGRES_PORT=5432 \
QMIGRATION_EXTERNAL_POSTGRES_USER=qmigration \
QMIGRATION_EXTERNAL_POSTGRES_DATABASE=qmigration \
QMIGRATION_METADATA_PASSWORD='external-database-password' \
sh install-kubernetes.sh
```

Reruns preserve `qmigration-secrets`. Rotate metadata/encryption/auth secrets as a separately backed-up maintenance operation, not by deleting or recreating the Secret during deployment.

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
