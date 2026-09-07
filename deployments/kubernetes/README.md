# Kubernetes deployment

Kubernetes with containerd is the only supported container deployment path. Docker is used only to build and export images; no Docker Engine or Compose installation is required on application nodes. The package entry points are `sh install.sh`, `sh verify.sh` and `sh uninstall.sh` (preserving data and Secrets).

`dts.yaml` provides the distributed application topology:

- 2 DTS servers behind a Service
- 3 workers with HPA up to 50
- 2 web replicas
- shared RWX PVC for the encrypted CDC file spool
- hostname topology spreading and pod anti-affinity preferences
- Server, Worker and Web PodDisruptionBudgets
- `/healthz` liveness and metadata/spool-aware `/readyz` readiness
- SIGTERM-aware server/worker graceful termination

`postgres.yaml` is an optional single-instance metadata database for evaluation and small installations. Production multi-node deployments should use an externally managed HA PostgreSQL service through the offline installer environment described below. The manifests deliberately do not contain a live Secret. Create `dts-secrets` before applying them; `dts-secrets.example.yaml` documents the required keys. The `DTS_MASTER_KEY` must be backed up independently; losing it makes encrypted database credentials, CDC DLQ payloads and CDC spool payloads unrecoverable.

The two Server replicas must see the **same** `DTS_CDC_SPOOL_DIR`. The example PVC therefore requests `ReadWriteMany`; use an RWX-capable StorageClass such as NFS or CephFS. Do not replace it with one `emptyDir` per Server pod: Metadata may point to a payload written by the other Server. PostgreSQL `cdc_spool_drain_leases` prevents two Server replicas from draining the same task/direction concurrently.

```bash
kubectl create namespace dts --dry-run=client -o yaml | kubectl apply -f -
kubectl -n dts create secret generic dts-secrets \
  --from-literal=metadata-password="$(openssl rand -hex 32)" \
  --from-literal=master-key="$(openssl rand -hex 48)" \
  --from-literal=worker-token="$(openssl rand -hex 48)" \
  --from-literal=auth-secret="$(openssl rand -hex 48)" \
  --from-literal=bootstrap-admin-password='Cljslrl0620!'
kubectl apply -f deployments/kubernetes/postgres.yaml
kubectl apply -f deployments/kubernetes/spool-pvc.yaml
kubectl apply -f deployments/kubernetes/dts.yaml
kubectl -n dts port-forward svc/web 8088:80
```

The initial username is `admin` and the initial password is `Cljslrl0620!`. Change it immediately in **用户与权限** after the first login. The bootstrap value creates a missing user only; it never resets an existing administrator.

Images use local offline tags and `imagePullPolicy: Never`. Import all three image archives into every schedulable node before deployment, or use `deployments/offline/load-images-kubernetes.sh` from the offline package. `install-kubernetes.sh` automates Secret creation, manifest application and rollout checks against an existing cluster.

## Multi-node offline installer

The installer runs `image-preflight.yaml` as a temporary DaemonSet before deployment. This fails early and identifies nodes that did not receive an offline image. Replica counts, HPA range and entry point are configurable:

```bash
DTS_SERVER_REPLICAS=3 \
DTS_WORKER_REPLICAS=6 \
DTS_WEB_REPLICAS=3 \
DTS_HPA_MAX_REPLICAS=60 \
DTS_RWX_STORAGE_CLASS=cephfs-rwx \
DTS_SPOOL_STORAGE_SIZE=500Gi \
DTS_WEB_SERVICE_TYPE=LoadBalancer \
sh install-kubernetes.sh
```

For an Ingress, set a host and an existing TLS Secret. The installer creates a `networking.k8s.io/v1` Ingress and derives the CORS origin from the host:

```bash
DTS_INGRESS_HOST=dts.example.com \
DTS_INGRESS_CLASS=nginx \
DTS_INGRESS_TLS_SECRET=dts-tls \
sh install-kubernetes.sh
```

For production HA metadata, omit `postgres.yaml` and provide the external service. The password is required only when the installer creates `dts-secrets` for the first time:

```bash
DTS_EXTERNAL_POSTGRES_HOST=postgres-ha.database.svc \
DTS_EXTERNAL_POSTGRES_PORT=5432 \
DTS_EXTERNAL_POSTGRES_USER=dts \
DTS_EXTERNAL_POSTGRES_DATABASE=dts \
DTS_METADATA_PASSWORD='external-database-password' \
sh install-kubernetes.sh
```

Reruns preserve `dts-secrets`. Rotate metadata/encryption/auth secrets as a separately backed-up maintenance operation, not by deleting or recreating the Secret during deployment.

## Unified-dev8 upgrade sequence

```bash
# 1. Back up metadata before changing the control plane.
DTS_METADATA_PASSWORD='...' deployments/scripts/backup.sh

# 2. Apply ordered idempotent migrations, including
#    024_v015_spool_s3_object_store.sql.
DTS_METADATA_PASSWORD='...' deployments/scripts/migrate-metadata.sh

# 3. Ensure the shared spool PVC is Bound and writable by every Server pod.
kubectl -n dts get pvc dts-cdc-spool

# 4. Deploy the unified-dev8 images.
kubectl apply -f deployments/kubernetes/dts.yaml

# 5. Wait for metadata/schema/spool-aware readiness.
kubectl -n dts rollout status deploy/server
kubectl -n dts rollout status deploy/worker
kubectl -n dts get pods
```

The Server returns `503` from `/readyz` when the PostgreSQL repository is unavailable, `metadata_schema_state.schema_version` does not match the binary, the spool filesystem is not writable, or its CRITICAL disk watermark is reached.

Workers stop claiming new Chunk/CDC work after SIGTERM. Unfinished Native chunks recover through durable cursor + lease; CDC source ACK is safe because an acknowledged transaction is either applied to the target or durably present in the encrypted spool.


## S3-compatible spool option

For multi-Server deployments you may replace the RWX filesystem spool with `DTS_CDC_SPOOL_STORAGE=s3`. In that mode the Server does not need the shared spool PVC. Put S3 access/secret/session credentials in a Kubernetes Secret, not the ConfigMap. See `docs/SPOOL_STORAGE.md` for the full environment contract. DTS performs application-level AES-256-GCM encryption before uploading any CDC payload.
# Customer database DNS

Database hostnames are supported. Pods use cluster DNS and do not inherit node hosts files. Configure enterprise DNS conditional forwarding in CoreDNS for dynamic endpoints, or use the packaged installer's `DTS_DNS_PATCH_FILE` with `dns-hosts.patch.example.yaml` for static hostAliases on both server and worker Deployments. This example is a merge patch, not a resource to apply directly. See [customer DNS configuration and verification](../offline/README.md#客户数据库域名解析) for installation, updates, and cutover limitations.
