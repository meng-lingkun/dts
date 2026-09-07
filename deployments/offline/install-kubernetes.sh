#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
KUBECTL=${KUBECTL:-$ROOT/runtime/kubectl}
NAMESPACE=dts
ADMIN_PASSWORD=${DTS_ADMIN_PASSWORD:-Cljslrl0620!}
SERVER_REPLICAS=${DTS_SERVER_REPLICAS:-2}
WORKER_REPLICAS=${DTS_WORKER_REPLICAS:-3}
WEB_REPLICAS=${DTS_WEB_REPLICAS:-2}
HPA_MAX_REPLICAS=${DTS_HPA_MAX_REPLICAS:-50}
WEB_SERVICE_TYPE=${DTS_WEB_SERVICE_TYPE:-ClusterIP}
INGRESS_HOST=${DTS_INGRESS_HOST:-}
INGRESS_CLASS=${DTS_INGRESS_CLASS:-nginx}
INGRESS_TLS_SECRET=${DTS_INGRESS_TLS_SECRET:-}
EXTERNAL_POSTGRES_HOST=${DTS_EXTERNAL_POSTGRES_HOST:-}
EXTERNAL_POSTGRES_PORT=${DTS_EXTERNAL_POSTGRES_PORT:-5432}
EXTERNAL_POSTGRES_USER=${DTS_EXTERNAL_POSTGRES_USER:-dts}
EXTERNAL_POSTGRES_DATABASE=${DTS_EXTERNAL_POSTGRES_DATABASE:-dts}
RWX_STORAGE_CLASS=${DTS_RWX_STORAGE_CLASS:-}
SPOOL_STORAGE_SIZE=${DTS_SPOOL_STORAGE_SIZE:-200Gi}
CORS_ORIGIN=${DTS_CORS_ORIGIN:-}
DNS_PATCH_FILE=${DTS_DNS_PATCH_FILE:-}
# Relative paths are resolved from the package directory after cd above.
if [ -n "$DNS_PATCH_FILE" ] && [ ! -r "$DNS_PATCH_FILE" ]; then
  echo "ERROR: DTS_DNS_PATCH_FILE must name a readable deployment patch (prefer an absolute path)" >&2
  exit 1
fi

if [ "$(uname -s)" != "Linux" ]; then
  echo "ERROR: this package targets Linux" >&2
  exit 1
fi
case "$(uname -m)" in x86_64|amd64) ;; *) echo "ERROR: this package targets linux/amd64" >&2; exit 1 ;; esac

positive_integer() {
  name=$1
  value=$2
  case "$value" in ''|*[!0-9]*) echo "ERROR: $name must be a positive integer" >&2; exit 1 ;; esac
  if [ "$value" -lt 1 ]; then echo "ERROR: $name must be at least 1" >&2; exit 1; fi
}
positive_integer DTS_SERVER_REPLICAS "$SERVER_REPLICAS"
positive_integer DTS_WORKER_REPLICAS "$WORKER_REPLICAS"
positive_integer DTS_WEB_REPLICAS "$WEB_REPLICAS"
positive_integer DTS_HPA_MAX_REPLICAS "$HPA_MAX_REPLICAS"
positive_integer DTS_EXTERNAL_POSTGRES_PORT "$EXTERNAL_POSTGRES_PORT"
if [ "$EXTERNAL_POSTGRES_PORT" -gt 65535 ]; then echo "ERROR: DTS_EXTERNAL_POSTGRES_PORT must be <= 65535" >&2; exit 1; fi
if [ "$HPA_MAX_REPLICAS" -lt "$WORKER_REPLICAS" ]; then
  echo "ERROR: DTS_HPA_MAX_REPLICAS must be >= DTS_WORKER_REPLICAS" >&2
  exit 1
fi
case "$WEB_SERVICE_TYPE" in ClusterIP|NodePort|LoadBalancer) ;; *) echo "ERROR: DTS_WEB_SERVICE_TYPE must be ClusterIP, NodePort or LoadBalancer" >&2; exit 1 ;; esac
case "$INGRESS_HOST" in *[!A-Za-z0-9.-]*) echo "ERROR: invalid DTS_INGRESS_HOST" >&2; exit 1 ;; esac
case "$INGRESS_CLASS" in *[!A-Za-z0-9.-]*) echo "ERROR: invalid DTS_INGRESS_CLASS" >&2; exit 1 ;; esac
case "$INGRESS_TLS_SECRET" in *[!A-Za-z0-9.-]*) echo "ERROR: invalid DTS_INGRESS_TLS_SECRET" >&2; exit 1 ;; esac
case "$RWX_STORAGE_CLASS" in *[!A-Za-z0-9.-]*) echo "ERROR: invalid DTS_RWX_STORAGE_CLASS" >&2; exit 1 ;; esac
spool_size_number=${SPOOL_STORAGE_SIZE%??}
spool_size_unit=${SPOOL_STORAGE_SIZE#"$spool_size_number"}
positive_integer DTS_SPOOL_STORAGE_SIZE "$spool_size_number"
case "$spool_size_unit" in Mi|Gi|Ti) ;; *) echo "ERROR: DTS_SPOOL_STORAGE_SIZE must use Mi, Gi or Ti" >&2; exit 1 ;; esac

if [ -z "$CORS_ORIGIN" ]; then
  if [ -n "$INGRESS_HOST" ]; then
    if [ -n "$INGRESS_TLS_SECRET" ]; then CORS_ORIGIN="https://$INGRESS_HOST"; else CORS_ORIGIN="http://$INGRESS_HOST"; fi
  else
    CORS_ORIGIN=http://127.0.0.1:8088
  fi
fi

chmod 0755 "$KUBECTL" load-images-kubernetes.sh 2>/dev/null || true
sha256sum -c SHA256SUMS >/dev/null
"$KUBECTL" version --client >/dev/null
"$KUBECTL" cluster-info >/dev/null

# Read the runtime reported by kubelet; an unrelated Docker installation
# must not be mistaken for the Kubernetes runtime.
node_runtimes=$("$KUBECTL" get nodes -o 'jsonpath={range .items[*]}{.metadata.name}{"="}{.status.nodeInfo.containerRuntimeVersion}{"\n"}{end}')
if [ -z "$node_runtimes" ]; then
  echo "ERROR: no Kubernetes node runtime information is available" >&2
  exit 1
fi
unsupported=$(printf '%s\n' "$node_runtimes" | awk 'index($0, "=containerd://") == 0 {print}')
if [ -n "$unsupported" ]; then
  printf 'ERROR: this offline package requires Kubernetes nodes using containerd:\n%s\n' "$unsupported" >&2
  exit 1
fi

ready_nodes=$("$KUBECTL" get nodes --no-headers 2>/dev/null | awk '$2 ~ /^Ready/ {count++} END {print count+0}')
if [ "$ready_nodes" -eq 0 ]; then
  echo "ERROR: the current kubectl context has no Ready nodes" >&2
  exit 1
fi
if [ "$ready_nodes" -lt 2 ]; then
  echo "WARNING: only $ready_nodes Ready node was found; replicas can run, but node-level high availability requires at least two nodes." >&2
fi

preflight_active=false
generated_ingress="$ROOT/.dts-ingress.generated.yaml"
generated_spool="$ROOT/.dts-spool.generated.yaml"
cleanup() {
  if [ "$preflight_active" = true ]; then
    "$KUBECTL" -n "$NAMESPACE" delete daemonset image-preflight --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  rm -f -- "$generated_ingress" "$generated_spool"
}
trap cleanup EXIT INT TERM

echo "[1/7] Creating namespace and runtime secrets"
"$KUBECTL" create namespace "$NAMESPACE" --dry-run=client -o yaml | "$KUBECTL" apply -f - >/dev/null
secret_created=false
if "$KUBECTL" -n "$NAMESPACE" get secret dts-secrets >/dev/null 2>&1; then
  echo "Keeping existing dts-secrets; reruns never rotate database or encryption keys."
else
  if [ -n "$EXTERNAL_POSTGRES_HOST" ] && [ -z "${DTS_METADATA_PASSWORD:-}" ]; then
    echo "ERROR: DTS_METADATA_PASSWORD is required when creating a Secret for an external PostgreSQL service" >&2
    exit 1
  fi
  random_hex() { od -An -N48 -tx1 /dev/urandom | tr -d ' \n'; }
  metadata_password=${DTS_METADATA_PASSWORD:-$(random_hex)}
  master_key=${DTS_MASTER_KEY:-$(random_hex)}
  worker_token=${DTS_WORKER_TOKEN:-$(random_hex)}
  auth_secret=${DTS_AUTH_SECRET:-$(random_hex)}
  "$KUBECTL" -n "$NAMESPACE" create secret generic dts-secrets \
    --from-literal=metadata-password="$metadata_password" \
    --from-literal=master-key="$master_key" \
    --from-literal=worker-token="$worker_token" \
    --from-literal=auth-secret="$auth_secret" \
    --from-literal=bootstrap-admin-password="$ADMIN_PASSWORD" \
    --dry-run=client -o yaml | "$KUBECTL" apply -f - >/dev/null
  secret_created=true
fi

echo "[2/7] Verifying offline images on every eligible node"
"$KUBECTL" apply -f kubernetes/image-preflight.yaml >/dev/null
preflight_active=true
if ! "$KUBECTL" -n "$NAMESPACE" rollout status daemonset/image-preflight --timeout=180s; then
  "$KUBECTL" -n "$NAMESPACE" get pods -l app=dts-image-preflight -o wide >&2 || true
  echo "ERROR: at least one node is missing an offline image. Run 'sudo sh load-images-kubernetes.sh' on every eligible node." >&2
  exit 1
fi
"$KUBECTL" -n "$NAMESPACE" delete daemonset image-preflight --wait=true >/dev/null
preflight_active=false

echo "[3/7] Configuring metadata database"
if [ -n "$EXTERNAL_POSTGRES_HOST" ]; then
  echo "Using external PostgreSQL at $EXTERNAL_POSTGRES_HOST:$EXTERNAL_POSTGRES_PORT/$EXTERNAL_POSTGRES_DATABASE"
  if "$KUBECTL" -n "$NAMESPACE" get statefulset postgres >/dev/null 2>&1; then
    echo "WARNING: an older bundled PostgreSQL StatefulSet still exists but will not be used." >&2
  fi
else
  "$KUBECTL" apply -f kubernetes/postgres.yaml
  "$KUBECTL" -n "$NAMESPACE" rollout status statefulset/postgres --timeout=300s
fi

echo "[4/7] Applying distributed DTS resources"
if [ -n "$RWX_STORAGE_CLASS" ]; then
  printf '%s\n' \
    "apiVersion: v1" "kind: PersistentVolumeClaim" "metadata:" \
    "  name: dts-cdc-spool" "  namespace: $NAMESPACE" "spec:" \
    "  storageClassName: $RWX_STORAGE_CLASS" "  accessModes: [ReadWriteMany]" \
    "  resources:" "    requests:" "      storage: $SPOOL_STORAGE_SIZE" > "$generated_spool"
  "$KUBECTL" apply -f "$generated_spool"
else
  "$KUBECTL" apply -f kubernetes/spool-pvc.yaml
fi
"$KUBECTL" apply -f kubernetes/dts.yaml
if [ -n "$DNS_PATCH_FILE" ]; then
  # Validate both targets before changing either. Merge replaces hostAliases as
  # a whole, so removed mappings do not survive an update.
  for deployment in server worker; do
    "$KUBECTL" -n "$NAMESPACE" patch deployment "$deployment" --type=merge --patch-file "$DNS_PATCH_FILE" --dry-run=server >/dev/null
  done
  for deployment in server worker; do
    "$KUBECTL" -n "$NAMESPACE" patch deployment "$deployment" --type=merge --patch-file "$DNS_PATCH_FILE" >/dev/null
  done
fi
if [ -n "$EXTERNAL_POSTGRES_HOST" ]; then
  "$KUBECTL" -n "$NAMESPACE" set env deployment/server \
    "DTS_METADATA_HOST=$EXTERNAL_POSTGRES_HOST" \
    "DTS_METADATA_PORT=$EXTERNAL_POSTGRES_PORT" \
    "DTS_METADATA_USER=$EXTERNAL_POSTGRES_USER" \
    "DTS_METADATA_DATABASE=$EXTERNAL_POSTGRES_DATABASE" >/dev/null
else
  "$KUBECTL" -n "$NAMESPACE" set env deployment/server \
    DTS_METADATA_HOST- DTS_METADATA_PORT- \
    DTS_METADATA_USER- DTS_METADATA_DATABASE- >/dev/null
fi
"$KUBECTL" -n "$NAMESPACE" set env deployment/server "DTS_CORS_ORIGIN=$CORS_ORIGIN" >/dev/null
"$KUBECTL" -n "$NAMESPACE" scale deployment/server --replicas="$SERVER_REPLICAS" >/dev/null
"$KUBECTL" -n "$NAMESPACE" scale deployment/worker --replicas="$WORKER_REPLICAS" >/dev/null
"$KUBECTL" -n "$NAMESPACE" scale deployment/web --replicas="$WEB_REPLICAS" >/dev/null
"$KUBECTL" -n "$NAMESPACE" patch hpa worker --type=merge -p "{\"spec\":{\"minReplicas\":$WORKER_REPLICAS,\"maxReplicas\":$HPA_MAX_REPLICAS}}" >/dev/null
server_min=$((SERVER_REPLICAS - 1)); if [ "$server_min" -lt 1 ]; then server_min=1; fi
worker_min=$((WORKER_REPLICAS - 1)); if [ "$worker_min" -lt 1 ]; then worker_min=1; fi
web_min=$((WEB_REPLICAS - 1)); if [ "$web_min" -lt 1 ]; then web_min=1; fi
"$KUBECTL" -n "$NAMESPACE" patch pdb server --type=merge -p "{\"spec\":{\"minAvailable\":$server_min}}" >/dev/null
"$KUBECTL" -n "$NAMESPACE" patch pdb worker --type=merge -p "{\"spec\":{\"minAvailable\":$worker_min}}" >/dev/null
"$KUBECTL" -n "$NAMESPACE" patch pdb web --type=merge -p "{\"spec\":{\"minAvailable\":$web_min}}" >/dev/null

echo "[5/7] Configuring cluster entry point"
"$KUBECTL" -n "$NAMESPACE" patch service web --type=merge -p "{\"spec\":{\"type\":\"$WEB_SERVICE_TYPE\"}}" >/dev/null
if [ -n "$INGRESS_HOST" ]; then
  if ! "$KUBECTL" get ingressclass "$INGRESS_CLASS" >/dev/null 2>&1; then
    echo "ERROR: IngressClass $INGRESS_CLASS does not exist in the cluster" >&2
    exit 1
  fi
  if [ -n "$INGRESS_TLS_SECRET" ] && ! "$KUBECTL" -n "$NAMESPACE" get secret "$INGRESS_TLS_SECRET" >/dev/null 2>&1; then
    echo "ERROR: TLS Secret $NAMESPACE/$INGRESS_TLS_SECRET does not exist" >&2
    exit 1
  fi
  if [ -n "$INGRESS_TLS_SECRET" ]; then
    tls_block="  tls:\n  - hosts: [$INGRESS_HOST]\n    secretName: $INGRESS_TLS_SECRET"
  else
    tls_block=""
  fi
  {
    printf '%s\n' "apiVersion: networking.k8s.io/v1" "kind: Ingress" "metadata:" "  name: dts" "  namespace: $NAMESPACE" "spec:" "  ingressClassName: $INGRESS_CLASS"
    if [ -n "$tls_block" ]; then printf '%b\n' "$tls_block"; fi
    printf '%s\n' "  rules:" "  - host: $INGRESS_HOST" "    http:" "      paths:" "      - path: /" "        pathType: Prefix" "        backend:" "          service:" "            name: web" "            port:" "              number: 80"
  } > "$generated_ingress"
  "$KUBECTL" apply -f "$generated_ingress"
fi

echo "[6/7] Waiting for shared storage and rollouts"
if ! "$KUBECTL" -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Bound pvc/dts-cdc-spool --timeout=180s; then
  echo "ERROR: dts-cdc-spool needs an RWX-capable default StorageClass. See README.md." >&2
  exit 1
fi
"$KUBECTL" -n "$NAMESPACE" rollout status deployment/server --timeout=300s
"$KUBECTL" -n "$NAMESPACE" rollout status deployment/worker --timeout=300s
"$KUBECTL" -n "$NAMESPACE" rollout status deployment/web --timeout=300s

echo "[7/7] Multi-node deployment summary"
"$KUBECTL" -n "$NAMESPACE" get pods -o wide
"$KUBECTL" -n "$NAMESPACE" get svc,pvc,hpa,pdb
if [ "$secret_created" = true ]; then
  umask 077
  printf '%s\n' "Initial admin user: admin" "Initial admin password: $ADMIN_PASSWORD" > INITIAL_ADMIN_CREDENTIALS.txt
  chmod 600 INITIAL_ADMIN_CREDENTIALS.txt
else
  echo "Existing credentials were preserved."
fi
if [ -n "$INGRESS_HOST" ]; then
  echo "Web: $CORS_ORIGIN"
elif [ "$WEB_SERVICE_TYPE" = ClusterIP ]; then
  echo "Run: $KUBECTL -n $NAMESPACE port-forward svc/web 8088:80"
  echo "Web: http://127.0.0.1:8088"
else
  echo "Inspect the Web endpoint with: $KUBECTL -n $NAMESPACE get service web"
fi
echo "Change the default password immediately after the first login."
