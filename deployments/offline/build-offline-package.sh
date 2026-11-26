#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
VERSION=$(cat "$REPO_ROOT/VERSION")
PLATFORM=${PLATFORM:-linux/amd64}
POSTGRES_SOURCE_IMAGE=${POSTGRES_SOURCE_IMAGE:-postgres:17}
OUTPUT_DIR=${OUTPUT_DIR:-$REPO_ROOT/dist}
PACKAGE_NAME=dts-kubernetes-offline-${VERSION}-linux-amd64
. "$SCRIPT_DIR/runtime/versions.env"

case "$PLATFORM" in
  linux/amd64) ;;
  *) echo "ERROR: only linux/amd64 is currently supported, got $PLATFORM" >&2; exit 1 ;;
esac
for command_name in curl docker sha256sum tar; do
  command -v "$command_name" >/dev/null 2>&1 || { echo "ERROR: missing build dependency: $command_name" >&2; exit 1; }
done
docker version >/dev/null

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/dts-offline.XXXXXX")
image_container=
archive_tmp=
cleanup() {
  if [ -n "$image_container" ]; then docker rm "$image_container" >/dev/null 2>&1 || true; fi
  if [ -n "$archive_tmp" ]; then rm -f -- "$archive_tmp" "$archive_tmp.sha256"; fi
  rm -rf -- "$WORK_DIR"
}
trap cleanup EXIT
trap 'exit 1' INT TERM
STAGE=$WORK_DIR/$PACKAGE_NAME
mkdir -p "$STAGE/images" "$STAGE/bin" "$STAGE/migrations" "$STAGE/docs" "$STAGE/runtime" "$STAGE/kubernetes"

SERVER_IMAGE=dts/server:$VERSION
WEB_IMAGE=dts/web:$VERSION
POSTGRES_IMAGE=dts/postgres:17

echo "[1/8] Building DTS linux/amd64 images"
docker build --platform "$PLATFORM" --file "$REPO_ROOT/deployments/Dockerfile.backend" --tag "$SERVER_IMAGE" "$REPO_ROOT"
docker build --platform "$PLATFORM" --file "$REPO_ROOT/deployments/Dockerfile.web" --tag "$WEB_IMAGE" "$REPO_ROOT"

echo "[2/8] Pulling and pinning PostgreSQL runtime image"
docker pull --platform "$PLATFORM" "$POSTGRES_SOURCE_IMAGE"
docker tag "$POSTGRES_SOURCE_IMAGE" "$POSTGRES_IMAGE"

echo "[3/8] Extracting CLI from the built image without starting a container"
image_container=$(docker create --platform "$PLATFORM" "$SERVER_IMAGE")
docker cp "$image_container:/usr/local/bin/dtsctl" "$STAGE/bin/dtsctl"
docker cp "$image_container:/usr/local/bin/dts-certgen" "$STAGE/bin/dts-certgen"
docker rm "$image_container" >/dev/null
image_container=

echo "[4/8] Exporting all images"
docker save --output "$STAGE/images/dts-server-$VERSION.tar" "$SERVER_IMAGE"
docker save --output "$STAGE/images/dts-web-$VERSION.tar" "$WEB_IMAGE"
docker save --output "$STAGE/images/postgres-17.tar" "$POSTGRES_IMAGE"

echo "[5/8] Downloading and verifying pinned kubectl"
curl -fL --retry 6 --retry-all-errors -o "$STAGE/runtime/kubectl" "https://dl.k8s.io/release/v$KUBECTL_VERSION/bin/linux/amd64/kubectl"
printf '%s  %s\n' "$KUBECTL_SHA256" "$STAGE/runtime/kubectl" | sha256sum -c -
for tool in kubeadm kubelet; do
  curl -fL --retry 6 --retry-all-errors -o "$STAGE/runtime/$tool" "https://dl.k8s.io/release/v$KUBECTL_VERSION/bin/linux/amd64/$tool"
  case "$tool" in kubeadm) tool_hash=$KUBEADM_SHA256 ;; kubelet) tool_hash=$KUBELET_SHA256 ;; esac
  printf '%s  %s\n' "$tool_hash" "$STAGE/runtime/$tool" | sha256sum -c -
done
cp "$SCRIPT_DIR/runtime/versions.env" "$STAGE/runtime/"
cp "$SCRIPT_DIR/install-kubernetes-tools.sh" "$STAGE/"

echo "[6/8] Staging installer, CLI, migrations and documentation"
cp "$SCRIPT_DIR/install.sh" "$SCRIPT_DIR/install-kubernetes.sh" "$SCRIPT_DIR/load-images-kubernetes.sh" "$SCRIPT_DIR/verify.sh" "$SCRIPT_DIR/uninstall.sh" "$SCRIPT_DIR/uninstall-all.sh" "$SCRIPT_DIR/README.md" "$STAGE/"
cp "$REPO_ROOT/deployments/kubernetes/"*.yaml "$STAGE/kubernetes/"
cp "$REPO_ROOT/VERSION" "$STAGE/VERSION"
cp "$REPO_ROOT/backend/migrations/"*.sql "$STAGE/migrations/"
cp "$REPO_ROOT/backend/internal/repository/postgres/schema.sql" "$STAGE/migrations/schema.sql"
mkdir -p "$STAGE/scripts"
cp "$REPO_ROOT/deployments/scripts/migrate-metadata.sh" "$STAGE/scripts/"
cp "$REPO_ROOT/deployments/scripts/recovery-set.sh" "$STAGE/scripts/"
cp "$REPO_ROOT/docs/USER_GUIDE.md" "$REPO_ROOT/docs/MAINTENANCE_GUIDE.md" "$REPO_ROOT/docs/PROJECT_ARCHITECTURE.md" "$REPO_ROOT/docs/ARCHITECTURE_ASSESSMENT.md" "$STAGE/docs/"
cp "$REPO_ROOT/docs/DTS_RENAME_UPGRADE.md" "$REPO_ROOT/docs/ENTERPRISE_HARDENING_RC50.md" "$REPO_ROOT/docs/OFFLINE_RELEASE_SECURITY.md" "$REPO_ROOT/docs/openapi.json" "$STAGE/docs/"
cp "$REPO_ROOT/docs/SUPPORT_MATRIX_V0.15_RC50.md" "$STAGE/docs/"
python3 "$REPO_ROOT/tools/s3-recovery/build.py" --out "$STAGE/tools"
cp "$REPO_ROOT/deployments/kubernetes/README.md" "$STAGE/kubernetes/README.md"
mkdir -p "$STAGE/tools/release-security"
cp "$REPO_ROOT/tools/release-security/release.py" "$STAGE/tools/release-security/"
server_archive=images/dts-server-$VERSION.tar
web_archive=images/dts-web-$VERSION.tar
postgres_archive=images/postgres-17.tar
cluster_payload=false
node_runtime="containerd (provided by existing Kubernetes cluster)"
if [ -n "${CLUSTER_PAYLOAD_DIR:-}" ]; then
  mkdir -p "$STAGE/cluster"
  for directory in runtime network rpms images; do cp -R "$CLUSTER_PAYLOAD_DIR/$directory" "$STAGE/cluster/"; done
  cp "$CLUSTER_PAYLOAD_DIR/kylin-rpms.json" "$CLUSTER_PAYLOAD_DIR/upstream-tools.json" "$CLUSTER_PAYLOAD_DIR/image-provenance.json" "$STAGE/cluster/"
  cp "$SCRIPT_DIR/cluster/"* "$SCRIPT_DIR/cluster-images.txt" "$STAGE/cluster/"
  cp "$SCRIPT_DIR/install-cluster.sh" "$SCRIPT_DIR/configure-kylin-419-cgroup.sh" "$SCRIPT_DIR/restore-kylin-419-cgroup.sh" "$SCRIPT_DIR/install-registry.sh" "$SCRIPT_DIR/configure-registry.sh" "$SCRIPT_DIR/CLUSTER_INSTALL.md" "$STAGE/"
  cp "$SCRIPT_DIR/deploy.sh" "$SCRIPT_DIR/unpack-images.sh" "$STAGE/"
  cp -R "$SCRIPT_DIR/automation" "$STAGE/"
  while IFS= read -r ref; do
    filename=$(printf '%s' "$ref" | tr '/:' '__')
    test -s "$STAGE/cluster/images/$filename.tar" || { echo "Missing cluster image: $ref" >&2; exit 1; }
  done < "$SCRIPT_DIR/cluster-images.txt"
  find "$STAGE/images" "$STAGE/cluster/images" -name '*.tar' -type f > "$WORK_DIR/merge-inputs.txt"
  (cd "$REPO_ROOT/tools/offline-image-builder" && GOTOOLCHAIN="go$(cat "$REPO_ROOT/.go-version")" go run . --merge-list "$WORK_DIR/merge-inputs.txt" --merged-archive "$STAGE/images/all-images.tar")
  (cd "$STAGE/images" && gzip all-images.tar)
  while IFS= read -r file; do rm -f -- "$file"; done < "$WORK_DIR/merge-inputs.txt"
  cluster_payload=true
  node_runtime="containerd (bundled for offline kubeadm bootstrap)"
  server_archive=images/all-images.tar.gz
  web_archive=$server_archive
  postgres_archive=$server_archive
fi
chmod 0755 "$STAGE/install.sh" "$STAGE/install-kubernetes.sh" "$STAGE/load-images-kubernetes.sh" "$STAGE/verify.sh" "$STAGE/uninstall.sh" "$STAGE/uninstall-all.sh" "$STAGE/bin/dtsctl" "$STAGE/bin/dts-certgen" "$STAGE/runtime/kubectl"

server_id=$(docker image inspect --format '{{.Id}}' "$SERVER_IMAGE")
web_id=$(docker image inspect --format '{{.Id}}' "$WEB_IMAGE")
postgres_id=$(docker image inspect --format '{{.Id}}' "$POSTGRES_IMAGE")
created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
cat > "$STAGE/manifest.json" <<EOF
{
  "name": "DTS offline installation package",
  "version": "$VERSION",
  "platform": "$PLATFORM",
  "created_at": "$created_at",
  "deployment": "kubernetes",
  "cluster_payload": $cluster_payload,
  "node_runtime": "$node_runtime",
  "cgroup_profiles": ["Kylin 4.19/cgroup v1 compatibility", "kernel 5.10+/cgroup v2"],
  "kubernetes_client": {
    "kubectl_version": "$KUBECTL_VERSION",
    "kubectl_archive": "runtime/kubectl",
    "kubectl_sha256": "$KUBECTL_SHA256",
    "kubeadm_archive": "runtime/kubeadm",
    "kubeadm_sha256": "$KUBEADM_SHA256",
    "kubelet_archive": "runtime/kubelet",
    "kubelet_sha256": "$KUBELET_SHA256"
  },
  "images": [
    {"name": "$SERVER_IMAGE", "image_id": "$server_id", "archive": "$server_archive"},
    {"name": "$WEB_IMAGE", "image_id": "$web_id", "archive": "$web_archive"},
    {"name": "$POSTGRES_IMAGE", "source": "$POSTGRES_SOURCE_IMAGE", "image_id": "$postgres_id", "archive": "$postgres_archive"}
  ]
}
EOF

echo "[7/8] Creating package integrity manifest"
(cd "$STAGE" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS)

echo "[8/8] Creating compressed offline package"
mkdir -p "$OUTPUT_DIR"
ARCHIVE=$OUTPUT_DIR/$PACKAGE_NAME.tar.gz
archive_tmp=$(mktemp "$OUTPUT_DIR/.$PACKAGE_NAME.XXXXXX")
tar -C "$WORK_DIR" -czf "$archive_tmp" "$PACKAGE_NAME"
archive_digest=$(sha256sum < "$archive_tmp")
printf '%s  %s\n' "${archive_digest%% *}" "$PACKAGE_NAME.tar.gz" > "$archive_tmp.sha256"
chmod 0644 "$archive_tmp" "$archive_tmp.sha256"
mv -f -- "$archive_tmp" "$ARCHIVE"
mv -f -- "$archive_tmp.sha256" "$ARCHIVE.sha256"
echo "Created: $ARCHIVE"
echo "Checksum: $ARCHIVE.sha256"
