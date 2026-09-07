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
cleanup() { if [ -n "$image_container" ]; then docker rm "$image_container" >/dev/null 2>&1 || true; fi; rm -rf -- "$WORK_DIR"; }
trap cleanup EXIT INT TERM
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
docker rm "$image_container" >/dev/null
image_container=

echo "[4/8] Exporting all images"
docker save --output "$STAGE/images/dts-server-$VERSION.tar" "$SERVER_IMAGE"
docker save --output "$STAGE/images/dts-web-$VERSION.tar" "$WEB_IMAGE"
docker save --output "$STAGE/images/postgres-17.tar" "$POSTGRES_IMAGE"

echo "[5/8] Downloading and verifying pinned kubectl"
curl -fL --retry 6 --retry-all-errors -o "$STAGE/runtime/kubectl" "https://dl.k8s.io/release/v$KUBECTL_VERSION/bin/linux/amd64/kubectl"
printf '%s  %s\n' "$KUBECTL_SHA256" "$STAGE/runtime/kubectl" | sha256sum -c -
cp "$SCRIPT_DIR/runtime/versions.env" "$STAGE/runtime/"

echo "[6/8] Staging installer, CLI, migrations and documentation"
cp "$SCRIPT_DIR/install.sh" "$SCRIPT_DIR/install-kubernetes.sh" "$SCRIPT_DIR/load-images-kubernetes.sh" "$SCRIPT_DIR/verify.sh" "$SCRIPT_DIR/uninstall.sh" "$SCRIPT_DIR/README.md" "$STAGE/"
cp "$REPO_ROOT/deployments/kubernetes/"*.yaml "$STAGE/kubernetes/"
cp "$REPO_ROOT/VERSION" "$STAGE/VERSION"
cp "$REPO_ROOT/backend/migrations/"*.sql "$STAGE/migrations/"
cp "$REPO_ROOT/docs/USER_GUIDE.md" "$REPO_ROOT/docs/MAINTENANCE_GUIDE.md" "$REPO_ROOT/docs/PROJECT_ARCHITECTURE.md" "$REPO_ROOT/docs/ARCHITECTURE_ASSESSMENT.md" "$STAGE/docs/"
chmod 0755 "$STAGE/install.sh" "$STAGE/install-kubernetes.sh" "$STAGE/load-images-kubernetes.sh" "$STAGE/verify.sh" "$STAGE/uninstall.sh" "$STAGE/bin/dtsctl" "$STAGE/runtime/kubectl"

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
  "node_runtime": "containerd (provided by existing Kubernetes cluster)",
  "kubernetes_client": {
    "kubectl_version": "$KUBECTL_VERSION",
    "kubectl_archive": "runtime/kubectl",
    "kubectl_sha256": "$KUBECTL_SHA256"
  },
  "images": [
    {"name": "$SERVER_IMAGE", "image_id": "$server_id", "archive": "images/dts-server-$VERSION.tar"},
    {"name": "$WEB_IMAGE", "image_id": "$web_id", "archive": "images/dts-web-$VERSION.tar"},
    {"name": "$POSTGRES_IMAGE", "source": "$POSTGRES_SOURCE_IMAGE", "image_id": "$postgres_id", "archive": "images/postgres-17.tar"}
  ]
}
EOF

echo "[7/8] Creating package integrity manifest"
(cd "$STAGE" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS)

echo "[8/8] Creating compressed offline package"
mkdir -p "$OUTPUT_DIR"
ARCHIVE=$OUTPUT_DIR/$PACKAGE_NAME.tar.gz
tar -C "$WORK_DIR" -czf "$ARCHIVE" "$PACKAGE_NAME"
(cd "$OUTPUT_DIR" && sha256sum "$PACKAGE_NAME.tar.gz" > "$PACKAGE_NAME.tar.gz.sha256")
echo "Created: $ARCHIVE"
echo "Checksum: $ARCHIVE.sha256"
