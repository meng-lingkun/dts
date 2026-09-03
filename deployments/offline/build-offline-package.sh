#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
VERSION=$(cat "$REPO_ROOT/VERSION")
PLATFORM=${PLATFORM:-linux/amd64}
POSTGRES_SOURCE_IMAGE=${POSTGRES_SOURCE_IMAGE:-postgres:17}
OUTPUT_DIR=${OUTPUT_DIR:-$REPO_ROOT/dist}
PACKAGE_NAME=qmigration-offline-${VERSION}-linux-amd64
. "$SCRIPT_DIR/runtime/versions.env"

case "$PLATFORM" in
  linux/amd64) ;;
  *) echo "ERROR: only linux/amd64 is currently supported, got $PLATFORM" >&2; exit 1 ;;
esac
for command_name in curl docker sha256sum tar; do
  command -v "$command_name" >/dev/null 2>&1 || { echo "ERROR: missing build dependency: $command_name" >&2; exit 1; }
done
docker version >/dev/null

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/qmigration-offline.XXXXXX")
cleanup() { rm -rf -- "$WORK_DIR"; }
trap cleanup EXIT INT TERM
STAGE=$WORK_DIR/$PACKAGE_NAME
mkdir -p "$STAGE/images" "$STAGE/bin" "$STAGE/migrations" "$STAGE/docs" "$STAGE/runtime" "$STAGE/kubernetes"

SERVER_IMAGE=qmigration/server:$VERSION
WEB_IMAGE=qmigration/web:$VERSION
POSTGRES_IMAGE=qmigration/postgres:17

echo "[1/8] Building QMigration linux/amd64 images"
docker build --platform "$PLATFORM" --file "$REPO_ROOT/deployments/Dockerfile.backend" --tag "$SERVER_IMAGE" "$REPO_ROOT"
docker build --platform "$PLATFORM" --file "$REPO_ROOT/deployments/Dockerfile.web" --tag "$WEB_IMAGE" "$REPO_ROOT"

echo "[2/8] Pulling and pinning PostgreSQL runtime image"
docker pull --platform "$PLATFORM" "$POSTGRES_SOURCE_IMAGE"
docker tag "$POSTGRES_SOURCE_IMAGE" "$POSTGRES_IMAGE"

echo "[3/8] Checking required runtime executables"
docker run --rm --platform "$PLATFORM" --entrypoint /bin/sh "$SERVER_IMAGE" -ec '
  for name in qmigration-server qmigration-worker qmigrationctl qmigration-cdc-bridge qmigration-binlog-inspect qmigration-mysql-cdc qmigration-tidb-cdc qmigration-postgres-cdc qmigration-opengauss-cdc qmigration-gaussdb-cdc qmigration-sqlserver-cdc qmigration-oracle-cdc qmigration-db2-cdc qmigration-dameng-cdc qmigration-gbase-cdc qmigration-gbase8s-cdc zstd; do
    command -v "$name" >/dev/null || { echo "missing runtime: $name" >&2; exit 1; }
  done
'

echo "[4/8] Exporting all images"
docker save --output "$STAGE/images/qmigration-server-$VERSION.tar" "$SERVER_IMAGE"
docker save --output "$STAGE/images/qmigration-web-$VERSION.tar" "$WEB_IMAGE"
docker save --output "$STAGE/images/postgres-17.tar" "$POSTGRES_IMAGE"

echo "[5/8] Downloading and verifying pinned Docker Engine and Compose runtimes"
docker_archive=docker-$DOCKER_ENGINE_VERSION.tgz
curl -fL --retry 6 --retry-all-errors -o "$STAGE/runtime/$docker_archive" "https://download.docker.com/linux/static/stable/x86_64/$docker_archive"
curl -fL --retry 6 --retry-all-errors -o "$STAGE/runtime/docker-compose-linux-x86_64" "https://github.com/docker/compose/releases/download/v$DOCKER_COMPOSE_VERSION/docker-compose-linux-x86_64"
curl -fL --retry 6 --retry-all-errors -o "$STAGE/runtime/kubectl" "https://dl.k8s.io/release/v$KUBECTL_VERSION/bin/linux/amd64/kubectl"
printf '%s  %s\n' "$DOCKER_ENGINE_SHA256" "$STAGE/runtime/$docker_archive" | sha256sum -c -
printf '%s  %s\n' "$DOCKER_COMPOSE_SHA256" "$STAGE/runtime/docker-compose-linux-x86_64" | sha256sum -c -
printf '%s  %s\n' "$KUBECTL_SHA256" "$STAGE/runtime/kubectl" | sha256sum -c -
cp "$SCRIPT_DIR/runtime/versions.env" "$SCRIPT_DIR/runtime/docker.service" "$STAGE/runtime/"

echo "[6/8] Staging installer, CLI, migrations and documentation"
cp "$SCRIPT_DIR/docker-compose.offline.yml" "$SCRIPT_DIR/install.sh" "$SCRIPT_DIR/install-kubernetes.sh" "$SCRIPT_DIR/load-images-kubernetes.sh" "$SCRIPT_DIR/install-container-runtime.sh" "$SCRIPT_DIR/verify.sh" "$SCRIPT_DIR/uninstall.sh" "$SCRIPT_DIR/README.md" "$STAGE/"
cp "$REPO_ROOT/deployments/kubernetes/"*.yaml "$STAGE/kubernetes/"
cp "$REPO_ROOT/VERSION" "$STAGE/VERSION"
cp "$REPO_ROOT/backend/migrations/"*.sql "$STAGE/migrations/"
cp "$REPO_ROOT/docs/USER_GUIDE.md" "$REPO_ROOT/docs/MAINTENANCE_GUIDE.md" "$REPO_ROOT/docs/PROJECT_ARCHITECTURE.md" "$REPO_ROOT/docs/ARCHITECTURE_ASSESSMENT.md" "$STAGE/docs/"
docker run --rm --platform "$PLATFORM" --entrypoint /bin/cat "$SERVER_IMAGE" /usr/local/bin/qmigrationctl > "$STAGE/bin/qmigrationctl"
chmod 0755 "$STAGE/install.sh" "$STAGE/install-kubernetes.sh" "$STAGE/load-images-kubernetes.sh" "$STAGE/install-container-runtime.sh" "$STAGE/verify.sh" "$STAGE/uninstall.sh" "$STAGE/bin/qmigrationctl" "$STAGE/runtime/docker-compose-linux-x86_64" "$STAGE/runtime/kubectl"

server_id=$(docker image inspect --format '{{.Id}}' "$SERVER_IMAGE")
web_id=$(docker image inspect --format '{{.Id}}' "$WEB_IMAGE")
postgres_id=$(docker image inspect --format '{{.Id}}' "$POSTGRES_IMAGE")
created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
cat > "$STAGE/manifest.json" <<EOF
{
  "name": "QMigration offline installation package",
  "version": "$VERSION",
  "platform": "$PLATFORM",
  "created_at": "$created_at",
  "container_runtime": {
    "docker_engine_version": "$DOCKER_ENGINE_VERSION",
    "docker_engine_archive": "runtime/$docker_archive",
    "docker_engine_sha256": "$DOCKER_ENGINE_SHA256",
    "docker_compose_version": "$DOCKER_COMPOSE_VERSION",
    "docker_compose_archive": "runtime/docker-compose-linux-x86_64",
    "docker_compose_sha256": "$DOCKER_COMPOSE_SHA256",
    "kubectl_version": "$KUBECTL_VERSION",
    "kubectl_archive": "runtime/kubectl",
    "kubectl_sha256": "$KUBECTL_SHA256"
  },
  "images": [
    {"name": "$SERVER_IMAGE", "image_id": "$server_id", "archive": "images/qmigration-server-$VERSION.tar"},
    {"name": "$WEB_IMAGE", "image_id": "$web_id", "archive": "images/qmigration-web-$VERSION.tar"},
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
sha256sum "$ARCHIVE" > "$ARCHIVE.sha256"
echo "Created: $ARCHIVE"
echo "Checksum: $ARCHIVE.sha256"
