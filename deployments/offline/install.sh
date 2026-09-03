#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
chmod 0755 install.sh install-kubernetes.sh load-images-kubernetes.sh install-container-runtime.sh verify.sh uninstall.sh bin/qmigrationctl runtime/kubectl 2>/dev/null || true

if [ "$(uname -s)" != "Linux" ]; then
  echo "ERROR: this package targets Linux" >&2
  exit 1
fi
case "$(uname -m)" in
  x86_64|amd64) ;;
  *) echo "ERROR: this package targets linux/amd64" >&2; exit 1 ;;
esac

runtime_ready() {
  command -v docker >/dev/null 2>&1 || return 1
  docker info >/dev/null 2>&1 || return 1
  docker compose version >/dev/null 2>&1 || return 1
  engine_version=$(docker version --format '{{.Server.Version}}' 2>/dev/null) || return 1
  engine_major=${engine_version%%.*}
  case "$engine_major" in ''|*[!0-9]*) return 1 ;; esac
  [ "$engine_major" -ge 24 ] || return 1
  compose_version=$(docker compose version --short 2>/dev/null) || return 1
  compose_version=${compose_version#v}
  compose_major=${compose_version%%.*}
  case "$compose_major" in ''|*[!0-9]*) return 1 ;; esac
  [ "$compose_major" -ge 2 ]
}

echo "[1/5] Verifying package checksums"
sha256sum -c SHA256SUMS

if ! runtime_ready; then
  echo "[2/5] Installing bundled Docker Engine and Compose"
  if [ "$(id -u)" -ne 0 ]; then
    if command -v sudo >/dev/null 2>&1; then
      echo "Root privileges are required; restarting installer through sudo."
      exec sudo sh "$0" "$@"
    fi
    echo "ERROR: run 'sudo sh install.sh' to install the bundled container runtime" >&2
    exit 1
  fi
  sh install-container-runtime.sh
else
  echo "[2/5] Existing Docker Engine and Compose are ready; keeping them unchanged"
fi

runtime_ready || { echo "ERROR: Docker Engine 24+ and Docker Compose v2+ are not ready" >&2; exit 1; }

echo "[3/5] Loading offline images"
for archive in images/*.tar; do
  docker load --input "$archive"
done

if [ ! -f .env ]; then
  umask 077
  random_hex() { od -An -N48 -tx1 /dev/urandom | tr -d ' \n'; }
  admin_password=${QMIGRATION_ADMIN_PASSWORD:-Cljslrl0620!}
  cat > .env <<EOF
QMIGRATION_VERSION=$(cat VERSION)
QMIGRATION_METADATA_PASSWORD=$(random_hex)
QMIGRATION_MASTER_KEY=$(random_hex)
QMIGRATION_WORKER_TOKEN=$(random_hex)
QMIGRATION_AUTH_SECRET=$(random_hex)
QMIGRATION_BOOTSTRAP_ADMIN_USER=admin
QMIGRATION_BOOTSTRAP_ADMIN_PASSWORD=$admin_password
QMIGRATION_CORS_ORIGIN=http://127.0.0.1:8088
QMIGRATION_WORKER_CONCURRENCY=4
QMIGRATION_WORKER_SHUTDOWN_GRACE_SECONDS=30
EOF
  chmod 600 .env
  printf '%s\n' "Initial admin user: admin" "Initial admin password: $admin_password" > INITIAL_ADMIN_CREDENTIALS.txt
  chmod 600 INITIAL_ADMIN_CREDENTIALS.txt
  echo "Generated .env and INITIAL_ADMIN_CREDENTIALS.txt (mode 600)."
else
  echo "Keeping existing .env."
fi

echo "[4/5] Validating offline Compose configuration"
docker compose --env-file .env -f docker-compose.offline.yml config >/dev/null

echo "[5/5] Starting QMigration"
docker compose --env-file .env -f docker-compose.offline.yml up -d --no-build --pull never
docker compose --env-file .env -f docker-compose.offline.yml ps

echo "Web: http://127.0.0.1:8088"
echo "API: http://127.0.0.1:8080"
echo "Initial credentials: sudo cat $ROOT/INITIAL_ADMIN_CREDENTIALS.txt"
