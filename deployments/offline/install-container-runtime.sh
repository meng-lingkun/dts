#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
RUNTIME_DIR=$ROOT/runtime
. "$RUNTIME_DIR/versions.env"

if [ "$(uname -s)" != "Linux" ] || [ "$(uname -m)" != "x86_64" ]; then
  echo "ERROR: bundled container runtime targets Linux x86_64" >&2
  exit 1
fi
if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: container runtime installation must run as root" >&2
  exit 1
fi
for required_command in sed tar sha256sum systemctl; do
  command -v "$required_command" >/dev/null 2>&1 || {
    echo "ERROR: host is missing required OS command: $required_command" >&2
    exit 1
  }
done
if [ ! -d /run/systemd/system ]; then
  echo "ERROR: automatic Docker setup requires systemd as PID 1" >&2
  echo "The bundled static binaries can be configured manually for another init system." >&2
  exit 1
fi
command -v iptables >/dev/null 2>&1 || {
  echo "ERROR: the host OS must provide iptables for Docker bridge networking" >&2
  exit 1
}

ENGINE_ARCHIVE=$RUNTIME_DIR/docker-$DOCKER_ENGINE_VERSION.tgz
COMPOSE_BINARY=$RUNTIME_DIR/docker-compose-linux-x86_64
printf '%s  %s\n' "$DOCKER_ENGINE_SHA256" "$ENGINE_ARCHIVE" | sha256sum -c -
printf '%s  %s\n' "$DOCKER_COMPOSE_SHA256" "$COMPOSE_BINARY" | sha256sum -c -

docker_path=$(command -v docker 2>/dev/null || true)
dockerd_path=$(command -v dockerd 2>/dev/null || true)
if [ -n "$docker_path" ] && [ -n "$dockerd_path" ]; then
  echo "Keeping existing Docker CLI and daemon: $docker_path, $dockerd_path"
elif [ -n "$docker_path" ] || [ -n "$dockerd_path" ]; then
  echo "ERROR: partial Docker installation detected; refusing to mix package-managed and bundled binaries" >&2
  exit 1
else
  work_dir=$(mktemp -d "${TMPDIR:-/tmp}/qmigration-docker.XXXXXX")
  cleanup() { rm -rf -- "$work_dir"; }
  trap cleanup EXIT INT TERM
  tar -xzf "$ENGINE_ARCHIVE" -C "$work_dir"
  for binary in ctr docker docker-init containerd containerd-shim-runc-v2 dockerd docker-proxy runc; do
    source_path=$work_dir/docker/$binary
    [ -f "$source_path" ] || { echo "ERROR: Docker archive is missing $binary" >&2; exit 1; }
    target_path=/usr/local/bin/$binary
    [ ! -e "$target_path" ] || { echo "ERROR: refusing to overwrite $target_path" >&2; exit 1; }
  done
  for binary in ctr docker docker-init containerd containerd-shim-runc-v2 dockerd docker-proxy runc; do
    source_path=$work_dir/docker/$binary
    target_path=/usr/local/bin/$binary
    cp "$source_path" "$target_path"
    chmod 0755 "$target_path"
  done
  echo "Installed Docker Engine $DOCKER_ENGINE_VERSION static binaries in /usr/local/bin."
fi

compose_target=/usr/local/lib/docker/cli-plugins/docker-compose
if docker compose version >/dev/null 2>&1; then
  echo "Keeping existing Docker Compose plugin."
else
  if [ -e "$compose_target" ]; then
    echo "ERROR: refusing to overwrite unrecognized Compose plugin $compose_target" >&2
    exit 1
  fi
  mkdir -p "$(dirname -- "$compose_target")"
  cp "$COMPOSE_BINARY" "$compose_target"
  chmod 0755 "$compose_target"
  echo "Installed Docker Compose v$DOCKER_COMPOSE_VERSION."
fi

if command -v groupadd >/dev/null 2>&1 && command -v getent >/dev/null 2>&1 && ! getent group docker >/dev/null 2>&1; then
  groupadd --system docker
fi

if ! docker info >/dev/null 2>&1; then
  if ! systemctl cat docker.service >/dev/null 2>&1; then
    dockerd_path=$(command -v dockerd)
    sed "s|@DOCKERD_PATH@|$dockerd_path|g" "$RUNTIME_DIR/docker.service" > /etc/systemd/system/docker.service
    chmod 0644 /etc/systemd/system/docker.service
    systemctl daemon-reload
  fi
  systemctl enable --now docker.service
fi

attempt=0
until docker info >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    systemctl --no-pager --full status docker.service >&2 || true
    echo "ERROR: Docker daemon did not become ready" >&2
    exit 1
  fi
  sleep 1
done

engine_version=$(docker version --format '{{.Server.Version}}')
engine_major=${engine_version%%.*}
case "$engine_major" in
  ''|*[!0-9]*) echo "ERROR: cannot parse Docker Engine version: $engine_version" >&2; exit 1 ;;
esac
if [ "$engine_major" -lt 24 ]; then
  echo "ERROR: existing Docker Engine $engine_version is older than required version 24" >&2
  echo "Remove or upgrade the package-managed Docker installation, then rerun this installer." >&2
  exit 1
fi
compose_version=$(docker compose version --short)
compose_version=${compose_version#v}
compose_major=${compose_version%%.*}
case "$compose_major" in
  ''|*[!0-9]*) echo "ERROR: cannot parse Docker Compose version: $compose_version" >&2; exit 1 ;;
esac
if [ "$compose_major" -lt 2 ]; then
  echo "ERROR: Docker Compose $compose_version is older than v2" >&2
  exit 1
fi

docker version
docker compose version
echo "Container runtime is ready. Docker group membership was not granted automatically."
