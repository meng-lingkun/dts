#!/bin/sh
set -eu
cd "$(dirname "$0")"
sha256sum -c --quiet SHA256SUMS
[ "$(uname -sm)" = "Linux x86_64" ] || { echo "Linux x86_64 required" >&2; exit 1; }
: "${DOCKER_HOST:=unix:///var/run/docker.sock}"
export DOCKER_HOST
if ! docker info >/dev/null 2>&1; then
  [ "$(id -u)" = 0 ] || { echo "Run with sudo to install/start Docker" >&2; exit 1; }
  [ -d /run/systemd/system ] && command -v iptables >/dev/null || { echo "systemd and iptables required" >&2; exit 1; }
  if ! systemctl cat docker.service >/dev/null 2>&1; then
    if command -v dockerd >/dev/null || command -v docker >/dev/null; then
      echo "Existing Docker installation detected. Start its daemon first." >&2; exit 1
    fi
    target=/opt/dts/docker-29.7.2
    mkdir -p "$target" /usr/local/bin
    tar -xzf runtime/docker-29.7.2.tgz --strip-components=1 -C "$target"
    ln -s "$target/docker" /usr/local/bin/docker
    install -m 644 runtime/docker.service /etc/systemd/system/docker.service
    systemctl daemon-reload
  fi
  systemctl enable --now docker.service
fi
attempt=0
until docker info >/dev/null 2>&1; do
  attempt=$((attempt+1)); [ "$attempt" -lt 30 ] || { echo "Docker startup failed" >&2; exit 1; }
  sleep 1
done
engine=$(docker version --format '{{.Server.Version}}')
[ "${engine%%.*}" -ge 24 ] || { echo "Docker Engine 24+ required" >&2; exit 1; }
if ! docker compose version >/dev/null 2>&1; then
  [ "$(id -u)" = 0 ] || { echo "Run with sudo to install the Compose plugin" >&2; exit 1; }
  plugin=/usr/local/lib/docker/cli-plugins/docker-compose
  [ ! -e "$plugin" ] || { echo "Existing Compose plugin cannot run" >&2; exit 1; }
  install -D -m 755 runtime/docker-compose-linux-x86_64 "$plugin"
fi
version=$(docker compose version --short)
case "${version#v}" in 2.*) ;; *) echo "Compose v2 required" >&2; exit 1;; esac
