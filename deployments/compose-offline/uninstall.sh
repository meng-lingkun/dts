#!/bin/sh
set -eu
cd "$(dirname "$0")"
DTS_VERSION=$(cat VERSION); export DTS_VERSION
export DOCKER_HOST=unix:///var/run/docker.sock
unset DOCKER_CONTEXT
sha256sum -c --quiet SHA256SUMS
exec 9>.install.lock
flock -n 9 || { echo "Installation or uninstall already running" >&2; exit 1; }
# Compose resolves service environment entries even for "down". Only the
# project and volume names are needed, including when .env has been lost.
for key in DTS_MASTER_KEY DTS_AUTH_SECRET DTS_WORKER_TOKEN DTS_BOOTSTRAP_ADMIN_PASSWORD DTS_METADATA_PASSWORD; do
  export "$key=unused-for-shutdown"
done
compose_down() { docker compose -p dts-offline --env-file /dev/null -f compose.yaml down "$@"; }
case "${1:-}" in
  "") compose_down ;;
  --purge-data)
    for volume in dts-offline_pgdata dts-offline_spool; do
      if docker volume inspect "$volume" >/dev/null 2>&1; then
        owner=$(docker volume inspect --format '{{index .Labels "com.docker.compose.project"}}' "$volume")
        [ "$owner" = dts-offline ] || { echo "Volume $volume belongs to another project" >&2; exit 1; }
      fi
    done
    compose_down --volumes
    volumes=$(docker volume ls -q)
    for volume in dts-offline_pgdata dts-offline_spool; do
      if printf '%s\n' "$volumes" | grep -Fxq "$volume"; then
        echo "Volume $volume remains; retained .env for recovery" >&2; exit 1
      fi
    done
    rm -f -- .env
    echo "Removed DTS database and spool volumes and .env." ;;
  *) echo "Usage: sh uninstall.sh [--purge-data]" >&2; exit 2 ;;
esac
