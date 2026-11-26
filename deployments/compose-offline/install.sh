#!/bin/sh
set -eu
cd "$(dirname "$0")"
umask 077
export DOCKER_HOST=unix:///var/run/docker.sock
unset DOCKER_CONTEXT COMPOSE_PROJECT_NAME DTS_METADATA_PASSWORD DTS_MASTER_KEY DTS_AUTH_SECRET DTS_WORKER_TOKEN DTS_BOOTSTRAP_ADMIN_PASSWORD
case "${1:-}" in
  "") ;;
  --install-kylin-deps) sh install-kylin-deps.sh ;;
  *) echo "Usage: sh install.sh [--install-kylin-deps]" >&2; exit 2 ;;
esac
sh install-runtime.sh
command -v flock >/dev/null || { echo "flock is required to serialize installation" >&2; exit 1; }
exec 9>.install.lock
flock -n 9 || { echo "Installation already running" >&2; exit 1; }
if [ ! -s .env ]; then
  for volume in dts-offline_pgdata dts-offline_spool; do
    if docker volume inspect "$volume" >/dev/null 2>&1; then
      echo "Found retained volume $volume. Restore its matching .env before starting." >&2; exit 1
    fi
  done
  secrets=$(mktemp .env.XXXXXX)
  trap 'rm -f -- "$secrets"' EXIT
  for key in DTS_METADATA_PASSWORD DTS_MASTER_KEY DTS_AUTH_SECRET DTS_WORKER_TOKEN DTS_BOOTSTRAP_ADMIN_PASSWORD; do
    random=$(od -An -N48 -tx1 /dev/urandom | tr -d ' \n')
    [ "${#random}" = 96 ] || exit 1
    printf '%s=%s\n' "$key" "$random" >> "$secrets"
  done
  mv -f -- "$secrets" .env
fi
DTS_VERSION=$(cat VERSION); export DTS_VERSION
docker compose -p dts-offline --env-file .env -f compose.yaml config --quiet
docker load -i images/application.tar.gz
docker compose -p dts-offline --env-file .env -f compose.yaml up -d --wait --wait-timeout 240 --no-build --pull never
sh verify.sh
