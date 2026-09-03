#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"

if [ "${1:-}" = "--purge" ]; then
  echo "WARNING: removing containers and persistent PostgreSQL/Spool volumes"
  docker compose --env-file .env -f docker-compose.offline.yml down --volumes
else
  docker compose --env-file .env -f docker-compose.offline.yml down
  echo "Persistent volumes were retained. Use '$0 --purge' only to delete all QMigration data."
fi
echo "Docker Engine and Compose were retained because they may be shared by other applications."
