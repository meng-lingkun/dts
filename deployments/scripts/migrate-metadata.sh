#!/usr/bin/env sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
: "${QMIGRATION_METADATA_HOST:=127.0.0.1}"
: "${QMIGRATION_METADATA_PORT:=5432}"
: "${QMIGRATION_METADATA_USER:=qmigration}"
: "${QMIGRATION_METADATA_DATABASE:=qmigration}"
export PGPASSWORD=${QMIGRATION_METADATA_PASSWORD:?QMIGRATION_METADATA_PASSWORD is required}
command -v psql >/dev/null 2>&1 || { echo "psql is required" >&2; exit 127; }

for file in "$ROOT"/backend/migrations/*.sql; do
  echo "[QMigration] applying $(basename "$file")"
  psql \
    -v ON_ERROR_STOP=1 \
    -h "$QMIGRATION_METADATA_HOST" \
    -p "$QMIGRATION_METADATA_PORT" \
    -U "$QMIGRATION_METADATA_USER" \
    -d "$QMIGRATION_METADATA_DATABASE" \
    -1 -f "$file"
done

psql \
  -v ON_ERROR_STOP=1 -At \
  -h "$QMIGRATION_METADATA_HOST" \
  -p "$QMIGRATION_METADATA_PORT" \
  -U "$QMIGRATION_METADATA_USER" \
  -d "$QMIGRATION_METADATA_DATABASE" \
  -c "SELECT 'metadata_schema_version=' || schema_version FROM metadata_schema_state WHERE id=1"
