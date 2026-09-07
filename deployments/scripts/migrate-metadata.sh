#!/usr/bin/env sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
: "${DTS_METADATA_HOST:=127.0.0.1}"
: "${DTS_METADATA_PORT:=5432}"
: "${DTS_METADATA_USER:=dts}"
: "${DTS_METADATA_DATABASE:=dts}"
export PGPASSWORD=${DTS_METADATA_PASSWORD:?DTS_METADATA_PASSWORD is required}
command -v psql >/dev/null 2>&1 || { echo "psql is required" >&2; exit 127; }

for file in "$ROOT"/backend/migrations/*.sql; do
  echo "[DTS] applying $(basename "$file")"
  psql \
    -v ON_ERROR_STOP=1 \
    -h "$DTS_METADATA_HOST" \
    -p "$DTS_METADATA_PORT" \
    -U "$DTS_METADATA_USER" \
    -d "$DTS_METADATA_DATABASE" \
    -1 -f "$file"
done

psql \
  -v ON_ERROR_STOP=1 -At \
  -h "$DTS_METADATA_HOST" \
  -p "$DTS_METADATA_PORT" \
  -U "$DTS_METADATA_USER" \
  -d "$DTS_METADATA_DATABASE" \
  -c "SELECT 'metadata_schema_version=' || schema_version FROM metadata_schema_state WHERE id=1"
