#!/usr/bin/env sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
schema="$SCRIPT_DIR/../../backend/internal/repository/postgres/schema.sql"
[ -r "$schema" ] || schema="$SCRIPT_DIR/../migrations/schema.sql"
[ -r "$schema" ] || { echo "DTS metadata schema.sql is missing" >&2; exit 1; }
: "${DTS_METADATA_HOST:=127.0.0.1}"
: "${DTS_METADATA_PORT:=5432}"
: "${DTS_METADATA_USER:=dts}"
: "${DTS_METADATA_DATABASE:=dts}"
export PGPASSWORD=${DTS_METADATA_PASSWORD:?DTS_METADATA_PASSWORD is required}
command -v psql >/dev/null 2>&1 || { echo "psql is required" >&2; exit 127; }

psql -X -1 -v ON_ERROR_STOP=1 -h "$DTS_METADATA_HOST" -p "$DTS_METADATA_PORT" \
  -U "$DTS_METADATA_USER" -d "$DTS_METADATA_DATABASE" -f "$schema"

psql \
  -X -v ON_ERROR_STOP=1 -At \
  -h "$DTS_METADATA_HOST" \
  -p "$DTS_METADATA_PORT" \
  -U "$DTS_METADATA_USER" \
  -d "$DTS_METADATA_DATABASE" \
  -c "SELECT 'metadata_schema_version=' || schema_version FROM metadata_schema_state WHERE id=1"
