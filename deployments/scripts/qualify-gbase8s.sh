#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN=${QMIGRATION_GBASE8S_QUALIFY_BIN:-"$ROOT/bin/qmigration-gbase8s-qualify"}
if [[ ! -x "$BIN" ]]; then
  echo "building qmigration-gbase8s-qualify" >&2
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/gbase8s-qualify)
fi
: "${GBASE8S_HOST:?set GBASE8S_HOST}"
: "${GBASE8S_USER:?set GBASE8S_USER}"
: "${GBASE8S_PASSWORD:?set GBASE8S_PASSWORD}"
: "${GBASE8S_DATABASE:?set GBASE8S_DATABASE}"
: "${GBASE8S_ODBC_DSN:?set GBASE8S_ODBC_DSN to a non-secret ODBC DSN/name}"
args=(
  --host "$GBASE8S_HOST"
  --port "${GBASE8S_PORT:-9088}"
  --user "$GBASE8S_USER"
  --database "$GBASE8S_DATABASE"
  --schema "${GBASE8S_SCHEMA:-$GBASE8S_USER}"
  --driver "${GBASE8S_SQL_DRIVER:-odbc}"
  --sample-rows "${GBASE8S_SAMPLE_ROWS:-16}"
  --timeout "${GBASE8S_QUALIFY_TIMEOUT:-90s}"
)
[[ -n "${GBASE8S_TABLE:-}" ]] && args+=(--table "$GBASE8S_TABLE")
[[ "${GBASE8S_QUALIFY_TARGET_WRITE:-0}" == "1" ]] && args+=(--target-write)
[[ "${GBASE8S_QUALIFY_CDC:-0}" == "1" ]] && args+=(--cdc --cdc-url "${GBASE8S_CDC_URL:?set GBASE8S_CDC_URL when GBASE8S_QUALIFY_CDC=1}")
[[ -n "${GBASE8S_QUALIFY_OUTPUT:-}" ]] && args+=(--output "$GBASE8S_QUALIFY_OUTPUT")
exec "$BIN" "${args[@]}"
