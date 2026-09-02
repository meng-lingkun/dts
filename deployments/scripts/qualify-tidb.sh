#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN=${QMIGRATION_TIDB_QUALIFY_BIN:-"$ROOT/bin/qmigration-tidb-qualify"}

if [[ ! -x "$BIN" ]]; then
  echo "building qmigration-tidb-qualify" >&2
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/tidb-qualify)
fi

: "${TIDB_HOST:?set TIDB_HOST}"
: "${TIDB_DATABASE:?set TIDB_DATABASE}"
: "${TIDB_USER:?set TIDB_USER}"
: "${TIDB_PASSWORD:?set TIDB_PASSWORD}"
: "${TIDB_TICDC_URL:?set TIDB_TICDC_URL, e.g. ticdc://cdc:8300?brokers=kafka:9092}"

args=(
  --host "$TIDB_HOST"
  --port "${TIDB_PORT:-4000}"
  --database "$TIDB_DATABASE"
  --user "$TIDB_USER"
  --password-env TIDB_PASSWORD
  --cdc-url "$TIDB_TICDC_URL"
  --schema "${TIDB_SCHEMA:-$TIDB_DATABASE}"
  --sample-rows "${TIDB_SAMPLE_ROWS:-16}"
  --timeout "${TIDB_QUALIFY_TIMEOUT:-90s}"
  --tls-mode "${TIDB_TLS_MODE:-DISABLE}"
)

[[ -n "${TIDB_TABLE:-}" ]] && args+=(--table "$TIDB_TABLE")
[[ "${TIDB_QUALIFY_CDC:-0}" == "1" ]] && args+=(--cdc)
[[ -n "${TIDB_QUALIFY_OUTPUT:-}" ]] && args+=(--output "$TIDB_QUALIFY_OUTPUT")

exec "$BIN" "${args[@]}"
