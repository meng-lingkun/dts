#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${QMIGRATION_BIN_DIR:-$ROOT/bin}/qmigration-oceanbase-qualify"
if [[ ! -x "$BIN" ]]; then
  echo "ERROR: $BIN not found; run 'make backend-build' first" >&2
  exit 1
fi
: "${OCEANBASE_HOST:?set OCEANBASE_HOST}"
: "${OCEANBASE_DATABASE:?set OCEANBASE_DATABASE}"
: "${OCEANBASE_USER:?set OCEANBASE_USER}"
: "${OCEANBASE_PASSWORD:?set OCEANBASE_PASSWORD}"
: "${OCEANBASE_BINLOG_URL:?set OCEANBASE_BINLOG_URL, e.g. obbinlog://odp-host:2883}"
export OCEANBASE_PASSWORD
args=(
  --host "$OCEANBASE_HOST"
  --port "${OCEANBASE_PORT:-2883}"
  --database "$OCEANBASE_DATABASE"
  --user "$OCEANBASE_USER"
  --cdc-url "$OCEANBASE_BINLOG_URL"
  --tls-mode "${OCEANBASE_TLS_MODE:-DISABLE}"
)
[[ -n "${OCEANBASE_SCHEMA:-}" ]] && args+=(--schema "$OCEANBASE_SCHEMA")
[[ -n "${OCEANBASE_TABLE:-}" ]] && args+=(--table "$OCEANBASE_TABLE")
[[ -n "${OCEANBASE_QUALIFY_OUTPUT:-}" ]] && args+=(--output "$OCEANBASE_QUALIFY_OUTPUT")
exec "$BIN" "${args[@]}"
