#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN=${QMIGRATION_DAMENG_QUALIFY_BIN:-"$ROOT/bin/qmigration-dameng-qualify"}
if [[ ! -x "$BIN" ]]; then
  echo "building qmigration-dameng-qualify" >&2
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/dameng-qualify)
fi
: "${DAMENG_HOST:?set DAMENG_HOST}"
: "${DAMENG_USER:?set DAMENG_USER}"
: "${DAMENG_PASSWORD:?set DAMENG_PASSWORD}"
args=(
  --host "$DAMENG_HOST"
  --port "${DAMENG_PORT:-5236}"
  --user "$DAMENG_USER"
  --password-env DAMENG_PASSWORD
  --schema "${DAMENG_SCHEMA:-$DAMENG_USER}"
  --sample-rows "${DAMENG_SAMPLE_ROWS:-16}"
  --timeout "${DAMENG_QUALIFY_TIMEOUT:-90s}"
)
[[ -n "${DAMENG_TABLE:-}" ]] && args+=(--table "$DAMENG_TABLE")
[[ "${DAMENG_QUALIFY_TARGET_WRITE:-0}" == "1" ]] && args+=(--target-write)
[[ "${DAMENG_QUALIFY_CDC:-0}" == "1" ]] && args+=(--cdc)
[[ -n "${DAMENG_QUALIFY_OUTPUT:-}" ]] && args+=(--output "$DAMENG_QUALIFY_OUTPUT")
exec "$BIN" "${args[@]}"
