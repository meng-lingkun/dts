#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN=${DTS_GBASE_QUALIFY_BIN:-"$ROOT/bin/dts-gbase-qualify"}
if [[ ! -x "$BIN" ]]; then
  echo "building dts-gbase-qualify" >&2
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/gbase-qualify)
fi
: "${GBASE_HOST:?set GBASE_HOST}"
: "${GBASE_USER:?set GBASE_USER}"
: "${GBASE_PASSWORD:?set GBASE_PASSWORD}"
: "${GBASE_DATABASE:?set GBASE_DATABASE}"
args=(
  --host "$GBASE_HOST"
  --port "${GBASE_PORT:-5258}"
  --user "$GBASE_USER"
  --password-env GBASE_PASSWORD
  --database "$GBASE_DATABASE"
  --sample-rows "${GBASE_SAMPLE_ROWS:-16}"
  --timeout "${GBASE_QUALIFY_TIMEOUT:-90s}"
)
[[ -n "${GBASE_TABLE:-}" ]] && args+=(--table "$GBASE_TABLE")
[[ "${GBASE_QUALIFY_TARGET_WRITE:-0}" == "1" ]] && args+=(--target-write)
[[ -n "${GBASE_QUALIFY_OUTPUT:-}" ]] && args+=(--output "$GBASE_QUALIFY_OUTPUT")
export DTS_EXPERIMENTAL_GBASE8A_NATIVE=1
exec "$BIN" "${args[@]}"
