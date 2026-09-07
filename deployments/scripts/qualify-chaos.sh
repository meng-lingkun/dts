#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${DTS_CHAOS_QUALIFY_BIN:-$ROOT/bin/dts-chaos-qualify}"
if [[ ! -x "$BIN" ]]; then
  echo "building dts-chaos-qualify" >&2
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/chaos-qualify)
fi
exec "$BIN" "$@"
