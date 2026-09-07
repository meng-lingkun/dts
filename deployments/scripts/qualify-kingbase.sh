#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${DTS_KINGBASE_QUALIFY_BIN:-$ROOT/bin/dts-kingbase-qualify}"
if [[ ! -x "$BIN" ]]; then
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/kingbase-qualify)
fi
exec "$BIN" "$@"
