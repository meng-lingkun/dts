#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${DTS_OPENGAUSS_QUALIFY_BIN:-$ROOT/bin/dts-opengauss-qualify}"
if [[ ! -x "$BIN" ]]; then
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/opengauss-qualify)
fi
exec "$BIN" "$@"
