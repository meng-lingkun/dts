#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${QMIGRATION_OPENGAUSS_QUALIFY_BIN:-$ROOT/bin/qmigration-opengauss-qualify}"
if [[ ! -x "$BIN" ]]; then
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/opengauss-qualify)
fi
exec "$BIN" "$@"
