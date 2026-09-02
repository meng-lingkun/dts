#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${QMIGRATION_KINGBASE_QUALIFY_BIN:-$ROOT/bin/qmigration-kingbase-qualify}"
if [[ ! -x "$BIN" ]]; then
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/kingbase-qualify)
fi
exec "$BIN" "$@"
