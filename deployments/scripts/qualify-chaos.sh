#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${QMIGRATION_CHAOS_QUALIFY_BIN:-$ROOT/bin/qmigration-chaos-qualify}"
if [[ ! -x "$BIN" ]]; then
  echo "building qmigration-chaos-qualify" >&2
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/chaos-qualify)
fi
exec "$BIN" "$@"
