#!/usr/bin/env bash
# Against the disposable MySQL/PostgreSQL instances in qualification/compose.yaml.
# Use DTS_SOAK_SECONDS=259200 for 72 h, or e.g. 20 for a harness smoke run.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
seconds="${DTS_SOAK_SECONDS:-86400}"
if ! [[ "$seconds" =~ ^[1-9][0-9]{0,5}$ ]] || ((seconds < 5 || seconds > 259200)); then
  echo "DTS_SOAK_SECONDS must be 5–259200" >&2
  exit 2
fi
out="${DTS_SOAK_EVIDENCE_DIR:-$ROOT/artifacts/continuous-cdc-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$out"
out="$(cd "$out" && pwd)"
export DTS_E2E_ISOLATED=1 DTS_E2E_POSTGRES_HOST=127.0.0.1 DTS_E2E_MYSQL_HOST=127.0.0.1
export DTS_E2E_CONTINUOUS_SECONDS="$seconds" DTS_E2E_CONTINUOUS_EVIDENCE="$out/continuous-cdc.jsonl"
cd "$ROOT/backend"
go test -tags integration ./integration -run '^TestContinuousPostgresCDC$' -count=1 -v -timeout "$((seconds+300))s"
