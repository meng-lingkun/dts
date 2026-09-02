#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN=${QMIGRATION_GAUSSDB_QUALIFY_BIN:-"$ROOT/bin/qmigration-gaussdb-qualify"}
if [[ ! -x "$BIN" ]]; then
  echo "building qmigration-gaussdb-qualify" >&2
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/gaussdb-qualify)
fi
: "${GAUSSDB_HOST:?set GAUSSDB_HOST}"
: "${GAUSSDB_USER:?set GAUSSDB_USER}"
: "${GAUSSDB_PASSWORD:?set GAUSSDB_PASSWORD}"
args=(
  --host "$GAUSSDB_HOST"
  --port "${GAUSSDB_PORT:-8000}"
  --user "$GAUSSDB_USER"
  --password-env GAUSSDB_PASSWORD
  --database "${GAUSSDB_DATABASE:-$GAUSSDB_USER}"
  --schema "${GAUSSDB_SCHEMA:-public}"
  --sample-rows "${GAUSSDB_SAMPLE_ROWS:-16}"
  --timeout "${GAUSSDB_QUALIFY_TIMEOUT:-90s}"
  --tls-mode "${GAUSSDB_TLS_MODE:-DISABLE}"
)
[[ -n "${GAUSSDB_TABLE:-}" ]] && args+=(--table "$GAUSSDB_TABLE")
[[ "${GAUSSDB_QUALIFY_CDC:-0}" == "1" ]] && args+=(--cdc)
[[ -n "${GAUSSDB_TLS_SERVER_NAME:-}" ]] && args+=(--tls-server-name "$GAUSSDB_TLS_SERVER_NAME")
[[ -n "${GAUSSDB_TLS_CA_FILE:-}" ]] && args+=(--tls-ca-file "$GAUSSDB_TLS_CA_FILE")
[[ -n "${GAUSSDB_TLS_CERT_FILE:-}" ]] && args+=(--tls-cert-file "$GAUSSDB_TLS_CERT_FILE")
[[ -n "${GAUSSDB_TLS_KEY_FILE:-}" ]] && args+=(--tls-key-file "$GAUSSDB_TLS_KEY_FILE")
[[ -n "${GAUSSDB_QUALIFY_OUTPUT:-}" ]] && args+=(--output "$GAUSSDB_QUALIFY_OUTPUT")
exec "$BIN" "${args[@]}"
