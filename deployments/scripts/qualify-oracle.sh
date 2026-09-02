#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN=${QMIGRATION_ORACLE_QUALIFY_BIN:-"$ROOT/bin/qmigration-oracle-qualify"}

if [[ ! -x "$BIN" ]]; then
  echo "building qmigration-oracle-qualify" >&2
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/oracle-qualify)
fi

: "${ORACLE_HOST:?set ORACLE_HOST}"
: "${ORACLE_SERVICE:?set ORACLE_SERVICE}"
: "${ORACLE_USER:?set ORACLE_USER}"
: "${ORACLE_PASSWORD:?set ORACLE_PASSWORD}"

args=(
  --host "$ORACLE_HOST"
  --port "${ORACLE_PORT:-1521}"
  --service "$ORACLE_SERVICE"
  --user "$ORACLE_USER"
  --password-env ORACLE_PASSWORD
  --schema "${ORACLE_SCHEMA:-$ORACLE_USER}"
  --sample-rows "${ORACLE_SAMPLE_ROWS:-16}"
  --timeout "${ORACLE_QUALIFY_TIMEOUT:-90s}"
  --tls-mode "${ORACLE_TLS_MODE:-DISABLE}"
)

[[ -n "${ORACLE_TABLE:-}" ]] && args+=(--table "$ORACLE_TABLE")
[[ "${ORACLE_QUALIFY_CDC:-0}" == "1" ]] && args+=(--cdc)
[[ "${ORACLE_QUALIFY_TARGET_WRITE:-0}" == "1" ]] && args+=(--target-write)
[[ -n "${ORACLE_TLS_SERVER_NAME:-}" ]] && args+=(--tls-server-name "$ORACLE_TLS_SERVER_NAME")
[[ -n "${ORACLE_TLS_CA_FILE:-}" ]] && args+=(--tls-ca-file "$ORACLE_TLS_CA_FILE")
[[ -n "${ORACLE_TLS_CERT_FILE:-}" ]] && args+=(--tls-cert-file "$ORACLE_TLS_CERT_FILE")
[[ -n "${ORACLE_TLS_KEY_FILE:-}" ]] && args+=(--tls-key-file "$ORACLE_TLS_KEY_FILE")
[[ -n "${ORACLE_QUALIFY_OUTPUT:-}" ]] && args+=(--output "$ORACLE_QUALIFY_OUTPUT")

exec "$BIN" "${args[@]}"
