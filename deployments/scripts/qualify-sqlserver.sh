#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN=${QMIGRATION_SQLSERVER_QUALIFY_BIN:-"$ROOT/bin/qmigration-sqlserver-qualify"}

if [[ ! -x "$BIN" ]]; then
  echo "building qmigration-sqlserver-qualify" >&2
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/sqlserver-qualify)
fi

: "${SQLSERVER_HOST:?set SQLSERVER_HOST}"
: "${SQLSERVER_DATABASE:?set SQLSERVER_DATABASE}"
: "${SQLSERVER_USER:?set SQLSERVER_USER}"
: "${SQLSERVER_PASSWORD:?set SQLSERVER_PASSWORD}"

args=(
  --host "$SQLSERVER_HOST"
  --port "${SQLSERVER_PORT:-1433}"
  --database "$SQLSERVER_DATABASE"
  --user "$SQLSERVER_USER"
  --password-env SQLSERVER_PASSWORD
  --schema "${SQLSERVER_SCHEMA:-dbo}"
  --sample-rows "${SQLSERVER_SAMPLE_ROWS:-16}"
  --timeout "${SQLSERVER_QUALIFY_TIMEOUT:-90s}"
  --tls-mode "${SQLSERVER_TLS_MODE:-PREFERRED}"
)

[[ -n "${SQLSERVER_TABLE:-}" ]] && args+=(--table "$SQLSERVER_TABLE")
[[ "${SQLSERVER_QUALIFY_CDC:-0}" == "1" ]] && args+=(--cdc)
[[ "${SQLSERVER_QUALIFY_TARGET_WRITE:-0}" == "1" ]] && args+=(--target-write)
[[ -n "${SQLSERVER_TLS_SERVER_NAME:-}" ]] && args+=(--tls-server-name "$SQLSERVER_TLS_SERVER_NAME")
[[ -n "${SQLSERVER_TLS_CA_FILE:-}" ]] && args+=(--tls-ca-file "$SQLSERVER_TLS_CA_FILE")
[[ -n "${SQLSERVER_TLS_CERT_FILE:-}" ]] && args+=(--tls-cert-file "$SQLSERVER_TLS_CERT_FILE")
[[ -n "${SQLSERVER_TLS_KEY_FILE:-}" ]] && args+=(--tls-key-file "$SQLSERVER_TLS_KEY_FILE")
[[ -n "${SQLSERVER_QUALIFY_OUTPUT:-}" ]] && args+=(--output "$SQLSERVER_QUALIFY_OUTPUT")

exec "$BIN" "${args[@]}"
