#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN=${QMIGRATION_DB2_QUALIFY_BIN:-"$ROOT/bin/qmigration-db2-qualify"}
if [[ ! -x "$BIN" ]]; then
  echo "building qmigration-db2-qualify" >&2
  (cd "$ROOT/backend" && go build -o "$BIN" ./cmd/db2-qualify)
fi
: "${DB2_HOST:?set DB2_HOST}"
: "${DB2_DATABASE:?set DB2_DATABASE}"
: "${DB2_USER:?set DB2_USER}"
: "${DB2_PASSWORD:?set DB2_PASSWORD}"
args=(
  --host "$DB2_HOST"
  --port "${DB2_PORT:-50000}"
  --database "$DB2_DATABASE"
  --user "$DB2_USER"
  --password-env DB2_PASSWORD
  --schema "${DB2_SCHEMA:-${DB2_USER^^}}"
  --sample-rows "${DB2_SAMPLE_ROWS:-16}"
  --timeout "${DB2_QUALIFY_TIMEOUT:-90s}"
  --tls-mode "${DB2_TLS_MODE:-PREFERRED}"
)
[[ -n "${DB2_TABLE:-}" ]] && args+=(--table "$DB2_TABLE")
[[ "${DB2_QUALIFY_TARGET_WRITE:-0}" == "1" ]] && args+=(--target-write)
[[ "${DB2_QUALIFY_TARGET_VECTOR:-0}" == "1" ]] && args+=(--target-vector)
if [[ "${DB2_QUALIFY_CDC:-0}" == "1" ]]; then
  : "${DB2_CDC_URL:?set DB2_CDC_URL when DB2_QUALIFY_CDC=1}"
  args+=(--cdc --cdc-url "$DB2_CDC_URL")
  [[ -n "${DB2_CDC_CA_FILE:-}" ]] && args+=(--cdc-ca-file "$DB2_CDC_CA_FILE")
  [[ -n "${DB2_CDC_SERVER_NAME:-}" ]] && args+=(--cdc-server-name "$DB2_CDC_SERVER_NAME")
  [[ -n "${DB2_CDC_TOKEN_ENV:-}" ]] && args+=(--cdc-token-env "$DB2_CDC_TOKEN_ENV")
fi
[[ -n "${DB2_TLS_SERVER_NAME:-}" ]] && args+=(--tls-server-name "$DB2_TLS_SERVER_NAME")
[[ -n "${DB2_TLS_CA_FILE:-}" ]] && args+=(--tls-ca-file "$DB2_TLS_CA_FILE")
[[ -n "${DB2_TLS_CERT_FILE:-}" ]] && args+=(--tls-cert-file "$DB2_TLS_CERT_FILE")
[[ -n "${DB2_TLS_KEY_FILE:-}" ]] && args+=(--tls-key-file "$DB2_TLS_KEY_FILE")
[[ -n "${DB2_QUALIFY_OUTPUT:-}" ]] && args+=(--output "$DB2_QUALIFY_OUTPUT")
exec "$BIN" "${args[@]}"
