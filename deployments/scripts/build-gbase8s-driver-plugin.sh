#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
: "${GBASE8S_GO_ODBC_DRIVER_DIR:?set GBASE8S_GO_ODBC_DRIVER_DIR to an unpacked Go database/sql ODBC wrapper source tree}"
OUT=${QMIGRATION_GBASE8S_DRIVER_PLUGIN_OUT:-"$ROOT/bin/qmigration-gbase8s-driver.so"}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/provider" "$WORK/odbc"
cp -a "$GBASE8S_GO_ODBC_DRIVER_DIR"/. "$WORK/odbc/"
if [[ ! -f "$WORK/odbc/go.mod" ]]; then
  cat > "$WORK/odbc/go.mod" <<'EOM'
module odbc

go 1.20
EOM
fi
cp "$ROOT/deployments/gbase8s-provider/provider.go.example" "$WORK/provider/provider.go"
cat > "$WORK/provider/go.mod" <<EOM
module qmigration-gbase8s-provider

go 1.20

require odbc v0.0.0
replace odbc => ../odbc
EOM
(
  cd "$WORK/provider"
  go mod tidy
  mkdir -p "$(dirname "$OUT")"
  CGO_ENABLED=1 go build -buildmode=plugin -o "$OUT" .
)
echo "built $OUT"
echo "Runtime also needs unixODBC and the matching GBase Client-SDK ODBC libraries/LD_LIBRARY_PATH."
echo "Use the same Go toolchain as the QMigration Server/Worker binaries."
