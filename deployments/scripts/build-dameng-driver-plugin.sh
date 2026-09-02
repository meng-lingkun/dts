#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
: "${DM_GO_DRIVER_DIR:?set DM_GO_DRIVER_DIR to the unpacked official DM Go driver package directory}"
OUT=${QMIGRATION_DAMENG_DRIVER_PLUGIN_OUT:-"$ROOT/bin/qmigration-dameng-driver.so"}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/provider" "$WORK/dm"
cp -a "$DM_GO_DRIVER_DIR"/. "$WORK/dm/"
if [[ ! -f "$WORK/dm/go.mod" ]]; then
  cat > "$WORK/dm/go.mod" <<'EOM'
module dm

go 1.20

require (
  github.com/golang/snappy v1.0.0
  golang.org/x/text v0.22.0
)
EOM
fi
cp "$ROOT/deployments/dameng-provider/provider.go.example" "$WORK/provider/provider.go"
cat > "$WORK/provider/go.mod" <<EOM
module qmigration-dameng-provider

go 1.20

require dm v0.0.0
replace dm => ../dm
EOM
(
  cd "$WORK/provider"
  go mod tidy
  mkdir -p "$(dirname "$OUT")"
  go build -buildmode=plugin -o "$OUT" .
)
echo "built $OUT"
echo "Use the same Go toolchain as the QMigration Server/Worker binaries."
