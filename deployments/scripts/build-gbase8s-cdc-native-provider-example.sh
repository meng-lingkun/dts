#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DIR="$ROOT/deployments/gbase8s-cdc-provider"
OUT=${GBASE8S_CDC_NATIVE_PROVIDER_EXAMPLE_OUT:-"$ROOT/bin/qmigration-gbase8s-cdc-provider-example.so"}
CC=${CC:-cc}
mkdir -p "$(dirname "$OUT")"
"$CC" -x c -shared -fPIC -O2 -Wall -Wextra -Werror -I"$DIR" -o "$OUT" "$DIR/native_provider.c.example"
echo "built ABI-only example: $OUT"
echo "This example intentionally fails open; replace it with the local GBase CSDK implementation."
