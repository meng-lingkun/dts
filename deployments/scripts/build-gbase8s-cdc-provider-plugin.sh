#!/usr/bin/env bash
set -euo pipefail
: "${GBASE8S_CDC_PROVIDER_DIR:?set GBASE8S_CDC_PROVIDER_DIR to a local Go package that implements NewProvider() using GBase Client-SDK}"
out=${GBASE8S_CDC_PROVIDER_OUTPUT:-bin/qmigration-gbase8s-cdc-provider.so}
mkdir -p "$(dirname "$out")"
go build -buildmode=plugin -o "$out" "$GBASE8S_CDC_PROVIDER_DIR"
echo "$out"
