#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
case "${1:-}" in
  '') ;;
  *) echo "Usage: sh uninstall.sh (retains PostgreSQL, PVCs and Secrets)" >&2; exit 2 ;;
esac
sha256sum -c SHA256SUMS >/dev/null
KUBECTL=${KUBECTL:-$ROOT/runtime/kubectl}
chmod 0755 "$KUBECTL" 2>/dev/null || true
"$KUBECTL" -n dts delete hpa worker --ignore-not-found
"$KUBECTL" -n dts delete deployment server worker web --ignore-not-found --wait=true
"$KUBECTL" -n dts delete daemonset image-preflight --ignore-not-found
"$KUBECTL" -n dts delete ingress dts --ignore-not-found
"$KUBECTL" -n dts delete service server web --ignore-not-found
"$KUBECTL" -n dts delete pdb server worker web --ignore-not-found
echo "DTS application stopped. PostgreSQL, all PVCs, ConfigMap, Secrets and namespace retained."
echo "Reinstall with the same database and storage configuration to resume."
