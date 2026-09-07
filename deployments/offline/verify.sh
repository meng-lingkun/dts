#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
sha256sum -c SHA256SUMS
KUBECTL=${KUBECTL:-$ROOT/runtime/kubectl}
chmod 0755 "$KUBECTL" 2>/dev/null || true
for workload in server worker web; do
  "$KUBECTL" -n dts rollout status "deployment/$workload" --timeout=180s
done
"$KUBECTL" -n dts get pods -o wide
"$KUBECTL" -n dts get svc,pvc,hpa,pdb
"$KUBECTL" -n dts exec deployment/server -- wget -q -O - http://127.0.0.1:8080/readyz
