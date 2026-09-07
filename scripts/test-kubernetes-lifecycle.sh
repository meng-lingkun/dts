#!/bin/sh
# Exercise packaged lifecycle scripts without contacting a cluster.
set -eu
REPO=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TEST_ROOT=$(mktemp -d)
trap 'rm -rf -- "$TEST_ROOT"' EXIT
cp "$REPO/deployments/offline/verify.sh" "$REPO/deployments/offline/uninstall.sh" "$REPO/deployments/offline/install.sh" "$TEST_ROOT/"
export TEST_LOG="$TEST_ROOT/calls"
export KUBECTL="$TEST_ROOT/kubectl"
cat > "$KUBECTL" <<'MOCK'
#!/bin/sh
printf '%s\n' "$*" >> "$TEST_LOG"
MOCK
chmod +x "$KUBECTL"
cd "$TEST_ROOT"
sha256sum verify.sh uninstall.sh > SHA256SUMS
sh verify.sh >/dev/null
grep -q 'rollout status deployment/server' "$TEST_LOG"
grep -q 'exec deployment/server -- wget' "$TEST_LOG"
sh uninstall.sh >/dev/null
grep -q 'delete deployment server worker web' "$TEST_LOG"
if grep -E 'delete.*(namespace|secret|configmap|pvc|statefulset|postgres)' "$TEST_LOG"; then
  echo "uninstall deleted a retained resource" >&2
  exit 1
fi
before=$(wc -l < "$TEST_LOG")
if sh uninstall.sh --purge >/dev/null 2>&1; then exit 1; fi
test "$before" -eq "$(wc -l < "$TEST_LOG")"
printf '\n# tamper\n' >> verify.sh
if sh verify.sh >/dev/null 2>&1; then exit 1; fi
test "$before" -eq "$(wc -l < "$TEST_LOG")"
cat > install-kubernetes.sh <<'MOCK'
#!/bin/sh
test "$1" = "argument with spaces"
exit 7
MOCK
status=0
sh install.sh "argument with spaces" || status=$?
test "$status" -eq 7

# An unsupported node runtime must fail before namespace/Secret mutations.
cp "$REPO/deployments/offline/install-kubernetes.sh" .
mkdir mocks
cat > mocks/uname <<'MOCK'
#!/bin/sh
case "$1" in -s) echo Linux ;; -m) echo x86_64 ;; esac
MOCK
cat > "$KUBECTL" <<'MOCK'
#!/bin/sh
printf '%s\n' "$*" >> "$TEST_LOG"
case "$*" in
  *containerRuntimeVersion*) printf 'node-1=%s\n' "$TEST_RUNTIME" ;;
  'get nodes --no-headers') echo 'node-1 NotReady' ;;
esac
MOCK
chmod +x mocks/uname "$KUBECTL"
PATH="$TEST_ROOT/mocks:$PATH"
export PATH TEST_RUNTIME
sha256sum install-kubernetes.sh > SHA256SUMS
TEST_RUNTIME=docker://24.0
if sh install.sh > output 2>&1; then exit 1; fi
grep -q 'requires Kubernetes nodes using containerd' output
TEST_RUNTIME=containerd://2.0
if sh install.sh > output 2>&1; then exit 1; fi
grep -q 'no Ready nodes' output
if grep -E '^(create|apply|patch|scale|set) ' "$TEST_LOG"; then
  echo "runtime/readiness validation mutated the cluster" >&2
  exit 1
fi
# DNS override is applied to both execution roles and validated first.
cat > "$KUBECTL" <<'MOCK'
#!/bin/sh
printf '%s\n' "$*" >> "$TEST_LOG"
case "$*" in
  *containerRuntimeVersion*) echo 'node-1=containerd://2.0' ;;
  'get nodes --no-headers') echo 'node-1 Ready' ;;
esac
MOCK
cp "$REPO/deployments/kubernetes/dns-hosts.patch.example.yaml" "$TEST_ROOT/customer-dns.yaml"
export DTS_DNS_PATCH_FILE="$TEST_ROOT/customer-dns.yaml"
sh install.sh >/dev/null
for role in server worker; do
  grep -q "patch deployment $role --type=merge --patch-file $DTS_DNS_PATCH_FILE --dry-run=server" "$TEST_LOG"
  grep -q "patch deployment $role --type=merge --patch-file $DTS_DNS_PATCH_FILE$" "$TEST_LOG"
done
before=$(wc -l < "$TEST_LOG")
DTS_DNS_PATCH_FILE="$TEST_ROOT/missing.yaml"
if sh install.sh > output 2>&1; then exit 1; fi
grep -q 'must name a readable deployment patch' output
test "$before" -eq "$(wc -l < "$TEST_LOG")"
echo "Kubernetes lifecycle contract passed"
