#!/bin/sh
# Exercise packaged lifecycle scripts without contacting a cluster.
set -eu
REPO=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TEST_ROOT=$(mktemp -d)
trap 'rm -rf -- "$TEST_ROOT"' EXIT
cp "$REPO/deployments/offline/verify.sh" "$REPO/deployments/offline/uninstall.sh" "$REPO/deployments/offline/uninstall-all.sh" "$REPO/deployments/offline/install.sh" "$TEST_ROOT/"
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
if bash uninstall-all.sh /does/not/exist --yes-destroy-all >/dev/null 2>&1; then exit 1; fi
if bash uninstall-all.sh /dev/null >/dev/null 2>&1; then exit 1; fi
grep -q '/var/lib/dts-cluster-bootstrap/owned' "$REPO/deployments/offline/automation/uninstall-all.yml"
grep -q 'dts_destroy_confirmation' "$REPO/deployments/offline/automation/uninstall-all.yml"
grep -q '/run/dts-kubeadm-reset reset --force' "$REPO/deployments/offline/automation/uninstall-all.yml"
grep -q 'Remove kubectl kubelet and kubeadm from their installed paths first' "$REPO/deployments/offline/automation/uninstall-all.yml"
grep -q "dts_remote_install_dir | default('/opt/dts-offline', true)" "$REPO/deployments/offline/automation/site.yml"
grep -q 'Remove the configured installation directory and all remaining contents last' "$REPO/deployments/offline/automation/uninstall-all.yml"
grep -q 'Refusing to delete an unsafe dts_remote_install_dir' "$REPO/deployments/offline/automation/uninstall-all.yml"
grep -q 'Remove single-node PostgreSQL and CDC spool data' "$REPO/deployments/offline/automation/uninstall-all.yml"
grep -q 'Allow DTS workloads on the control plane in single-node mode' "$REPO/deployments/offline/automation/site.yml"
grep -q 'Create pre-bound local persistent volumes for single-node DTS' "$REPO/deployments/offline/automation/site.yml"
grep -q "groups\['workers'\] | default(\[\]) | length == 0" "$REPO/deployments/offline/automation/site.yml"
if grep -q "groups\['workers'\] | default(\[\]) | length > 0$" "$REPO/deployments/offline/automation/site.yml"; then
  # > 0 is valid on token/join-only tasks, but not as a topology assertion.
  if sed -n '/Validate supported topology/,/run_once: true/p' "$REPO/deployments/offline/automation/site.yml" | grep -q "workers.*length > 0"; then
    echo 'single-node topology is still rejected by validation' >&2
    exit 1
  fi
fi
grep -q '^dts_remote_install_dir=/opt/dts-offline$' "$REPO/deployments/offline/automation/inventory.single-node.example.ini"
grep -q '^dts_single_node_local_storage=true$' "$REPO/deployments/offline/automation/inventory.single-node.example.ini"
grep -q '^dts_single_node_local_storage=false$' "$REPO/deployments/offline/automation/inventory.multi-node.example.ini"
grep -q 'bin/dts-certgen --output /etc/dts-registry --ip' "$REPO/deployments/offline/install-registry.sh"
grep -q "curl --noproxy '\*'" "$REPO/deployments/offline/install-registry.sh"
grep -q 'registry_diagnostics' "$REPO/deployments/offline/install-registry.sh"
grep -q 'Reconcile all required images into the registry' "$REPO/deployments/offline/automation/site.yml"
if sed -n '/Reconcile all required images into the registry/,+4p' "$REPO/deployments/offline/automation/site.yml" | grep -q 'creates:.*\.seeded'; then
  echo 'registry reconciliation must not be skipped by a stale .seeded marker' >&2
  exit 1
fi
grep -q 'existing cluster may need new image tags' "$REPO/deployments/offline/install-registry.sh"
grep -q 'sha256sum images/all-images.tar.gz' "$REPO/deployments/offline/install-registry.sh"
grep -q 'install-cluster.sh preflight' "$REPO/deployments/offline/automation/site.yml"
grep -q 'DTS KUBERNETES NODE' "$REPO/deployments/offline/automation/site.yml"
grep -q 'Kubernetes node hostnames must be unique' "$REPO/deployments/offline/automation/site.yml"
grep -q 'kubernetesVersion: v1.32.5' "$REPO/deployments/offline/install-cluster.sh"
grep -q 'reconcile_kubernetes_tools' "$REPO/deployments/offline/install-cluster.sh"
grep -q 'Reconciled.*KUBECTL_VERSION' "$REPO/deployments/offline/install-cluster.sh"
grep -q 'installed kubeadm is.*expected' "$REPO/deployments/offline/install-cluster.sh"
grep -q 'KUBEADM_SHA256' "$REPO/deployments/offline/install-cluster.sh"
grep -q 'apply_kubernetes_network_settings' "$REPO/deployments/offline/install-cluster.sh"
grep -q 'sysctl -p /etc/sysctl.d/99-dts-kubernetes.conf' "$REPO/deployments/offline/install-cluster.sh"
grep -q 'Apply Kubernetes sysctl parameters immediately' "$REPO/deployments/offline/automation/site.yml"
grep -q 'Verify effective Kubernetes sysctl parameters' "$REPO/deployments/offline/automation/site.yml"
for setting in net.ipv4.ip_forward net.bridge.bridge-nf-call-iptables net.bridge.bridge-nf-call-ip6tables; do
  grep -q "^$setting = 1$" "$REPO/deployments/offline/cluster/99-dts-kubernetes.conf"
done
if grep -q 'failCgroupV1\|ignore-preflight-errors=SystemVerification' "$REPO/deployments/offline/install-cluster.sh"; then
  echo 'Kubernetes 1.32 must not use the 1.35+ cgroup v1 deprecation override' >&2
  exit 1
fi
grep -q 'cpu cpuacct cpuset devices freezer memory pids blkio' "$REPO/deployments/offline/install-cluster.sh"
grep -q 'Kylin kernel 4.19 requires the packaged cgroup v1 compatibility switch' "$REPO/deployments/offline/install-cluster.sh"
grep -q 'systemd.unified_cgroup_hierarchy=0' "$REPO/deployments/offline/configure-kylin-419-cgroup.sh"
grep -q 'refusing to change the cgroup hierarchy on a node that already belongs' "$REPO/deployments/offline/configure-kylin-419-cgroup.sh"
grep -q 'dts_cgroup_compatibility.rc == 194' "$REPO/deployments/offline/automation/site.yml"
grep -q 'Join Kubernetes and preserve non-secret diagnostics' "$REPO/deployments/offline/automation/site.yml"
grep -q 'Verify the worker can reach the Kubernetes API server' "$REPO/deployments/offline/automation/site.yml"
grep -q 'Remove generated credential file even when join fails' "$REPO/deployments/offline/automation/site.yml"
grep -q 'restore-kylin-419-cgroup.sh' "$REPO/deployments/offline/automation/uninstall-all.yml"
unpack_root="$TEST_ROOT/unpack"
mkdir -p "$unpack_root/images" "$unpack_root/fixture"
cp "$REPO/deployments/offline/unpack-images.sh" "$unpack_root/"
printf '[]\n' > "$unpack_root/fixture/manifest.json"
(cd "$unpack_root/fixture" && tar -cf "$unpack_root/images/expected.tar" manifest.json)
gzip -c "$unpack_root/images/expected.tar" > "$unpack_root/images/all-images.tar.gz"
printf 'stale archive\n' > "$unpack_root/images/all-images.tar"
(cd "$unpack_root" && sh unpack-images.sh >/dev/null)
cmp "$unpack_root/images/expected.tar" "$unpack_root/images/all-images.tar"
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
# Registry mode renders all workloads with pullable private references.
cat > "$KUBECTL" <<'MOCK'
#!/bin/sh
printf '%s\n' "$*" >> "$TEST_LOG"
case "$*" in
  *containerRuntimeVersion*) echo 'node-1=containerd://2.0' ;;
  'get nodes --no-headers') echo 'node-1 Ready' ;;
  'apply -f -') cat >> "$TEST_RENDERED" ;;
esac
MOCK
mkdir -p kubernetes
cp "$REPO/deployments/kubernetes/"*.yaml kubernetes/
export TEST_RENDERED="$TEST_ROOT/rendered.yaml" DTS_REGISTRY=10.20.30.40:5000
DTS_DNS_PATCH_FILE="$TEST_ROOT/customer-dns.yaml"
sh install.sh >/dev/null
for image in server web postgres; do grep -q "image: $DTS_REGISTRY/dts/$image:" "$TEST_RENDERED"; done
if grep -q 'imagePullPolicy: Never' "$TEST_RENDERED"; then echo 'Registry workloads cannot use Never' >&2; exit 1; fi
. "$REPO/deployments/offline/cluster/registry-image-map.sh"
test "$(registry_image registry.k8s.io/coredns/coredns:v1.11.3)" = "$DTS_REGISTRY/kubernetes/coredns:v1.11.3"
test "$(registry_image ghcr.io/flannel-io/flannel:v0.28.9)" = "$DTS_REGISTRY/flannel/flannel:v0.28.9"
test "$(registry_image dts/server:0.15.0-rc49)" = "$DTS_REGISTRY/dts/server:0.15.0-rc49"
if registry_image unknown.example/image:latest >/dev/null 2>&1; then exit 1; fi
echo "Kubernetes lifecycle and registry contracts passed"
