#!/bin/sh
# Install verified tools only; cluster bootstrap is a separate operation.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
[ "$(uname -s)" = Linux ] || { echo 'ERROR: Linux required' >&2; exit 1; }
case "$(uname -m)" in x86_64|amd64) ;; *) echo 'ERROR: linux/amd64 tools only' >&2; exit 1 ;; esac
[ "$(id -u)" = 0 ] || { echo 'ERROR: run as root' >&2; exit 1; }
sha256sum -c SHA256SUMS >/dev/null
. ./runtime/versions.env
destination="/opt/dts/kubernetes/$KUBECTL_VERSION/bin"
# Preflight all targets before copying; never replace a differing binary.
for tool in kubectl kubeadm kubelet; do
  if [ -e "$destination/$tool" ] && ! cmp -s "runtime/$tool" "$destination/$tool"; then
    echo "ERROR: differing existing file $destination/$tool" >&2
    exit 1
  fi
done
install -d -m 0755 "$destination"
for tool in kubectl kubeadm kubelet; do
  install -m 0755 "runtime/$tool" "$destination/$tool"
done
echo "Installed Kubernetes tools: $destination"
printf 'Add to your shell: export PATH="%s:$PATH"\n' "$destination"
"$destination/kubectl" version --client
"$destination/kubeadm" version -o short
"$destination/kubelet" --version
echo 'Host dependency inventory (missing entries must be supplied separately):'
for dependency in systemctl containerd ctr runc crictl conntrack iptables ip mount; do
  if command -v "$dependency" >/dev/null 2>&1; then
    printf 'FOUND %s\n' "$dependency"
  else
    printf 'MISSING %s\n' "$dependency"
  fi
done
echo 'Tools installed only. No cluster was initialized and no system services were changed.'
echo 'A new cluster also needs runtime/CNI, control-plane images, OS dependencies and kubeconfig; see README.md.'
