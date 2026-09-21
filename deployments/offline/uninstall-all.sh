#!/bin/bash
# Destroy a DTS-owned kubeadm cluster, its registry, containerd data and DTS data.
set -euo pipefail
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
inventory=${1:-}
confirmation=${2:-}
if [ -z "$inventory" ] || [ ! -r "$inventory" ] || [ "$confirmation" != '--yes-destroy-all' ]; then
  echo 'Usage: sudo bash uninstall-all.sh /absolute/inventory.ini --yes-destroy-all' >&2
  echo 'WARNING: deletes the DTS namespace, PostgreSQL/PVCs, kubeadm cluster, registry, containerd data, configured installation directory, and single-node local data.' >&2
  exit 2
fi
[ "$(id -u)" = 0 ] || { echo 'ERROR: run as root on the original Ansible orchestration host' >&2; exit 1; }
inventory=$(realpath "$inventory")
cd "$ROOT"
sha256sum -c SHA256SUMS >/dev/null

if ! command -v ansible-playbook >/dev/null 2>&1; then
  if command -v yum >/dev/null 2>&1; then
    package_manager=yum
  elif command -v dnf >/dev/null 2>&1; then
    package_manager=dnf
  else
    echo 'ERROR: ansible-playbook is unavailable and neither yum nor dnf exists' >&2; exit 1
  fi
  repo_config_dir=$(mktemp -d /tmp/dts-yum-repo.XXXXXX)
  cleanup() { rm -f -- "$repo_config_dir/dts-offline.repo"; rmdir -- "$repo_config_dir" 2>/dev/null || true; }
  trap cleanup EXIT
  cat > "$repo_config_dir/dts-offline.repo" <<EOF
[dts-offline]
name=DTS Kylin offline repository
baseurl=file://$ROOT/cluster/rpms
enabled=1
gpgcheck=1
repo_gpgcheck=0
EOF
  "$package_manager" -y --setopt="reposdir=$repo_config_dir" --disablerepo='*' --enablerepo=dts-offline --setopt=install_weak_deps=False install ansible openssh-clients
fi

export ANSIBLE_HOST_KEY_CHECKING=True
ansible-playbook -i "$inventory" automation/uninstall-all.yml \
  -e dts_destroy_confirmation=DESTROY_DTS_CLUSTER

echo 'DTS-owned Kubernetes environment removal completed on inventory nodes.'
echo 'The configured remote installation directory and any enabled single-node local data directory were removed.'
echo "The local delivery archive and inventory were retained: $ROOT and $inventory"
echo 'External NFS/CSI storage and host firewall rules are not blindly erased; review the printed storage warning.'
