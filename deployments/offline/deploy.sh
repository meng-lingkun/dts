#!/bin/bash
# Shell entry point for offline Ansible deployment on a Kylin control machine.
set -euo pipefail
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
inventory=${1:-}
if [ -z "$inventory" ] || [ ! -r "$inventory" ]; then
  echo 'Usage: sudo bash deploy.sh /absolute/inventory.ini [ansible-playbook options]' >&2; exit 1
fi
inventory=$(realpath "$inventory")
shift
cd "$ROOT"
[ "$(id -u)" = 0 ] || { echo 'ERROR: run as root on the Kylin orchestration host' >&2; exit 1; }
sha256sum -c SHA256SUMS >/dev/null
if command -v yum >/dev/null 2>&1; then
  package_manager=yum
elif command -v dnf >/dev/null 2>&1; then
  package_manager=dnf
else
  echo 'ERROR: yum or dnf from the Kylin base OS is required' >&2; exit 1
fi
repo_config_dir=$(mktemp -d /tmp/dts-yum-repo.XXXXXX)
client_cache=
cleanup() {
  rm -f -- "$repo_config_dir/dts-offline.repo"
  rmdir -- "$repo_config_dir" 2>/dev/null || true
  if [ -n "$client_cache" ]; then
    rm -f -- "$client_cache/endpoint" "$client_cache/ca.crt" "$client_cache/client.crt" "$client_cache/client.key"
    rmdir -- "$client_cache" 2>/dev/null || true
  fi
}
trap cleanup EXIT
cat > "$repo_config_dir/dts-offline.repo" <<EOF
[dts-offline]
name=DTS Kylin offline repository
baseurl=file://$ROOT/cluster/rpms
enabled=1
gpgcheck=1
repo_gpgcheck=0
EOF
if ! command -v ansible-playbook >/dev/null || ! command -v rsync >/dev/null; then
  "$package_manager" -y --setopt="reposdir=$repo_config_dir" --disablerepo='*' --enablerepo=dts-offline --setopt=install_weak_deps=False install ansible rsync openssh-clients
fi
client_cache=$(mktemp -d /tmp/dts-registry-client.XXXXXX)
chmod 0700 "$client_cache"
export ANSIBLE_HOST_KEY_CHECKING=True
export DTS_PACKAGE_ROOT="$ROOT" DTS_REGISTRY_CLIENT_CACHE="$client_cache"
ansible-playbook -i "$inventory" automation/site.yml \
  -e @automation/paths.yml "$@"
