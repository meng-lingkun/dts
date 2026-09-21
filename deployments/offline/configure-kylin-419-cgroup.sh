#!/bin/sh
# Configure the supported DTS compatibility profile for Kylin's 4.19 kernel.
set -eu

[ "$(uname -s)" = Linux ] && [ "$(uname -m)" = x86_64 ] || { echo 'ERROR: Linux x86_64 required' >&2; exit 1; }
[ "$(id -u)" = 0 ] || { echo 'ERROR: run as root' >&2; exit 1; }
. /etc/os-release
case "${ID:-}" in kylin|Kylin) ;; *) echo 'ERROR: this compatibility profile only supports Kylin V10' >&2; exit 1 ;; esac

kernel_version=$(uname -r | sed 's/[^0-9.].*$//')
kernel_major=$(printf '%s\n' "$kernel_version" | cut -d. -f1)
kernel_minor=$(printf '%s\n' "$kernel_version" | cut -d. -f2)
if [ "$kernel_major" != 4 ] || [ "$kernel_minor" != 19 ]; then
  echo "Kernel $(uname -r) does not require the Kylin 4.19 cgroup v1 compatibility switch."
  exit 0
fi

cgroup_type=$(stat -fc %T /sys/fs/cgroup/)
state=/var/lib/dts-cluster-bootstrap
if [ "$cgroup_type" = tmpfs ]; then
  awk '$1 == "cpuset" && $4 == 1 {found=1} END {exit !found}' /proc/cgroups || {
    echo 'ERROR: the Kylin 4.19 kernel does not expose an enabled cgroup v1 cpuset controller' >&2; exit 1;
  }
  awk '$3 == "cgroup" && $4 ~ /(^|,)cpuset(,|$)/ {found=1} END {exit !found}' /proc/mounts || {
    echo 'ERROR: cgroup v1 cpuset is enabled but not mounted; repair the OS cgroup mounts before deployment' >&2; exit 1;
  }
  rm -f -- "$state/reboot-required-for-cgroup-v1"
  echo "Kylin 4.19 compatibility ready: cgroup v1 cpuset is mounted."
  exit 0
fi

[ "$cgroup_type" = cgroup2fs ] || { echo "ERROR: unsupported cgroup root filesystem: $cgroup_type" >&2; exit 1; }
[ ! -e /etc/kubernetes/kubelet.conf ] && [ ! -e /etc/kubernetes/admin.conf ] || {
  echo 'ERROR: refusing to change the cgroup hierarchy on a node that already belongs to a Kubernetes cluster' >&2; exit 1;
}
command -v grubby >/dev/null 2>&1 || { echo 'ERROR: grubby is required to configure the Kylin boot entry' >&2; exit 1; }
install -d -m 0700 "$state"
if [ ! -f "$state/grubby-info.before-cgroup-v1.txt" ]; then
  grubby --info=ALL > "$state/grubby-info.before-cgroup-v1.txt"
  chmod 0600 "$state/grubby-info.before-cgroup-v1.txt"
fi

# Linux 4.19 lacks the cgroup v2 cpuset controller required by kubeadm. Use the
# kernel's mature cgroup v1 implementation instead. grubby updates every boot
# entry so a later kernel selection does not silently return to the bad mode.
grubby --update-kernel=ALL --remove-args='systemd.unified_cgroup_hierarchy cgroup_no_v1 systemd.legacy_systemd_cgroup_controller'
grubby --update-kernel=ALL --args='systemd.unified_cgroup_hierarchy=0 systemd.legacy_systemd_cgroup_controller'
grubby --info=DEFAULT | grep -q 'systemd.unified_cgroup_hierarchy=0' || {
  echo 'ERROR: grubby did not persist the cgroup v1 boot parameter' >&2; exit 1;
}
touch "$state/reboot-required-for-cgroup-v1"
echo 'REBOOT_REQUIRED: configured full cgroup v1 for the Kylin 4.19 kernel.'
exit 194
