#!/bin/sh
# Restore the boot cgroup arguments recorded before DTS compatibility setup.
set -eu
state=/var/lib/dts-cluster-bootstrap
before="$state/grubby-info.before-cgroup-v1.txt"
[ -f "$before" ] || { echo 'No DTS cgroup boot change was recorded.'; exit 0; }
command -v grubby >/dev/null 2>&1 || { echo 'ERROR: grubby is required to restore the boot entry' >&2; exit 1; }

grubby --update-kernel=ALL --remove-args='systemd.unified_cgroup_hierarchy cgroup_no_v1 systemd.legacy_systemd_cgroup_controller'
restore_args=
if grep -q 'systemd.unified_cgroup_hierarchy=1' "$before"; then
  restore_args='systemd.unified_cgroup_hierarchy=1'
fi
if grep -q 'cgroup_no_v1=all' "$before"; then
  restore_args="$restore_args cgroup_no_v1=all"
fi
if grep -q 'systemd.legacy_systemd_cgroup_controller' "$before"; then
  restore_args="$restore_args systemd.legacy_systemd_cgroup_controller"
fi
if [ -n "$restore_args" ]; then grubby --update-kernel=ALL --args="$restore_args"; fi
echo 'REBOOT_REQUIRED: restored the pre-DTS cgroup boot arguments.'
exit 194
