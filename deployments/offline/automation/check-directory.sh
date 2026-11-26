#!/bin/sh
# Run on the inventory host before sync or recursive deletion.
set -eu
role=$1
path=$2
state=/var/lib/dts-cluster-bootstrap
fail() { echo "ERROR: unsafe DTS $role directory $path: $1" >&2; exit 1; }
case "$role" in package|storage) ;; *) exit 2;; esac
case "$path" in /*) ;; *) fail 'absolute path required';; esac
[ "$(realpath -m -- "$path")" = "$path" ] || fail 'symlink or noncanonical path'
case "$path" in /|/bin|/boot|/dev|/etc|/home|/opt|/proc|/root|/run|/sbin|/sys|/usr|/usr/local|/var|/var/lib) fail 'system directory';; esac
if [ -f "$state/$role-root" ]; then
  [ "$(cat "$state/$role-root")" = "$path" ] || fail 'inventory differs from the recorded installation'
  [ ! -e "$path" ] || [ -d "$path" ] || fail 'not a directory'
  exit 0
fi
[ -e "$path" ] || exit 0
[ -d "$path" ] || fail 'not a directory'
contents=$(ls -A -- "$path")
[ -n "$contents" ] || exit 0
if [ "$role" = package ]; then
  [ -f "$path/SHA256SUMS" ] && [ -f "$path/install-cluster.sh" ] && [ -f "$path/runtime/versions.env" ] && exit 0
elif [ -f "$state/owned" ] && [ -r /etc/kubernetes/admin.conf ]; then
  # Packages predating the directory record used these two named hostPath PVs.
  for entry in postgres spool; do
    actual=$(/usr/local/bin/kubectl --kubeconfig /etc/kubernetes/admin.conf get pv "dts-single-$entry" -o jsonpath='{.spec.hostPath.path}')
    [ "$actual" = "$path/$entry" ] || fail 'existing volume points to a different directory'
  done
  exit 0
fi
fail 'nonempty directory is not identified as DTS-owned'
