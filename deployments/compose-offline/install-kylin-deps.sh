#!/bin/sh
set -eu
cd "$(dirname "$0")"
missing=
command -v iptables >/dev/null || missing=iptables
command -v flock >/dev/null || missing="$missing util-linux"
[ -n "$missing" ] || exit 0
[ "$(id -u)" = 0 ] && [ "$(uname -m)" = x86_64 ] || { echo "Use root on a Kylin x86_64 node" >&2; exit 1; }
. /etc/os-release
case "${ID:-}:${VERSION_ID:-}" in kylin:V10|kylin:10|kylin:10.*|Kylin:V10|Kylin:10|Kylin:10.*) ;;
  *) echo "Bundled RPMs target Kylin V10 SP3 2403 only" >&2; exit 1;;
esac
sha256sum -c --quiet SHA256SUMS
if command -v dnf >/dev/null; then manager=dnf; else manager=yum; fi
repo=$(pwd -P)/kylin/rpms
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
mkdir "$work/repos"
cat > "$work/repos/dts-host.repo" <<EOF
[dts-host]
name=DTS Kylin V10 SP3 2403 offline host dependencies
baseurl=file://$repo
enabled=1
gpgcheck=1
repo_gpgcheck=0
EOF
# Exclude update candidates for all installed package names. An incompatible
# host must fail dependency resolution instead of upgrading its system packages.
rpm -qa --qf '%{NAME}\n' > "$work/installed"
installed=$(LC_ALL=C sort -u "$work/installed" | paste -sd, -)
for package in $missing; do
  if grep -Fxq "$package" "$work/installed"; then
    echo "Installed $package lacks its required command. Check PATH or repair it using the OS package manager." >&2; exit 1
  fi
done
"$manager" -y --noplugins --setopt="reposdir=$work/repos" --setopt="cachedir=$work/cache" \
  --setopt="dts-host.exclude=$installed" --setopt=obsoletes=0 --setopt=install_weak_deps=False \
  --disablerepo='*' --enablerepo=dts-host install $missing
command -v iptables >/dev/null && command -v flock >/dev/null
