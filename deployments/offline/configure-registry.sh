#!/bin/sh
# Install registry CA and mutual TLS client identity on a prepared node.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
[ "$(id -u)" = 0 ] || { echo 'ERROR: root required' >&2; exit 1; }
[ "$#" = 1 ] || { echo 'Usage: sh configure-registry.sh /absolute/client-bundle-directory' >&2; exit 1; }
sha256sum -c SHA256SUMS >/dev/null
[ -f /var/lib/dts-cluster-bootstrap/prepared ] || { echo 'ERROR: prepare the node first' >&2; exit 1; }
[ ! -e /etc/kubernetes/kubelet.conf ] || { echo 'ERROR: registry bootstrap configuration requires a node not yet joined' >&2; exit 1; }
client_dir=$1
for file in endpoint ca.crt client.crt client.key; do [ -r "$client_dir/$file" ] || { echo "ERROR: missing client bundle file $file" >&2; exit 1; }; done
DTS_REGISTRY=$(cat "$client_dir/endpoint")
printf '%s\n' "$DTS_REGISTRY" | awk -F: 'NF!=2 || $2!=5000 {exit 1} {n=split($1,a,"."); if(n!=4) exit 1; for(i=1;i<=4;i++) if(a[i] !~ /^[0-9]+$/ || a[i]>255) exit 1}' || { echo 'ERROR: expected registry IPv4:5000' >&2; exit 1; }
curl --fail --silent --show-error --connect-timeout 10 --max-time 30 --cacert "$client_dir/ca.crt" --cert "$client_dir/client.crt" --key "$client_dir/client.key" "https://$DTS_REGISTRY/v2/" >/dev/null
umask 077
cert_dir="/etc/containerd/certs.d/$DTS_REGISTRY"
install -d -m 0700 "$cert_dir" /etc/dts
for file in ca.crt client.crt client.key; do install -m 0600 "$client_dir/$file" "$cert_dir/$file"; done
cat > "$cert_dir/hosts.toml" <<EOF
server = "https://$DTS_REGISTRY"
[host."https://$DTS_REGISTRY"]
  capabilities = ["pull", "resolve", "push"]
  ca = "$cert_dir/ca.crt"
  client = [["$cert_dir/client.crt", "$cert_dir/client.key"]]
EOF
printf 'DTS_REGISTRY=%s\n' "$DTS_REGISTRY" > /etc/dts/registry.env
sed "s|registry.k8s.io/pause:3.10|$DTS_REGISTRY/kubernetes/pause:3.10|" cluster/containerd-config.toml > /etc/containerd/config.toml
systemctl restart containerd
echo "Node configured to pull from https://$DTS_REGISTRY using mutual TLS"
