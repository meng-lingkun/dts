#!/bin/sh
# Seed the private registry before the Kubernetes API exists.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin
[ "$(id -u)" = 0 ] || { echo 'ERROR: root required' >&2; exit 1; }
[ "$#" = 1 ] || { echo 'Usage: sh install-registry.sh <registry-node-ipv4>' >&2; exit 1; }
registry_ip=$1
printf '%s\n' "$registry_ip" | awk -F. 'NF!=4 {exit 1} {for(i=1;i<=4;i++) if($i !~ /^[0-9]+$/ || $i>255) exit 1}' || { echo 'ERROR: invalid IPv4 address' >&2; exit 1; }
sha256sum -c SHA256SUMS >/dev/null
[ -f /var/lib/dts-cluster-bootstrap/prepared ] || { echo 'ERROR: run install-cluster.sh prepare first' >&2; exit 1; }
DTS_REGISTRY="$registry_ip:5000"
. ./cluster/registry-image-map.sh
registry_health() {
  curl --noproxy '*' --fail --silent --show-error --connect-timeout 2 --max-time 5 \
    --cacert /etc/dts-registry/client/ca.crt \
    --cert /etc/dts-registry/client/client.crt \
    --key /etc/dts-registry/client/client.key \
    "https://$DTS_REGISTRY/v2/" >/dev/null
}
registry_diagnostics() {
  echo '--- dts-registry systemd status ---' >&2
  systemctl status dts-registry --no-pager --full >&2 || true
  echo '--- dts-registry journal (last 120 lines) ---' >&2
  journalctl -u dts-registry -b -n 120 --no-pager >&2 || true
  echo '--- containerd registry objects ---' >&2
  ctr --namespace dts-registry containers list >&2 || true
  ctr --namespace dts-registry tasks list >&2 || true
  echo '--- TCP port 5000 listeners ---' >&2
  ss -lntp 2>&1 | awk 'NR == 1 || $4 ~ /:5000$/' >&2 || true
  echo '--- certificate checks ---' >&2
  openssl verify -CAfile /etc/dts-registry/ca.crt /etc/dts-registry/server.crt /etc/dts-registry/client/client.crt >&2 || true
  echo '--- direct registry HTTPS check (proxy bypassed) ---' >&2
  curl --noproxy '*' --verbose --connect-timeout 2 --max-time 5 \
    --cacert /etc/dts-registry/client/ca.crt \
    --cert /etc/dts-registry/client/client.crt \
    --key /etc/dts-registry/client/client.key \
    "https://$DTS_REGISTRY/v2/" >/dev/null || true
}
wait_for_registry() {
  label=$1
  attempt=0
  while [ "$attempt" -lt 30 ]; do
    if systemctl is-active --quiet dts-registry && registry_health; then return 0; fi
    attempt=$((attempt+1))
    sleep 2
  done
  echo "ERROR: $label" >&2
  registry_diagnostics
  return 1
}
umask 077
if [ -d /etc/dts-registry ] && [ ! -e /var/lib/dts-registry/.seeded ] && [ ! -e /etc/kubernetes/kubelet.conf ]; then
  if [ ! -r /etc/dts-registry/client/endpoint ]; then
    echo 'ERROR: existing /etc/dts-registry is not a completed or recognizable DTS bootstrap; inspect it manually' >&2
    exit 1
  fi
  [ "$(cat /etc/dts-registry/client/endpoint)" = "$DTS_REGISTRY" ] || { echo 'ERROR: existing registry uses a different address' >&2; exit 1; }
  # A failed bootstrap has not distributed a usable client identity yet. Rotate
  # the incomplete certificate set so Kylin-specific OpenSSL defaults cannot be
  # retained across a retry.
  systemctl disable --now dts-registry >/dev/null 2>&1 || true
  rm -f /etc/dts-registry/ca.crt /etc/dts-registry/ca.key /etc/dts-registry/ca.srl \
    /etc/dts-registry/server.crt /etc/dts-registry/server.key /etc/dts-registry/server.csr /etc/dts-registry/server.ext \
    /etc/dts-registry/client/client.crt /etc/dts-registry/client/client.key /etc/dts-registry/client.csr /etc/dts-registry/client.ext
fi
if [ ! -d /etc/dts-registry ]; then
  [ ! -e /etc/systemd/system/dts-registry.service ] && [ ! -d /var/lib/dts-registry ] || { echo 'ERROR: existing registry state; inspect it before bootstrap' >&2; exit 1; }
  install -d -m 0700 /etc/dts-registry/client
  install -d -m 0755 /var/lib/dts-registry
fi
install -d -m 0700 /etc/dts-registry/client
install -d -m 0755 /var/lib/dts-registry
certificate_bundle_complete=true
for file in ca.crt ca.key server.crt server.key client/ca.crt client/client.crt client/client.key client/endpoint; do
  [ -s "/etc/dts-registry/$file" ] || certificate_bundle_complete=false
done
if [ "$certificate_bundle_complete" = false ]; then
  [ ! -e /etc/kubernetes/kubelet.conf ] || {
    echo 'ERROR: initialized registry node has an incomplete certificate bundle; restore it from backup instead of rotating live credentials' >&2
    exit 1
  }
  # Generate with Go crypto/x509, the same parser used by Registry. This avoids
  # Kylin OpenSSL/GM configuration changing the certificate encoding or OIDs.
  chmod 0755 bin/dts-certgen
  bin/dts-certgen --output /etc/dts-registry --ip "$registry_ip"
fi
for certificate in /etc/dts-registry/ca.crt /etc/dts-registry/server.crt /etc/dts-registry/client/client.crt; do
  grep -q '^-----BEGIN CERTIFICATE-----$' "$certificate" || { echo "ERROR: certificate is not PEM: $certificate" >&2; exit 1; }
  openssl x509 -in "$certificate" -noout -text | grep -qi 'Signature Algorithm: sha256WithRSAEncryption' || { echo "ERROR: certificate is not RSA/SHA-256: $certificate" >&2; exit 1; }
done
openssl verify -CAfile /etc/dts-registry/ca.crt /etc/dts-registry/server.crt /etc/dts-registry/client/client.crt
[ "$(cat /etc/dts-registry/client/endpoint)" = "$DTS_REGISTRY" ] || { echo 'ERROR: existing registry uses a different address' >&2; exit 1; }
install -d -m 0700 /etc/dts-registry/server
for file in ca.crt server.crt server.key; do install -m 0600 "/etc/dts-registry/$file" "/etc/dts-registry/server/$file"; done
[ -s /etc/dts-registry/http.secret ] || openssl rand -hex 32 > /etc/dts-registry/http.secret
registry_http_secret=$(cat /etc/dts-registry/http.secret)
cat > /etc/dts-registry/server/config.yml <<EOF
version: 0.1
log:
  level: info
storage:
  filesystem:
    rootdirectory: /var/lib/registry
http:
  addr: :5000
  secret: "$registry_http_secret"
  tls:
    certificate: /etc/distribution/server.crt
    key: /etc/distribution/server.key
    clientauth: require-and-verify-client-cert
    clientcas:
    - /etc/distribution/ca.crt
EOF
sh unpack-images.sh
ctr --namespace dts-registry images import images/all-images.tar
install -m 0644 cluster/dts-registry.service /etc/systemd/system/dts-registry.service
systemctl daemon-reload
systemctl enable dts-registry
if ! systemctl restart dts-registry; then
  echo 'ERROR: systemd could not start dts-registry' >&2
  registry_diagnostics
  exit 1
fi
wait_for_registry 'registry did not become ready'
if [ ! -e /etc/kubernetes/kubelet.conf ]; then
  sh configure-registry.sh /etc/dts-registry/client
  systemctl restart dts-registry
  wait_for_registry 'registry failed after runtime configuration'
else
  # An existing cluster may need new image tags after an offline package update.
  # Keep its runtime/certificates intact and reconcile the registry contents only.
  [ -r /etc/dts/registry.env ] || {
    echo 'ERROR: initialized registry node has no /etc/dts/registry.env; restore its client configuration before reseeding' >&2
    exit 1
  }
  [ "$(sed -n 's/^DTS_REGISTRY=//p' /etc/dts/registry.env)" = "$DTS_REGISTRY" ] || {
    echo 'ERROR: initialized node is configured for a different registry endpoint' >&2
    exit 1
  }
fi
# Imports are only used to seed this registry host. Worker nodes pull images.
ctr --namespace dts-seed images import images/all-images.tar
version=$(cat VERSION)
image_refs=$(cat cluster/cluster-images.txt; printf '\ndts/server:%s\ndts/web:%s\ndts/postgres:17\n' "$version" "$version")
for ref in $image_refs; do
  target=$(registry_image "$ref")
  source=$ref
  case "$source" in dts/*) source="docker.io/$source" ;; esac
  ctr --namespace dts-seed images tag --force "$source" "$target"
  ctr --namespace dts-seed images push --hosts-dir /etc/containerd/certs.d --platform linux/amd64 "$target"
done
# Verify the kubelet CRI path, not just ctr push, before allowing init.
crictl --runtime-endpoint unix:///run/containerd/containerd.sock pull "$DTS_REGISTRY/kubernetes/pause:3.10"
{
  printf 'registry=%s\n' "$DTS_REGISTRY"
  printf 'version=%s\n' "$version"
  sha256sum images/all-images.tar.gz
} > /var/lib/dts-registry/.seeded
echo "Registry seeded at https://$DTS_REGISTRY. Client bundle: /etc/dts-registry/client"
echo 'Copy ONLY the client directory securely to other nodes, then run configure-registry.sh there.'
echo 'Keep ca.key/server.key on the registry host; the client key grants registry access and must remain private.'
