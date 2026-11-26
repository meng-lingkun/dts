#!/bin/sh
# Offline Kubernetes bootstrap for new Kylin V10 SP3 2403 x86_64 nodes.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin
usage() { echo 'Usage: sh install-cluster.sh preflight | prepare | init <node-ip> | join <kubeadm-join-config.yaml>' >&2; exit 1; }
write_crictl_config() {
  cat > /etc/crictl.yaml <<'EOF'
runtime-endpoint: unix:///run/containerd/containerd.sock
image-endpoint: unix:///run/containerd/containerd.sock
timeout: 30
debug: false
EOF
}
reconcile_kubernetes_tools() {
  # A previous package may have left newer binaries in /usr/local/bin while the
  # Ansible prepared marker skips the expensive OS/runtime preparation task.
  # Replace tools atomically from this verified package before init or join.
  . "$ROOT/runtime/versions.env"
  for tool in kubectl kubelet kubeadm; do
    case "$tool" in
      kubectl) expected_sha256=$KUBECTL_SHA256 ;;
      kubelet) expected_sha256=$KUBELET_SHA256 ;;
      kubeadm) expected_sha256=$KUBEADM_SHA256 ;;
    esac
    source_tool="$ROOT/runtime/$tool"
    [ "$(sha256sum "$source_tool" | awk '{print $1}')" = "$expected_sha256" ] || {
      echo "ERROR: packaged $tool does not match runtime/versions.env" >&2
      exit 1
    }
    installed_sha256=
    if [ -f "/usr/local/bin/$tool" ]; then
      installed_sha256=$(sha256sum "/usr/local/bin/$tool" | awk '{print $1}')
    fi
    if [ "$installed_sha256" != "$expected_sha256" ]; then
      staged_tool="/usr/local/bin/.$tool.dts-new.$$"
      install -m 0755 "$source_tool" "$staged_tool"
      mv -f "$staged_tool" "/usr/local/bin/$tool"
      echo "Reconciled $tool to v$KUBECTL_VERSION"
    fi
  done
  installed_kubeadm_version=$(kubeadm version -o short)
  [ "$installed_kubeadm_version" = "v$KUBECTL_VERSION" ] || {
    echo "ERROR: installed kubeadm is $installed_kubeadm_version, expected v$KUBECTL_VERSION" >&2
    exit 1
  }
  echo "Kubernetes tools ready: kubeadm $installed_kubeadm_version"
}
apply_kubernetes_network_settings() {
  install -d /etc/modules-load.d /etc/sysctl.d
  printf 'overlay\nbr_netfilter\n' > /etc/modules-load.d/dts-kubernetes.conf
  modprobe overlay
  modprobe br_netfilter
  install -m 0644 "$ROOT/cluster/99-dts-kubernetes.conf" /etc/sysctl.d/99-dts-kubernetes.conf
  sysctl -p /etc/sysctl.d/99-dts-kubernetes.conf
  for setting in \
    net.ipv4.ip_forward \
    net.bridge.bridge-nf-call-iptables \
    net.bridge.bridge-nf-call-ip6tables; do
    [ "$(sysctl -n "$setting")" = 1 ] || {
      echo "ERROR: $setting did not converge to 1" >&2
      exit 1
    }
  done
  echo 'Kubernetes network sysctl settings are active.'
}
mode=${1:-}; case "$mode" in preflight|prepare|init|join) ;; *) usage ;; esac
[ "$(uname -s)" = Linux ] && [ "$(uname -m)" = x86_64 ] || { echo 'ERROR: Linux x86_64 required' >&2; exit 1; }
[ "$(id -u)" = 0 ] || { echo 'ERROR: run as root' >&2; exit 1; }
# The Ansible preflight intentionally runs before the large image archive is
# transferred. Full package integrity is still verified by deploy.sh and again
# here before every mutating prepare/init/join operation.
if [ "$mode" != preflight ]; then sha256sum -c SHA256SUMS >/dev/null; fi
. /etc/os-release
case "${ID:-}" in kylin|Kylin) ;; *) echo 'ERROR: this RPM bundle targets Kylin V10 SP3 2403' >&2; exit 1 ;; esac
case "${VERSION_ID:-}" in V10|10|10.*) ;; *) echo 'ERROR: Kylin V10 required' >&2; exit 1 ;; esac
kernel_version=$(uname -r | sed 's/[^0-9.].*$//')
kernel_major=$(printf '%s\n' "$kernel_version" | cut -d. -f1)
kernel_minor=$(printf '%s\n' "$kernel_version" | cut -d. -f2)
case "$kernel_major:$kernel_minor" in
  ''|*[!0-9:]*|*:)
    echo "ERROR: cannot parse kernel release $(uname -r)" >&2; exit 1 ;;
esac
cgroup_type=$(stat -fc %T /sys/fs/cgroup/)
if [ "$kernel_major" -eq 4 ] && [ "$kernel_minor" -eq 19 ]; then
  [ "$cgroup_type" = tmpfs ] || {
    echo 'ERROR: Kylin kernel 4.19 requires the packaged cgroup v1 compatibility switch and a reboot; run configure-kylin-419-cgroup.sh' >&2; exit 1;
  }
  for controller in cpu cpuacct cpuset devices freezer memory pids blkio; do
    awk -v name="$controller" '$1 == name && $4 == 1 {found=1} END {exit !found}' /proc/cgroups || {
      echo "ERROR: required cgroup v1 $controller controller is not enabled" >&2; exit 1;
    }
    awk -v name="$controller" '$3 == "cgroup" {count=split($4, options, ","); for (i=1; i<=count; i++) if (options[i] == name) found=1} END {exit !found}' /proc/mounts || {
      echo "ERROR: required cgroup v1 $controller controller is not mounted" >&2; exit 1;
    }
  done
  cgroup_profile='v1 compatibility'
elif [ "$kernel_major" -gt 5 ] || { [ "$kernel_major" -eq 5 ] && [ "$kernel_minor" -ge 10 ]; }; then
  [ "$cgroup_type" = cgroup2fs ] && [ -f /sys/fs/cgroup/cgroup.controllers ] || {
    echo 'ERROR: kernel 5.10+ nodes require unified cgroup v2' >&2; exit 1;
  }
  cgroup_controllers=" $(cat /sys/fs/cgroup/cgroup.controllers) "
  case "$cgroup_controllers" in
    *' cpuset '*) ;;
    *) echo 'ERROR: required cpuset controller is absent from the cgroup v2 root' >&2; exit 1 ;;
  esac
  cgroup_profile='v2'
else
  echo "ERROR: unsupported kernel $(uname -r); this package supports Kylin 4.19 with cgroup v1 or kernel 5.10+ with cgroup v2" >&2
  exit 1
fi
if awk 'NR>1 {found=1} END {exit !found}' /proc/swaps; then
  echo 'ERROR: active swap detected; disable swap before cluster installation' >&2; exit 1
fi
if command -v getenforce >/dev/null && [ "$(getenforce)" = Enforcing ]; then
  echo 'ERROR: this bootstrap profile requires SELinux Permissive/Disabled; configure host policy before proceeding' >&2; exit 1
fi
if [ "$mode" = preflight ]; then
  echo "Host preflight passed: kernel $(uname -r), cgroup $cgroup_profile cpuset available."
  exit 0
fi
state=/var/lib/dts-cluster-bootstrap
if [ "$mode" = prepare ]; then
  [ ! -e /etc/kubernetes/kubelet.conf ] || { echo 'ERROR: node already belongs to a cluster' >&2; exit 1; }
  if [ ! -e "$state/owned" ]; then
    for file in /etc/containerd/config.toml /etc/crictl.yaml /etc/systemd/system/containerd.service /etc/systemd/system/kubelet.service /usr/local/bin/containerd /usr/local/bin/kubelet /usr/local/bin/kubeadm /usr/local/bin/kubectl /usr/local/sbin/runc /usr/local/bin/crictl; do
      [ ! -e "$file" ] || { echo "ERROR: existing $file; this installer only initializes new nodes" >&2; exit 1; }
    done
    if command -v containerd >/dev/null || command -v kubelet >/dev/null || command -v kubeadm >/dev/null || systemctl is-active --quiet containerd || systemctl is-active --quiet kubelet; then
      echo 'ERROR: existing container runtime/kubelet; use the existing-cluster installer' >&2; exit 1
    fi
    [ ! -d /etc/cni/net.d ] || [ -z "$(ls -A /etc/cni/net.d)" ] || { echo 'ERROR: existing CNI configuration' >&2; exit 1; }
    [ ! -d /opt/cni/bin ] || [ -z "$(ls -A /opt/cni/bin)" ] || { echo 'ERROR: existing CNI binaries' >&2; exit 1; }
  fi
  if command -v yum >/dev/null 2>&1; then
    package_manager=yum
  elif command -v dnf >/dev/null 2>&1; then
    package_manager=dnf
  else
    echo 'ERROR: yum or dnf from the Kylin base OS is required' >&2; exit 1
  fi
  install -d /etc/yum.repos.d
  cat > /etc/yum.repos.d/dts-offline.repo <<EOF
[dts-offline]
name=DTS Kylin offline repository
baseurl=file://$ROOT/cluster/rpms
enabled=0
gpgcheck=1
repo_gpgcheck=0
EOF
  # Only the bundled repository is enabled; existing Kylin RPM signing keys
  # remain authoritative. Never use --nodeps, --allowerasing or --nogpgcheck.
  "$package_manager" -y --disablerepo='*' --enablerepo=dts-offline --setopt=install_weak_deps=False install conntrack-tools iptables iproute socat ethtool ebtables util-linux kmod libseccomp nfs-utils openssl curl grubby
  install -d "$state" /usr/local/bin /usr/local/sbin /opt/cni/bin /etc/containerd /etc/cni/net.d /etc/systemd/system /etc/modules-load.d /etc/sysctl.d
  touch "$state/owned"
  tar -xzf cluster/runtime/containerd-static-2.3.5-linux-amd64.tar.gz -C /usr/local
  install -m 0755 cluster/runtime/runc.amd64 /usr/local/sbin/runc
  tar -xzf cluster/runtime/crictl-v1.32.0-linux-amd64.tar.gz -C /usr/local/bin
  tar -xzf cluster/runtime/cni-plugins-linux-amd64-v1.9.1.tgz -C /opt/cni/bin
  reconcile_kubernetes_tools
  kubeadm version -o short
  kubelet --version
  containerd --version
  runc --version
  install -m 0644 cluster/containerd-config.toml /etc/containerd/config.toml
  install -m 0644 cluster/containerd.service /etc/systemd/system/containerd.service
  install -m 0644 cluster/kubelet.service /etc/systemd/system/kubelet.service
  write_crictl_config
  apply_kubernetes_network_settings
  systemctl daemon-reload
  systemctl enable --now containerd
  systemctl enable kubelet
  crictl --runtime-endpoint unix:///run/containerd/containerd.sock info >/dev/null
  touch "$state/prepared"
  echo 'Runtime prepared. Seed the registry on one node and configure-registry.sh on all nodes before init/join.'
  exit 0
fi
[ -f "$state/prepared" ] || { echo 'ERROR: run prepare on this node first' >&2; exit 1; }
[ -r /etc/dts/registry.env ] || { echo 'ERROR: configure the private registry first' >&2; exit 1; }
if [ "$mode" = init ]; then
  [ "$#" = 2 ] || usage
  node_ip=$2
  printf '%s\n' "$node_ip" | awk -F. 'NF!=4 {exit 1} {for(i=1;i<=4;i++) if($i !~ /^[0-9]+$/ || $i>255) exit 1}' || { echo 'ERROR: provide the control node IPv4 address' >&2; exit 1; }
else
  [ "$#" = 2 ] && [ -r "$2" ] || usage
  [ ! -e /etc/kubernetes/kubelet.conf ] || { echo 'ERROR: node already joined' >&2; exit 1; }
fi
apply_kubernetes_network_settings
reconcile_kubernetes_tools
write_crictl_config
. /etc/dts/registry.env
# Test actual CRI authentication from this node before starting Kubernetes.
crictl --runtime-endpoint unix:///run/containerd/containerd.sock pull "$DTS_REGISTRY/kubernetes/pause:3.10"
if [ "$mode" = init ]; then
  export KUBECONFIG=/etc/kubernetes/admin.conf
  if [ -e "$KUBECONFIG" ]; then
    # kubeadm writes admin.conf before starting the API. Resume only a
    # completed bootstrap on a node owned by this installer.
    [ -f "$state/owned" ] && kubectl --request-timeout=15s -n kube-system get deployment/coredns daemonset/kube-proxy >/dev/null || {
      echo 'ERROR: incomplete kubeadm control plane; inspect kubelet and the API before retrying init' >&2
      exit 1
    }
  else
    kubeadm_config="$state/kubeadm-init.yaml"
    cat > "$kubeadm_config" <<EOF
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: "$node_ip"
nodeRegistration:
  criSocket: unix:///run/containerd/containerd.sock
  imagePullPolicy: IfNotPresent
---
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
kubernetesVersion: v1.32.5
imageRepository: "$DTS_REGISTRY/kubernetes"
networking:
  podSubnet: 10.244.0.0/16
  serviceSubnet: 10.96.0.0/12
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
cgroupDriver: systemd
EOF
    chmod 0600 "$kubeadm_config"
    kubeadm config images pull --config "$kubeadm_config"
    kubeadm init --config "$kubeadm_config"
  fi
  sed "s|ghcr.io/flannel-io/|$DTS_REGISTRY/flannel/|g" cluster/network/kube-flannel.yaml | kubectl apply -f -
  kubectl -n kube-flannel rollout status daemonset/kube-flannel-ds --timeout=300s
  kubectl get nodes -o wide
  echo 'Cluster initialized. Export KUBECONFIG=/etc/kubernetes/admin.conf before DTS installation.'
  echo 'Use kubeadm token create --print-join-command to obtain worker join credentials.'
else
  # Join config remains outside the package and is never included in logs.
  kubeadm join --config "$2"
fi
