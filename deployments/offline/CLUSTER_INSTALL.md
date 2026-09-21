# 麒麟 V10 SP3 2403 离线建群

本包面向新建 Linux x86_64 节点部署 Kubernetes 1.32.5 + containerd 2.3.5（官方静态构建，避免高版本 glibc 依赖），不使用 Docker Engine。包含 kubeadm/kubelet/kubectl 1.32.5、runc 1.5.1、crictl 1.32.0、CNI Plugins 1.9.1、Flannel 0.28.9、Registry 3、控制面/etcd/CoreDNS/pause 镜像及麒麟官方 2403 base 软件源的 239 个 RPM 依赖集合。`cluster/kylin-rpms.json`、`cluster/upstream-tools.json`、`cluster/image-provenance.json` 记录来源和校验信息，`SHA256SUMS` 覆盖全部文件。镜像通过 m.daocloud.io 加速源下载并验证归档平台、标签、文件系统摘要；GitHub 二进制经下载代理取得并与官方 SHA256 校验值核对。

## 主机准备

推荐入口是 Shell 调 Ansible。单节点和多节点分别使用包内示例：

```bash
## 单节点：在同一台机器运行控制面、Registry 和 DTS
cp automation/inventory.single-node.example.ini /root/dts-hosts.ini

## 多节点：一个控制面/Registry，加一个或多个 Worker
# cp automation/inventory.multi-node.example.ini /root/dts-hosts.ini

# 编辑包外 /root/dts-hosts.ini，填写实际节点 IP，配置 SSH 密钥和 known_hosts
bash deploy.sh /root/dts-hosts.ini
```

编排机同样使用麒麟 V10 SP3 2403 x86_64，Shell 会从包内仓库安装麒麟提供的 Ansible 2.8.8、rsync 和依赖。远端基础系统需已有 Python 3 与 SSH 服务，默认使用 root SSH 密钥，不关闭主机密钥校验。每个 Kubernetes 节点都接收唯一的 `images/all-images.tar.gz`，执行 `unpack-images.sh` 检查 gzip 流和 Docker archive 结构，并强制重建 `all-images.tar`，避免复用旧版本残留的原始 tar。此 tar 含全部 13 个镜像及 DTS 自编译镜像，支持 containerd 导入；运行组件通过私有仓库拉取。

Ansible 先通过 SSH copy 引导离线安装 rsync 和 grubby，再用 synchronize 分发安装目录和单个镜像压缩包。远端安装目录由 inventory 的 `dts_remote_install_dir` 指定，默认 `/opt/dts-offline`；必须使用独立的绝对路径，不能设置为 `/`、`/opt`、`/var` 等系统目录。镜像解压需要额外磁盘空间。仓库客户端私钥经受保护的临时目录分发，任务隐藏密钥输出，控制端临时文件在脚本退出时清理。

单节点 inventory 的 `workers` 组必须为空，控制面和 Registry 使用同一个 inventory 主机。自动化会移除控制面调度污点、把 Server/Worker/Web/HPA 默认副本设为 1，并在 `dts_single_node_storage_dir`（默认 `/var/lib/dts-single-node`）创建 PostgreSQL RWO 和 CDC spool RWX 静态本地卷。该模式没有节点级高可用，只适合验证、演示或明确接受单机故障域的小规模环境。多节点 inventory 必须至少配置一个独立 Worker；控制面不能同时放进 workers，业务副本可通过 `dts_server_replicas`、`dts_worker_replicas`、`dts_web_replicas` 和 `dts_hpa_max_replicas` 调整。

多节点若暂未配置应用 RWO/RWX 存储，将 inventory 的 `dts_install_application=false`，先完成集群和仓库；准备存储后再安装 DTS。单节点示例默认使用本机静态卷，不需要外部 StorageClass。默认 true 会继续安装应用，存储缺失时安装会明确失败。自动化使用运行状态文件避免重复初始化已有集群，不是自动升级/重置工具。下面保留分步手动执行方式，便于定位故障。

适用于全新专用节点。已有集群继续使用应用安装入口，不要运行本脚本初始化已有节点。操作系统需已完成基础安装，提供 yum（不存在时回退 dnf）、rpm、systemd、tar、校验工具及可信的麒麟 RPM 公钥。依赖安装只启用包内仓库，保持 RPM 签名检查，yum/dnf 会验证版本依赖及冲突；若失败应查看报错并补齐匹配物料，禁止使用 --nodeps 或 --allowerasing 强制安装。包内未包含或升级内核、固件、基础操作系统安装镜像。

- 至少一台控制节点；单点模式不配置 Worker，多点模式配置一个或多个独立 Worker。所有主机名必须唯一，使用固定 IPv4 地址并保持时间同步。单点建议至少 4 CPU / 8 GiB 内存，多点每节点至少 2 CPU / 4 GiB，并预留镜像和数据库磁盘空间。
- 本包支持两种明确配置：麒麟 4.19 使用完整 cgroup v1 兼容模式；官方 5.10+ 内核使用统一 cgroup v2。Linux 4.19 的 cgroup v2 没有 kubeadm 必需的 cpuset 控制器，不能在该内核上使用 cgroup v2，也不能用 `--ignore-preflight-errors=SystemVerification` 绕过检查。
- 自动化入口检测到麒麟 4.19 正运行 cgroup v2 时，会用包内 grubby 将所有启动项切换为 `systemd.unified_cgroup_hierarchy=0 systemd.legacy_systemd_cgroup_controller`，然后自动重启节点并验证 cgroup v1 cpuset。请在批准的维护窗口执行，确保带外管理和启动盘备份可用。卸载流程会恢复安装前记录的 cgroup 启动参数并再次重启。
- Kubernetes 1.32 原生支持 cgroup v1，本包不再使用 Kubernetes 1.35+ 才引入的 `failCgroupV1` 门禁字段或 `SystemVerification` 跳过参数；脚本仍会自行确认 cpu、cpuacct、cpuset、devices、freezer、memory、pids、blkio 全部启用并挂载，并让 kubelet 与 containerd 都使用 systemd cgroup 驱动。Kubernetes 1.32 已结束上游安全维护，使用 1.32.5 是针对客户指定版本与麒麟 4.19 的兼容交付，后续应规划升级。
- 关闭 swap 并按主机运维规范处理持久配置。当前安装模板要求 SELinux 为 Permissive 或 Disabled，脚本不会自动关闭 SELinux、防火墙或修改启动器。
- 节点之间及管理网络按角色允许 TCP 6443、10250，控制节点间 TCP 2379-2380（多控制面时），节点之间 UDP 8472（Flannel VXLAN）；Pod 能访问 DNS UDP/TCP 53、数据库及存储服务端口。NodePort 如需使用另行放行。不要向公网开放 etcd。
- 主机网络、客户数据库网段不得与 Pod `10.244.0.0/16`、Service `10.96.0.0/12` 重叠。需要更改时同时修改初始化脚本与 Flannel 配置，使用重新生成校验和的定制包。

## 每个节点准备

把完整压缩包复制到每个节点，解压后运行：

```bash
sha256sum -c SHA256SUMS
sudo sh configure-kylin-419-cgroup.sh
# 若输出 REBOOT_REQUIRED，则先执行 sudo reboot，重新登录后继续
sudo sh install-cluster.sh prepare
```

脚本安装包内 RPM、运行时、systemd 服务、网络二进制，加载 overlay/br_netfilter、配置 IPv4 转发并启动 containerd。不会访问公网，也不会初始化集群。运行 prepare 会写入系统路径、安装 RPM 并启动服务，应只用于本次新建节点。

自动化入口会完成上述 cgroup 切换和重启。手动入口可在重启后检查主机：

```bash
sudo sh install-cluster.sh preflight
uname -r
stat -fc %T /sys/fs/cgroup/
cat /proc/cgroups | grep cpuset
```

自动化入口会为每个节点在 `/etc/hosts` 写入受标记的 `节点IP 主机名 inventory名` 映射，避免 kubeadm 因内部 DNS 中不存在主机名而告警；这不替代客户业务域名和集群入口域名的 DNS 规划。

Ansible 会加载 `overlay` 和 `br_netfilter`，把包内 `cluster/99-dts-kubernetes.conf` 安装为 `/etc/sysctl.d/99-dts-kubernetes.conf`，随后执行 `sysctl -p /etc/sysctl.d/99-dts-kubernetes.conf`。固定参数为 `net.ipv4.ip_forward=1`、`net.bridge.bridge-nf-call-iptables=1`、`net.bridge.bridge-nf-call-ip6tables=1`；`prepare`、`init`、`join` 还会再次应用并核验实际值，防止 NetworkManager、安全基线或旧部署状态将转发参数改回 0。当前集群是 IPv4 单栈，因此不强制打开 IPv6 forwarding；conntrack 上限、端口范围和内存 overcommit 属于按容量评估的调优项，不在通用安装器中修改。

## 先建立私有镜像仓库

选择固定地址的仓库节点（可与首台控制节点共用），准备好足够的 `/var/lib/dts-registry` 磁盘空间。以下示例使用 `192.168.10.10:5000`，替换为实际 IP：

```bash
sudo sh install-registry.sh 192.168.10.10
```

这一步仅在仓库节点执行：从包内导入 Registry 引导镜像，用 systemd + containerd 启动 HTTPS/mTLS 仓库，然后把包内全部 13 个镜像推入仓库（包含仓库镜像本身）。自动化每次运行都会对全部标签重新对账，不会因旧 `.seeded` 标记跳过新版本镜像；已有集群也可补灌镜像而不重建证书。运行时由 CRI 验证 `kubernetes/pause:3.10` 能被实际拉取后才允许初始化。仓库绑定 TCP 5000，请允许各节点访问。镜像目录持久化于 `/var/lib/dts-registry`，证书位于 `/etc/dts-registry`，应纳入备份；仓库当前为单实例，节点故障时已有 Pod 可继续运行，但新拉取会失败。

将仓库节点的 `/etc/dts-registry/client/` 目录通过安全通道复制到每个其他节点的 `/root/dts-registry-client/`，保留私钥文件 0600 权限；只复制 client 子目录，不能复制 CA 私钥或服务器私钥。然后在其他节点执行：

```bash
sudo sh configure-registry.sh /root/dts-registry-client
```

客户端包含 CA、公钥证书、客户端私钥和仓库地址，授予镜像读写能力，不能放入公开安装包。仓库节点已自动配置，无需重复执行。脚本验证 mTLS 连接并配置 containerd 的 certs.d、私有 pause 镜像和 `/etc/dts/registry.env`。所有后续节点都需先 prepare，再配置证书；无需在工作节点逐个导入镜像。包内静态 `dts-certgen` 使用 Go `crypto/x509` 生成并自校验 RSA/SHA-256 PEM 证书，避免麒麟国密 OpenSSL 默认值与 Registry 的 Go X.509 解析器不兼容。证书有效期 825 天，到期前应安排续签并更新各节点；已完成的仓库保留证书，未完成的失败引导在重试时会重新生成证书。

镜像路径：`仓库/kubernetes/*` 用于控制面、etcd、CoreDNS、pause；`仓库/flannel/*` 用于网络插件；`仓库/dts/*` 用于业务；`仓库/library/registry:3` 用于仓库镜像存档。工作节点与业务组件均从私有仓库拉取；只有仓库首次启动需要从包内种子归档导入自身镜像。

## 初始化第一台控制节点

用控制节点自身实际 IP 替换示例：

```bash
sudo sh install-cluster.sh init 192.168.10.10
export KUBECONFIG=/etc/kubernetes/admin.conf
kubectl get nodes -o wide
kubeadm token create --print-join-command
```

此快捷入口建立单控制面、多工作节点集群，控制面本身不具备高可用。生产多控制面需预先规划负载均衡地址，并自行使用经过审核的 kubeadm HA 配置初始化，当前脚本不会部署负载均衡器。

## 加入工作节点

先执行 prepare，再把上一条命令产生的控制面地址、token 和 CA hash 写入包外 `/root/dts-join.yaml`（权限 0600）：

```yaml
apiVersion: kubeadm.k8s.io/v1beta4
kind: JoinConfiguration
discovery:
  bootstrapToken:
    apiServerEndpoint: "192.168.10.10:6443"
    token: "替换成实际TOKEN"
    caCertHashes:
    - "sha256:替换成实际CA哈希"
nodeRegistration:
  criSocket: "unix:///run/containerd/containerd.sock"
  imagePullPolicy: IfNotPresent
```

```bash
sudo sh install-cluster.sh join /root/dts-join.yaml
```

token 是集群加入凭据，不应放入可分发安装包；过期后在控制节点重新生成。完成后在控制节点检查所有节点 Ready、kube-system 和 kube-flannel 下 Pod 正常。

## 安装 DTS

```bash
export KUBECONFIG=/etc/kubernetes/admin.conf
kubectl get nodes
kubectl get pods -A -o wide
sh install-kubernetes.sh
```

应用仍需要存储：内置 PostgreSQL 需要默认 RWO StorageClass（或管理员预建匹配 PV）；多副本 CDC Spool 需要 RWX StorageClass（或预建匹配 PV）。本包提供 nfs-utils 客户端依赖，但不会自动创建客户共享存储或部署 CSI。Ingress Controller 和 metrics-server 也不在此建群模板中：无 Ingress 可使用端口转发/NodePort；未安装 metrics-server 时 HPA 不会自动扩容，但可配置固定 Worker 副本。

## 验证与故障处理

```bash
systemctl status containerd kubelet
crictl --runtime-endpoint unix:///run/containerd/containerd.sock info
ctr --namespace k8s.io images list
kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods -A -o wide
journalctl -u kubelet -n 100 --no-pager
```

脚本检查 cgroup、swap、SELinux 和已有节点配置；不会强行覆盖已有集群或运行 kubeadm reset。若 init 中途失败，保留日志与已有状态进行定位，不要直接删除 etcd/证书目录。此包已做文件及构建验证，交付环境缺少麒麟虚拟机，尚未完成该系统上的离线端到端建群验收。

`init` 和 `join` 每次都会按 `runtime/versions.env` 的 SHA256 将 `/usr/local/bin/kubectl`、`kubelet`、`kubeadm` 收敛到包内 1.32.5，即使旧节点已有 `prepared` 标记也不会继续使用更高版本 kubeadm。日志必须出现 `Kubernetes tools ready: kubeadm v1.32.5`；若版本或哈希不一致，脚本会在 kubeadm 修改集群前停止。

## 整套环境卸载

先备份需要保留的数据，然后在原 Ansible 编排机使用安装时的 inventory 执行：

```bash
sudo bash uninstall-all.sh /root/dts-hosts.ini --yes-destroy-all
```

命令会删除 DTS namespace、PostgreSQL、PVC/PV API 对象、私有 Registry 数据、kubeadm 控制面和工作节点状态、containerd 镜像与运行数据、CNI、证书、服务，以及 inventory 中 `dts_remote_install_dir` 指定的整个安装目录。单点本地存储模式还会清空并删除 `dts_single_node_storage_dir`，其中的 PostgreSQL 和 CDC spool 数据不可恢复。脚本会拒绝系统根目录或其他高风险目录，并且仅接受带 `/var/lib/dts-cluster-bootstrap/owned` 标记的节点，防止误删其他 Kubernetes 集群。RPM 系统依赖不会自动卸载，避免删除其他软件正在使用的基础包。

卸载顺序为：先用 kubectl 删除 DTS API/存储对象；停止 kubelet并将 kubeadm 临时复制到 `/run`；先删除正式安装路径中的 kubectl、kubelet、kubeadm；使用临时 kubeadm 完成 Worker 和控制面的 reset；最后停止 containerd并删除镜像、Kubernetes/CNI/containerd 数据和临时 reset 工具。这样既满足工具先卸载，也不会因为提前删除 kubeadm 而失去安全 reset 能力。

Kubernetes 官方说明 `kubeadm reset` 不清理 kube-proxy 创建的 iptables、nftables 或 IPVS 规则；本脚本也不全局 flush 防火墙，以免破坏宿主机其他业务。外部 NFS/CSI 存储使用 `Retain` 策略时，物理数据必须在存储后端另行销毁。确认卸载完成后，可手工删除编排机保留的安装压缩包和 inventory。
