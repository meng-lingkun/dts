# DTS Kubernetes 离线安装包

目标平台：Linux x86_64；运行底座为 Kubernetes + containerd。Docker 仅用于构建机制作、导出镜像。

包内包含三套应用镜像（Server/Worker/全部 CDC Runtime、Web、PostgreSQL 17）、kubectl、Linux CLI、元数据 Migration、Kubernetes 清单及运维文档。不再包含 Docker Engine、Compose 或 Docker systemd 服务。

默认管理员为 `admin` / `Cljslrl0620!`，首次登录后请在“用户与权限”修改。安装器为数据库、加密及 Token 生成独立随机 Secret；重复安装保留既有 Secret。

### Kubernetes 离线安装

Kubernetes 模式部署到**现有 Linux amd64 多节点集群**，不会安装或修改控制面、CNI、CSI 或 Ingress Controller。内置 PostgreSQL 模式要求默认 RWO StorageClass；多 Server 的 CDC Spool 要求 RWX StorageClass。若没有默认 RWX 存储，请通过 `DTS_RWX_STORAGE_CLASS` 指定现有 RWX StorageClass。

先把解压后的完整目录复制到每个可调度节点，并在每个节点导入镜像：

```bash
cd dts-kubernetes-offline-*
sudo sh load-images-kubernetes.sh
```

脚本支持 K3s、MicroK8s 和标准 containerd（ctr/nerdctl）。所有节点完成后，在能访问集群 kubeconfig 的控制机执行；临时 Image Preflight DaemonSet 会再次确认所有可调度节点都能启动包内镜像：

```bash
cd dts-kubernetes-offline-*/
sh install.sh
./runtime/kubectl -n dts port-forward svc/web 8088:80
```

访问 `http://127.0.0.1:8088`。安装器会生成独立随机内部 Secret，只在首次安装时使用管理员默认密码，并在重复执行时保留已有 Secret。若集群版本与包内 kubectl 不在一个次版本范围内，可通过 `KUBECTL=/已有兼容版本/kubectl sh install-kubernetes.sh` 覆盖客户端。Worker HPA 需要集群已有 Metrics Server；缺少时不会阻止基础 Pod 启动，但不会自动扩缩容。

多节点副本和入口可通过环境变量调整：

```bash
DTS_SERVER_REPLICAS=3 \
DTS_WORKER_REPLICAS=6 \
DTS_WEB_REPLICAS=3 \
DTS_HPA_MAX_REPLICAS=60 \
DTS_RWX_STORAGE_CLASS=cephfs-rwx \
DTS_SPOOL_STORAGE_SIZE=500Gi \
DTS_WEB_SERVICE_TYPE=LoadBalancer \
sh install-kubernetes.sh
```

若已有 Ingress Controller，可设置 `DTS_INGRESS_HOST`、`DTS_INGRESS_CLASS` 和 `DTS_INGRESS_TLS_SECRET`。生产元数据建议连接外部 HA PostgreSQL：

```bash
DTS_EXTERNAL_POSTGRES_HOST=postgres-ha.database.svc \
DTS_EXTERNAL_POSTGRES_USER=dts \
DTS_EXTERNAL_POSTGRES_DATABASE=dts \
DTS_METADATA_PASSWORD='数据库密码' \
DTS_INGRESS_HOST=dts.example.com \
DTS_INGRESS_TLS_SECRET=dts-tls \
sh install-kubernetes.sh
```

未设置外部 PostgreSQL 时，安装器部署包内 `postgres.yaml` 单实例数据库，适合测试和小规模环境，但不提供元数据层高可用。
### 状态检查与卸载

在 kubeconfig 所在控制机执行 `sh verify.sh`，核验包内文件、三组 Deployment、Pod 分布和 Server Readiness。

`sh uninstall.sh` 移除应用 Deployment、入口 Service/Ingress、HPA/PDB 和临时镜像预检。保留 namespace、PostgreSQL、PVC、ConfigMap 和所有 Secret。再次安装请使用相同数据库、存储和入口参数；不提供自动删除数据的 `--purge` 选项。

旧 Docker 部署升级须先备份元数据、共享 Spool 和所有 Secret，再按恢复流程迁移到 Kubernetes。安装器不会自动搬迁旧容器数据或卸载宿主机已有 Docker。

### 构建与运行边界

联网构建机保留 Dockerfile、docker build/pull/save；Linux 构建脚本仅使用未启动的临时容器从镜像复制 CLI。安装、镜像导入、检查、卸载均不调用 Docker。节点镜像导入必须连接 kubelet 使用的 containerd；不能用 Docker Engine 自带的私有 containerd 替代集群运行时。
# 客户数据库域名解析

数据源 Host 支持域名。默认 Pod 使用 ClusterFirst，经集群 DNS 解析；离线环境不要求公网，但需要可达的内网 DNS。宿主机 `/etc/hosts` 不会自动同步到 Pod。Server 执行连接测试，Worker 执行迁移，两者都必须能解析数据库以及独立 CDC Agent、Kafka broker 等端点。

有企业 DNS 时，建议由集群管理员在 CoreDNS 中对客户域后缀配置条件转发，例如在现有 Corefile 增加下面的独立块（替换域名和 DNS IP，保留原有集群配置）：

```text
customer.example:53 {
    errors
    cache 30
    forward . 10.20.0.53 10.20.0.54
}
```

需允许 Pod 到集群 DNS、CoreDNS 到企业 DNS 的 UDP/TCP 53，以及 Server/Worker 到数据库端口的流量。不要仅在 ClusterFirst 的 nameservers 末尾追加企业 DNS 来实现分域转发；NXDOMAIN 不保证回退到下一个服务器。不要直接切换到只认识企业域的 DNS，否则 postgres.dts.svc 等服务名可能无法解析。

没有 DNS、只有固定映射时，将 `kubernetes/dns-hosts.patch.example.yaml` 复制到包外，例如 `/etc/dts/dns.patch.yaml`，编辑真实地址，再运行：

```bash
DTS_DNS_PATCH_FILE=/etc/dts/dns.patch.yaml sh install.sh
```

安装程序会对 Server 和 Worker 的 Pod 模板应用同一份补丁；扩容、跨节点调度和重建 Pod 都继承映射。该文件是 Deployment merge patch，不要直接 kubectl apply。升级时继续传入此参数。移除所有静态映射时，传入含 `spec.template.spec.hostAliases: []` 的补丁。修改包内校验文件会导致 SHA256 校验失败，所以客户配置应存放在包外。

对已安装集群更新映射，可在迁移暂停、任务已停止后执行：

```bash
for component in server worker; do
  kubectl -n dts patch deployment "$component" --type=merge --patch-file=/etc/dts/dns.patch.yaml --dry-run=server
done
for component in server worker; do
  kubectl -n dts patch deployment "$component" --type=merge --patch-file=/etc/dts/dns.patch.yaml
  kubectl -n dts rollout status deployment/"$component" --timeout=300s
done
```

模板变更会滚动重建 Pod。静态映射不跟随 DNS TTL 或数据库故障转移，动态 HA 域名应使用企业 DNS。数据源保留原始域名以维持 TLS 主机名校验，不要关闭 TLS 来处理解析问题。

验收必须检查每个 Server/Worker Pod：`kubectl -n dts get pods -o wide`，然后用 `kubectl -n dts exec POD -- cat /etc/resolv.conf` 和 `kubectl -n dts exec POD -- cat /etc/hosts` 核对配置。镜像包含 getent 时可用 `getent hosts 域名` 检验包括 hosts 的解析；nslookup 只查 DNS，不能验证 hostAliases。最后执行页面连接测试和实际迁移预检查；Server 测试通过不代表所有 Worker 网络都已连通。

数据库迁移域名与业务割接域名应分开：源端与目标端使用各自稳定端点，避免业务域名切换后，CDC 重连把目标库误当源库。当前割接功能不会修改客户 DNS 记录，也不会把既有数据库连接立即转移到新 IP。业务切换需停写、确认最终位点追平并校验、完成割接，再更新客户 DNS/连接配置并重建业务连接；回切同样需要协调反向同步和业务连接。
