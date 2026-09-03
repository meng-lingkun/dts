# QMigration Linux 离线安装包

目标平台：Linux x86_64。安装过程不访问网络。

包内包含：

- QMigration Server/Worker/全部 CDC Runtime 镜像；
- QMigration Web 镜像；
- PostgreSQL 17 镜像；
- Docker Engine 29.7.2 Linux 静态运行时及 Docker Compose v2.40.2 插件；
- kubectl v1.37.0、Compose/Kubernetes 部署清单、自动 Secret 初始化、校验和与镜像清单；
- `qmigrationctl` Linux CLI、元数据 Migration 和运维文档。

宿主机无需预装 Docker。安装脚本在 Docker 不可用时安装包内运行时，并创建 systemd 服务；已有且可用的 Docker/Compose 会原样保留。目标系统仍须提供 Linux x86_64 内核、systemd、cgroup、iptables、`tar` 和 `sha256sum`，这些属于操作系统能力，无法由通用应用包替换。

### Compose 单机安装

```bash
tar -xzf qmigration-offline-*.tar.gz
cd qmigration-offline-*
sudo sh install.sh
sudo cat INITIAL_ADMIN_CREDENTIALS.txt
```

校验运行状态：`sudo sh verify.sh`。

停止并保留数据：`sudo sh uninstall.sh`。只有确认要删除 PostgreSQL 和 Spool 数据时才运行 `sudo sh uninstall.sh --purge`。

自动安装的 Docker 二进制位于 `/usr/local/bin`，Compose 插件位于 `/usr/local/lib/docker/cli-plugins`。安装器不会自动把普通用户加入具有 root 等价权限的 `docker` 组，也不会在卸载 QMigration 时删除共享容器运行时。

新部署的默认账号为 `admin`，默认密码为 `Cljslrl0620!`。首次登录后请立即在“用户与权限”中重置管理员密码，然后从 `.env` 删除 `QMIGRATION_BOOTSTRAP_ADMIN_PASSWORD`。`.env`、初始凭据和 Master Key 必须备份到安全的 Secret 管理系统。

### Kubernetes 离线安装

Kubernetes 模式部署到**现有 Linux amd64 多节点集群**，不会安装或修改控制面、CNI、CSI 或 Ingress Controller。内置 PostgreSQL 模式要求默认 RWO StorageClass；多 Server 的 CDC Spool 要求 RWX StorageClass。若没有默认 RWX 存储，应先在 `kubernetes/qmigration.yaml` 中指定可用的 `storageClassName`。

先把解压后的完整目录复制到每个可调度节点，并在每个节点导入镜像：

```bash
cd qmigration-offline-*
sudo sh load-images-kubernetes.sh
```

脚本支持 K3s、MicroK8s、containerd、nerdctl 和 Docker 运行时。所有节点完成后，在能访问集群 kubeconfig 的控制机执行；临时 Image Preflight DaemonSet 会再次确认所有可调度节点都能启动包内镜像：

```bash
cd qmigration-offline-*
sh install-kubernetes.sh
./runtime/kubectl -n qmigration port-forward svc/web 8088:80
```

访问 `http://127.0.0.1:8088`。安装器会生成独立随机内部 Secret，只在首次安装时使用管理员默认密码，并在重复执行时保留已有 Secret。若集群版本与包内 kubectl 不在一个次版本范围内，可通过 `KUBECTL=/已有兼容版本/kubectl sh install-kubernetes.sh` 覆盖客户端。Worker HPA 需要集群已有 Metrics Server；缺少时不会阻止基础 Pod 启动，但不会自动扩缩容。

多节点副本和入口可通过环境变量调整：

```bash
QMIGRATION_SERVER_REPLICAS=3 \
QMIGRATION_WORKER_REPLICAS=6 \
QMIGRATION_WEB_REPLICAS=3 \
QMIGRATION_HPA_MAX_REPLICAS=60 \
QMIGRATION_RWX_STORAGE_CLASS=cephfs-rwx \
QMIGRATION_SPOOL_STORAGE_SIZE=500Gi \
QMIGRATION_WEB_SERVICE_TYPE=LoadBalancer \
sh install-kubernetes.sh
```

若已有 Ingress Controller，可设置 `QMIGRATION_INGRESS_HOST`、`QMIGRATION_INGRESS_CLASS` 和 `QMIGRATION_INGRESS_TLS_SECRET`。生产元数据建议连接外部 HA PostgreSQL：

```bash
QMIGRATION_EXTERNAL_POSTGRES_HOST=postgres-ha.database.svc \
QMIGRATION_EXTERNAL_POSTGRES_USER=qmigration \
QMIGRATION_EXTERNAL_POSTGRES_DATABASE=qmigration \
QMIGRATION_METADATA_PASSWORD='数据库密码' \
QMIGRATION_INGRESS_HOST=qmigration.example.com \
QMIGRATION_INGRESS_TLS_SECRET=qmigration-tls \
sh install-kubernetes.sh
```

未设置外部 PostgreSQL 时，安装器部署包内 `postgres.yaml` 单实例数据库，适合测试和小规模环境，但不提供元数据层高可用。
