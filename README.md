# DTS

DTS 是一个以 Go 实现的统一数据库迁移平台，当前代码版本为 `0.15.0-rc49`。系统由控制面 API、分布式 Worker、Vue 管理控制台、PostgreSQL 元数据仓库以及 DTS 自研的全量/CDC 数据面组成。

> 当前仓库仍处于 RC 阶段。新部署的默认管理员为 `admin`，初始密码为 `Cljslrl0620!`；首次登录后必须立即修改。数据库密码、Master Key、Worker Token 和 Auth Secret 仍必须使用互不相同的随机值。

## 文档入口

- [项目架构](docs/PROJECT_ARCHITECTURE.md)
- [使用手册](docs/USER_GUIDE.md)
- [维护手册](docs/MAINTENANCE_GUIDE.md)
- [架构评估与缺失功能](docs/ARCHITECTURE_ASSESSMENT.md)

## Kubernetes 部署

运行架构为 Kubernetes → containerd → DTS Pods。Docker 只在联网构建机用于镜像构建、拉取和导出。目标节点不需要 Docker Engine 或 Compose。

离线包部署到现有 Linux amd64 Kubernetes 集群；集群需要 CNI 和 RWX Spool 存储，内置 PostgreSQL 模式还需要 RWO 存储。生产多节点可连接外部 HA PostgreSQL。

1. 将离线包解压到每个可调度节点，执行 `sudo sh load-images-kubernetes.sh`。
2. 在配置好 kubeconfig 的控制机执行 `sh install.sh`（等同于 `sh install-kubernetes.sh`）。
3. 执行 `./runtime/kubectl -n dts port-forward svc/web 8088:80`，访问 `http://127.0.0.1:8088`。

安装参数、Ingress、扩缩容、状态检查和卸载见[离线部署手册](deployments/offline/README.md)。

## Linux 离线安装包

联网构建机执行：

```bash
sh deployments/offline/build-offline-package.sh
```

Windows 可执行 `deployments/offline/build-offline-package.ps1`，无需本机 Docker daemon。成品为 `dist/dts-kubernetes-offline-<版本>-linux-amd64.tar.gz`，包含 Server/Worker/全部 CDC Runtime、Web、PostgreSQL 镜像、kubectl 和部署资料。安装过程不下载镜像或依赖。Kubernetes 控制面、containerd、CNI、CSI 由现有集群提供。
