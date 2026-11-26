# DTS

DTS 是一个以 Go 实现的统一数据库迁移平台，当前代码版本为 `0.15.0-rc50`。系统由控制面 API、分布式 Worker、Vue 管理控制台、PostgreSQL 元数据仓库以及 DTS 自研的全量/CDC 数据面组成。

> 当前仓库仍处于 RC 阶段。离线安装器为新部署的 `admin` 生成随机密码，保存到安装目录的 `INITIAL_ADMIN_CREDENTIALS.txt`（权限 600）。手工部署须设置独立随机的管理员密码（至少 16 字符）。已有账号不会被升级重置；历史部署应主动更换原默认密码。数据库密码、Master Key、Worker Token 和 Auth Secret 必须使用互不相同的随机值。

## 文档入口

- [项目架构](docs/PROJECT_ARCHITECTURE.md)
- [RC50 数据库支持范围和实测版本](docs/SUPPORT_MATRIX_V0.15_RC50.md)
- [RC50 四项企业门禁实施及验收边界](docs/ENTERPRISE_HARDENING_RC50.md)（停写证明、执行代次/目标围栏、IAM/Saga、隔离真库验收入口）
- [使用手册](docs/USER_GUIDE.md)
- [维护手册](docs/MAINTENANCE_GUIDE.md)
- [架构评估与缺失功能](docs/ARCHITECTURE_ASSESSMENT.md)
- [企业就绪度复查与后续整改](docs/ENTERPRISE_READINESS.md)（2026-10-30，含 CDC 并发与恢复路径续查）
- [代码审核、测试与整改报告](docs/CODE_REVIEW_2026-10-30.md)（本轮协议恢复、认证/RBAC、请求契约与前端整改）

## 单机 Docker Compose 部署

Linux amd64 单机包内提供 Docker Engine、Compose v2、PostgreSQL、Server、Worker 和 Web 镜像。解压并在包目录执行 `sudo sh install.sh`，完成后执行 `sh verify.sh`。卸载及数据保留选项见[单机离线包手册](deployments/compose-offline/README.md)。

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
