# QMigration

QMigration 是一个以 Go 实现的统一数据库迁移平台，当前代码版本为 `0.15.0-rc49`。系统由控制面 API、分布式 Worker、Vue 管理控制台、PostgreSQL 元数据仓库以及 QMigration 自研的全量/CDC 数据面组成。

> 当前仓库仍处于 RC 阶段。新部署的默认管理员为 `admin`，初始密码为 `Cljslrl0620!`；首次登录后必须立即修改。数据库密码、Master Key、Worker Token 和 Auth Secret 仍必须使用互不相同的随机值。

## 文档入口

- [项目架构](docs/PROJECT_ARCHITECTURE.md)
- [使用手册](docs/USER_GUIDE.md)
- [维护手册](docs/MAINTENANCE_GUIDE.md)
- [架构评估与缺失功能](docs/ARCHITECTURE_ASSESSMENT.md)

## 快速体验

Linux/macOS 或支持 Linux 容器的环境中执行：

```bash
cp deployments/.env.example deployments/.env.local
# 编辑 deployments/.env.local，为每个空值填入独立的强随机值
docker compose --env-file deployments/.env.local -f deployments/docker-compose.yml up --build
```

启动后访问：

- Web：`http://127.0.0.1:8088`
- API：`http://127.0.0.1:8080`
- 健康检查：`http://127.0.0.1:8080/healthz`
- 就绪检查：`http://127.0.0.1:8080/readyz`
- Prometheus 指标：`http://127.0.0.1:8080/metrics`

安全配置、首次登录、任务创建和生产部署要求请阅读[使用手册](docs/USER_GUIDE.md)。

## Linux 离线安装包

仓库提供包含 Server、Worker、全部 CDC Runtime、Web 和 PostgreSQL 镜像的 Linux x86_64 离线包。联网构建机执行：

```bash
sh deployments/offline/build-offline-package.sh
```

Windows 构建机可执行 `deployments/offline/build-offline-package.ps1`。成品写入 `dist/`；目标 Linux 主机解压后可运行 `sudo sh install.sh` 使用 Compose，或按 `deployments/offline/README.md` 导入各 Kubernetes 节点后运行 `sh install-kubernetes.sh`。安装期间不会拉取镜像或下载依赖。包内包含固定版本的 Docker Engine、Docker Compose v2 和 kubectl；Kubernetes 模式支持现有多节点集群、跨节点副本分散、外部 HA PostgreSQL、LoadBalancer 和 Ingress。
