# QMigration

QMigration 是一个以 Go 实现的统一数据库迁移平台，当前代码版本为 `0.15.0-rc49`。系统由控制面 API、分布式 Worker、Vue 管理控制台、PostgreSQL 元数据仓库以及 QMigration 自研的全量/CDC 数据面组成。

> 当前仓库仍处于 RC 阶段。Compose 已启用生产安全校验，不提供默认密码；运行前必须生成独立 Secret。

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
