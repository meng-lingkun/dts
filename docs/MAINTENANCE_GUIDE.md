# QMigration 维护手册

适用版本：`0.15.0-rc49`

## 1. 维护原则

- 数据正确性优先于吞吐：目标提交、Spool 持久化和 Checkpoint 顺序不能被优化破坏；
- Connector 能力失败关闭：未资格验证的能力不得通过名称或协议兼容性自动放开；
- Master Key、Auth Secret、Worker Token 和报告签名密钥必须独立托管和轮换；
- 元数据与 Spool 是一个恢复集合，备份和灾备必须同时考虑；
- 变更状态机、Repository 字段或 CDC 位点语义时必须增加回归测试和升级说明。

## 2. 代码地图与维护责任

| 路径 | 维护关注点 |
|---|---|
| `backend/cmd/server` | 启动、Repository 装配、Connector 注册、TLS、优雅退出 |
| `backend/cmd/worker` | 资源采样、能力上报、并发、Chunk/CDC 执行、子进程生命周期 |
| `backend/internal/api` | API 兼容、认证授权、审计、指标、WebSocket |
| `backend/internal/migration` | 生命周期和数据正确性核心，变更风险最高 |
| `backend/internal/connector` | SPI 和数据库实现；必须保持能力门禁准确 |
| `backend/internal/cdc` | 日志协议、事务边界、位点和 ACK 语义 |
| `backend/internal/pipeline` | 有界缓冲、写后提交、限速和自适应 Batch |
| `backend/internal/repository` | 元数据一致性、Lease、加密装饰器、Spool |
| `backend/migrations` | 有序、幂等的 PostgreSQL Schema 变更 |
| `web/src` | 管理台、权限可见性、API 契约和长任务 UX |
| `deployments` | 镜像、Compose/Kubernetes、备份恢复、资格验证 |
| `docs` | 当前事实、支持边界、发布和运维证据 |

## 3. 开发与验证

### 后端

```bash
cd backend
go test ./...
go vet ./...
go build ./cmd/server
go build ./cmd/worker
go build ./cmd/qmigrationctl
```

File Spool 已按 Build Tag 拆分：Linux 使用 `statfs`，Windows 开发测试使用 `GetDiskFreeSpaceExW`。GBase 8s 文件权限和动态库 Provider 仍属于 Linux 运行能力；Windows 只接受 JSON 环境配置。正式 Release Gate 必须在目标 Linux 环境运行，Windows 测试仅作为额外兼容性检查。

### 前端

```bash
cd web
npm ci
npm run build
```

仓库已提交 `package-lock.json`，CI 使用 `npm ci`、`vue-tsc --noEmit` 和 Vite 生产构建。前端单元/E2E 测试和 Lint 配置仍待补充。

### 部署文件

发布前至少验证：

- Docker 镜像可构建并以非 root 用户运行；
- Compose 解析和健康检查；
- Kubernetes YAML、PDB、HPA、PVC 和 Secret 引用；
- Prometheus Rule 语法和指标名一致；
- `backup.sh`、`restore.sh`、`migrate-metadata.sh` 在临时数据库演练；
- Server/Worker SIGTERM 和 Lease 恢复。

## 4. 版本与发布

版本至少存在于：

- `backend/internal/version/version.go`；
- `web/package.json`；
- Docker/Kubernetes 镜像 Tag；
- `metadata_schema_state` 的最新 Migration；
- Web 页头；
- Release Notes、支持矩阵和构建验证文档。

根目录 `VERSION` 是当前发布版本源；`node scripts/check-version.mjs` 会校验 Go、npm、Kubernetes 镜像、最新 Migration、README 和 UI 注入版本。历史 Release Notes 保留各自版本号，不应作为当前部署基线。

推荐发布流程：

1. 冻结功能并更新版本；
2. 新增幂等 Migration，更新 Schema Marker；
3. 执行后端测试、Vet、Race Test 和关键数据库 E2E；
4. 执行前端 `npm ci && npm run build` 及 UI E2E；
5. 构建不可变镜像并生成 SBOM/漏洞扫描结果；
6. 在元数据副本上演练迁移、升级和回滚；
7. 更新支持矩阵、Release Notes 和本文档；
8. 灰度部署，观察 Readiness、Lease、Spool 和 Schema Version；
9. 完成灾备恢复演练后再扩大范围。

## 5. 元数据迁移

生产升级前：

```bash
QMIGRATION_METADATA_PASSWORD='...' deployments/scripts/backup.sh
QMIGRATION_METADATA_PASSWORD='...' deployments/scripts/migrate-metadata.sh
```

脚本按文件名顺序、单文件事务执行 `backend/migrations/*.sql`。迁移完成后确认：

```sql
SELECT schema_version, updated_at
FROM metadata_schema_state
WHERE id = 1;
```

应与二进制版本完全一致，否则 `/readyz` 会失败。

Server 启动时也会应用嵌入的幂等 `schema.sql`。维护时必须保证独立 Migration 集合与嵌入 Schema 的最终状态一致，避免“两套迁移事实源”漂移。

## 6. 离线包构建与验证

推荐在 Linux x86_64 联网构建机运行：

```bash
sh deployments/offline/build-offline-package.sh
```

Windows 构建机可运行 `deployments/offline/build-offline-package.ps1`；该流程使用 Go 直接组装标准 Docker image archive，不要求本机 Docker daemon。两种构建方式都会产出 `dist/qmigration-offline-<版本>-linux-amd64.tar.gz`、外层 SHA-256 文件和包内 `SHA256SUMS`。

发布前必须检查：所有镜像均为 `linux/amd64`；Server 镜像包含 Server、Worker、CLI、所有 CDC Runtime、`zstd` 和 CA 根证书；Docker Engine/Compose/kubectl 二进制与 `runtime/versions.env` 固定哈希一致；Compose/Kubernetes 只引用包内 Tag；`SHA256SUMS` 使用 LF；在隔离网络的多节点 Linux 集群完整执行逐节点镜像导入、DaemonSet Image Preflight、跨节点调度、节点排空、外部 HA PostgreSQL、登录、迁移冒烟和重启恢复。离线包包含 Docker Engine、Compose 和 kubectl，但不替代 Kubernetes 控制面、CNI、CSI、Ingress、宿主机内核、cgroup、iptables、磁盘及网络驱动的运维基线。

## 6. 备份与恢复

### 备份范围

必须备份：

- PostgreSQL 元数据；
- File Spool 共享卷，或 S3 Bucket/Prefix 及其版本/保留策略；
- `QMIGRATION_MASTER_KEY`；
- `QMIGRATION_AUTH_SECRET`；
- Worker Token 和静态 RBAC Token；
- 报告签名私钥/HSM 配置、公钥 Trust Store、TSA 信任链；
- 当前部署清单和镜像 Digest。

Master Key 不包含在 `backup.sh` 输出中，必须从 Secret Manager 单独备份。

### 恢复顺序

1. 停止 Server 和 Worker；
2. 恢复 PostgreSQL；
3. 恢复对应时点的 File/S3 Spool；
4. 注入与备份时相同的 Master Key；
5. 恢复 Auth/Worker/Signing Secret；
6. 执行 Schema Migration 并核对版本；
7. 先启动 Server，确认 `/readyz`；
8. 再启动 Worker；
9. 核对 Pending Spool、Engine Job、Lease、DLQ 和任务状态；
10. 在业务确认前不要直接执行 Cutover/Rollback。

`restore.sh` 使用 `pg_restore --clean --if-exists`，属于破坏性操作，必须设置 `QMIGRATION_RESTORE_CONFIRM=YES`，且应只对已确认的目标数据库执行。

## 7. 配置分类

### Server 与认证

- `QMIGRATION_ADDR`；
- `QMIGRATION_TLS_CERT`、`QMIGRATION_TLS_KEY`；
- `QMIGRATION_AUTH_REQUIRED`；
- `QMIGRATION_BOOTSTRAP_ADMIN_USER/PASSWORD`；
- `QMIGRATION_AUTH_SECRET`；
- `QMIGRATION_SESSION_TTL_HOURS`；
- `QMIGRATION_RBAC_TOKENS`；
- `QMIGRATION_CORS_ORIGIN`；
- `QMIGRATION_WORKER_TOKEN`。

### 元数据

- `QMIGRATION_REPOSITORY=postgres`；
- `QMIGRATION_METADATA_HOST/PORT/USER/PASSWORD/DATABASE`；
- 开发用 `QMIGRATION_STATE_FILE`；
- Maintenance Retention 相关变量。

### Worker

- `QMIGRATION_SERVER`；
- `QMIGRATION_WORKER_CONCURRENCY`；
- `QMIGRATION_CDC_CONCURRENCY`；
- `QMIGRATION_WORKER_LABELS`；
- `QMIGRATION_WORKER_SHUTDOWN_GRACE_SECONDS`；
- `QMIGRATION_BIN_DIR`；
- `QMIGRATION_PIPELINE_BUFFER_BATCHES`；
- `QMIGRATION_ADAPTIVE_BATCH`。

### CDC Spool

- `QMIGRATION_CDC_SPOOL_STORAGE=file|shared-fs|s3|metadata`；
- `QMIGRATION_CDC_SPOOL_DIR`；
- `QMIGRATION_CDC_SPOOL_MAX_TRANSACTION_BYTES`；
- `QMIGRATION_CDC_SPOOL_MAX_PENDING_BYTES`；
- `QMIGRATION_CDC_SPOOL_DISK_WARN_PCT`；
- `QMIGRATION_CDC_SPOOL_DISK_CRITICAL_PCT`；
- S3 Endpoint、Bucket、Region、Credential、TLS 和 Retry 变量。

S3 完整契约见 `docs/SPOOL_STORAGE.md`。报告归档、HSM/KMS、Ed25519 和 TSA 配置见现有 Validation Report 相关 Release/Implementation 文档和代码环境变量。

### 实验 Connector

`QMIGRATION_EXPERIMENTAL_*` 只能在完成对应真实数据库资格验证、故障注入、长稳和回滚验证后开启。Server 与 Worker 必须使用一致的实验开关，否则能力上报与执行会不匹配。

## 8. 可观测性

### 健康检查

- `/healthz`：进程存活和版本；
- `/readyz`：Repository、Schema Version 和 Spool 可用性；
- `/metrics`：Prometheus 文本指标。

### 重点指标

- `qmigration_migrations_failed`；
- `qmigration_task_progress`、吞吐、ETA、P95/P99 SLA；
- `qmigration_task_chunks_pending/running/failed`；
- `qmigration_cdc_lag_seconds`；
- `qmigration_cdc_spool_pending_*`、Storage Used、Critical ETA；
- `qmigration_cdc_dlq_open`、Commit Uncertain、Replay Required；
- Worker CPU、Memory、Running Jobs、Scheduler Load；
- 元数据表大小、Dead Ratio、Maintenance Failures；
- Validation Mismatch 和报告归档失败。

`deployments/prometheus-rules.yml` 提供基础告警。当前告警只在产品内展示/确认，没有邮件、短信、Webhook、值班平台路由和静默策略。

## 9. 日常巡检

每日：

- `/readyz`、失败任务、SUSPECT/OFFLINE Worker；
- CDC Lag、Spool 水位和增长速度；
- 未解决 DLQ 和 Commit Unknown；
- 校验不一致和报告归档失败；
- PostgreSQL 容量、Dead Tuple 和备份结果。

每周：

- 恢复抽检、Master Key/Secret 备份可用性；
- 源端日志保留与 CDC 权限；
- Worker 负载分布、拓扑标签和长尾 Chunk；
- 证书/Token/报告签名密钥到期时间；
- 支持矩阵与实际环境版本是否仍一致。

每个发布周期：

- 真实数据库 E2E、断网/进程崩溃/磁盘满故障注入；
- Metadata + Spool 一致性恢复；
- Cutover/Rollback 演练；
- 镜像和依赖安全扫描；
- 文档和版本一致性检查。

## 10. 故障处理

### Worker 宕机

停止新的领取，等待 Chunk/Engine Job Lease 过期后由其他 Worker 接管。核对旧 Worker 是否仍可能运行，避免时钟/网络分区造成双执行风险。QMigration Repository 的 Owner/Lease 校验是最后防线。

### Server 宕机

Worker 的 Chunk/Engine Job 依赖持久 Lease 恢复。Server 的 Precheck/Validation 使用 PostgreSQL `control_operation_leases`，每 30 秒巡检：Precheck 可安全重启，Validation 会删除本轮不完整结果后重跑。Prepare 在可能修改目标 Schema 后不可证明幂等，因此租约过期时失败关闭；按错误提示核查目标对象后修复或重建任务。Cutover/Rollback 仍应避免与 Server 滚动发布重叠，后续需改为持久 Saga。

### Spool CRITICAL

系统会对源端 ACK 施加反压。不要直接删除 Pending 文件或数据库索引。先扩容、修复挂载/S3、降低 Full 并行度或提高 Drain 能力；确认 Metadata 与对象一致后再恢复。

### COMMIT_UNCERTAIN

禁止自动重放。通过目标库事务/业务证据确认：

- 已提交：选择 `COMMITTED`，只推进位点；
- 未提交：选择 `NOT_COMMITTED`，进入受控重放；
- 无法确认：保持阻塞并升级处理。

### Schema Version Mismatch

停止滚动发布，确认二进制版本、Migration 文件和数据库 Marker。不要手工只改 Marker 来绕过 `/readyz`。

## 11. 新增 Connector 的要求

1. 实现 Factory 和准确的 Descriptor；
2. 先实现 Protocol Probe 和 Metadata；
3. 实现 Full Read/Write、稳定键和 Schema；
4. 接入统一 Pipeline，不绕过写后 Checkpoint；
5. CDC 必须证明事务边界、持久位点、重连和 ACK 顺序；
6. Target CDC 应提供原子事务 Apply；
7. 增加数据类型、NULL/空值、LOB/Binary、DDL 和错误注入测试；
8. 增加真实数据库 Qualification 工具和文档；
9. 默认保持门禁，完成 E2E/长稳后才能调整 Maturity；
10. 更新 UI、支持矩阵、镜像、Worker Capability 和 Release Notes。

## 12. 当前维护债务

优先处理：

1. 将 Cutover/Rollback 改为持久、幂等 Saga；
2. 前端 ESLint、单元测试和 Playwright E2E；
3. 拆分超大 Migration Service、API Server 和 Migrations 页面；
4. OpenAPI、分页、过滤、Operation Resource 和幂等键规范；
5. PostgreSQL Repository 连接池/并发模型和压测；
6. Helm/Kustomize、NetworkPolicy、供应链签名和漏洞扫描；
7. 登录防护、OIDC/MFA 和 Secret Provider；
8. 真实数据库版本矩阵、长稳与故障注入资格验证。
