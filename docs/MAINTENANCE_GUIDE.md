# DTS 维护手册

适用源码版本为 `0.15.0-rc50`。

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
| `deployments` | 镜像、Kubernetes、备份恢复、资格验证 |
| `docs` | 当前事实、支持边界、发布和运维证据 |

## 3. 开发与验证

### 后端

```bash
cd backend
go test ./...
go vet ./...
go build ./cmd/server
go build ./cmd/worker
go build ./cmd/dtsctl
```

File Spool 已按 Build Tag 拆分：Linux 使用 `statfs`，Windows 开发测试使用 `GetDiskFreeSpaceExW`。GBase 8s 文件权限和动态库 Provider 仍属于 Linux 运行能力；Windows 只接受 JSON 环境配置。正式 Release Gate 必须在目标 Linux 环境运行，Windows 测试仅作为额外兼容性检查。

### 前端

```bash
cd web
npm ci
npm test
npm run build
```

CI 使用 `package-lock.json` 安装依赖，执行依赖审计、前端单测、类型检查和 Vite 生产构建。

### 部署文件

发布前至少验证：

- Docker 镜像可构建并以非 root 用户运行；
- Kubernetes 安装、检查及保留数据卸载流程；
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
DTS_METADATA_PASSWORD='...' deployments/scripts/backup.sh
DTS_METADATA_PASSWORD='...' deployments/scripts/migrate-metadata.sh
```

脚本在一个事务内执行与 Server 相同的 `schema.sql`。数据库事务级 advisory lock 将并发的 Schema 初始化串行化。迁移完成后确认：

```sql
SELECT schema_version, updated_at
FROM metadata_schema_state
WHERE id = 1;
```

应与二进制版本完全一致，否则 `/readyz` 会失败。若由 DBA 先执行迁移，可在所有 Server 上设 `DTS_METADATA_AUTO_MIGRATE=false`，服务将核对现有版本再启动。

运行时和运维脚本共用 `backend/internal/repository/postgres/schema.sql`，离线副本位于 `migrations/schema.sql`。编号 SQL 保留为各版本升级记录。Server 元数据连接池默认每副本最多 4 条连接，`DTS_METADATA_POOL_SIZE` 可设为 1 到 128。数据库连接预算需计入全部 Server 副本。

## 6. 离线包构建与验证

推荐在 Linux x86_64 联网构建机运行：

```bash
sh deployments/offline/build-offline-package.sh
```

Windows 构建机可运行 `deployments/offline/build-offline-package.ps1`；该流程使用 Go 直接组装标准 Docker image archive，不要求本机 Docker daemon。两种构建方式都会产出 `dist/dts-kubernetes-offline-<版本>-linux-amd64.tar.gz`、外层 SHA-256 文件和包内 `SHA256SUMS`。

设置 `CLUSTER_PAYLOAD_DIR` 后，Linux 构建器会同时打入控制面镜像、Registry、containerd、CNI 和麒麟 RPM。完整包应包含 `images/all-images.tar.gz`。精简包面向已有 Kubernetes 集群，分别存放应用镜像归档。

验收时核对镜像架构为 `linux/amd64`，检查 Server/Worker、CLI、CDC Runtime、`zstd` 和 CA 根证书，并按 `runtime/versions.env` 核对三个 Kubernetes 工具。完整包须在断网节点验证私有仓库拉取、跨节点调度、持久存储和重启恢复。精简包另需验证各可调度节点的镜像导入。两种包均使用 LF 换行的 `SHA256SUMS`。

## 6. 备份与恢复

### 备份范围

必须备份：

- PostgreSQL 元数据；
- File Spool 共享卷，或 S3 Bucket/Prefix 及其版本/保留策略；
- `DTS_MASTER_KEY`；
- `DTS_AUTH_SECRET`；
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

`restore.sh` 使用 `pg_restore --clean --if-exists`，属于破坏性操作，必须设置 `DTS_RESTORE_CONFIRM=YES`，且应只对已确认的目标数据库执行。

## 7. 配置分类

### Server 与认证

- `DTS_ADDR`；
- `DTS_TLS_CERT`、`DTS_TLS_KEY`；
- `DTS_AUTH_REQUIRED`；
- `DTS_BOOTSTRAP_ADMIN_USER/PASSWORD`；
- `DTS_AUTH_SECRET`；
- `DTS_SESSION_TTL_HOURS`；
- `DTS_RBAC_TOKENS`；
- `DTS_CORS_ORIGIN`；
- `DTS_WORKER_TOKEN`。

### 元数据

- `DTS_REPOSITORY=postgres`；
- `DTS_METADATA_HOST/PORT/USER/PASSWORD/DATABASE`；
- 开发用 `DTS_STATE_FILE`；
- Maintenance Retention 相关变量。

### Worker

- `DTS_SERVER`；
- `DTS_WORKER_CONCURRENCY`；
- `DTS_CDC_CONCURRENCY`；
- `DTS_WORKER_LABELS`；
- `DTS_WORKER_SHUTDOWN_GRACE_SECONDS`；
- `DTS_BIN_DIR`；
- `DTS_PIPELINE_BUFFER_BATCHES`；
- `DTS_ADAPTIVE_BATCH`。

### CDC Spool

- `DTS_CDC_SPOOL_STORAGE=file|shared-fs|s3|metadata`；
- `DTS_CDC_SPOOL_DIR`；
- `DTS_CDC_SPOOL_MAX_TRANSACTION_BYTES`；
- `DTS_CDC_SPOOL_MAX_PENDING_BYTES`；
- `DTS_CDC_SPOOL_DISK_WARN_PCT`；
- `DTS_CDC_SPOOL_DISK_CRITICAL_PCT`；
- S3 Endpoint、Bucket、Region、Credential、TLS 和 Retry 变量。

S3 完整契约见 `docs/SPOOL_STORAGE.md`。报告归档、HSM/KMS、Ed25519 和 TSA 配置见现有 Validation Report 相关 Release/Implementation 文档和代码环境变量。

### 实验 Connector

`DTS_EXPERIMENTAL_*` 只能在完成对应真实数据库资格验证、故障注入、长稳和回滚验证后开启。Server 与 Worker 必须使用一致的实验开关，否则能力上报与执行会不匹配。

## 8. 可观测性

### 健康检查

- `/healthz`：进程存活和版本；
- `/readyz`：Repository、Schema Version 和 Spool 可用性；
- `/metrics`：Prometheus 文本指标。

### 重点指标

- `dts_migrations_failed`；
- `dts_task_progress`、吞吐、ETA、P95/P99 SLA；
- `dts_task_chunks_pending/running/failed`；
- `dts_cdc_lag_seconds`；
- `dts_cdc_spool_pending_*`、Storage Used、Critical ETA；
- `dts_cdc_dlq_open`、Commit Uncertain、Replay Required；
- Worker CPU、Memory、Running Jobs、Scheduler Load；
- 元数据表大小、Dead Ratio、Maintenance Failures；
- Validation Mismatch 和报告归档失败。

### 遥测和通知

Server 与 Worker 支持 OpenTelemetry OTLP/HTTP。将 `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` 指向采集器的完整 `/v1/traces` 地址。Worker 调用控制面时传递 W3C Trace Context，Server 设置 `DTS_LOG_FORMAT=json` 可在 HTTP 日志中按 trace ID 和请求 ID 检索。含查询参数的请求（包括 OIDC 授权码回调）会跳过 HTTP 跟踪。

Prometheus 加载 `deployments/prometheus-rules.yml` 并配置已有 Alertmanager 地址。Alertmanager 可使用 `deployments/alertmanager.example.yml`，两个 Secret 文件分别保存接收标准 Alertmanager Webhook 的运维入口和当班告警入口。严重告警在持续期间每 30 分钟重发，恢复时发送解除通知。静默通过 Alertmanager 的 Silence 界面管理。正式上线前在现场验证接收端的触发和解除。

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

停止新的领取，等待 Chunk/Engine Job Lease 过期后由其他 Worker 接管。核对旧 Worker 是否仍可能运行，避免时钟/网络分区造成双执行风险。DTS Repository 的 Owner/Lease 校验是最后防线。

### Server 宕机

Worker 的 Chunk/Engine Job 依赖持久 Lease 恢复。Server 的 Precheck/Validation 使用 PostgreSQL `control_operation_leases`，每 30 秒巡检：Precheck 可安全重启，Validation 会删除本轮不完整结果后重跑。Prepare 在可能修改目标 Schema 后不可证明幂等，因此租约过期时失败关闭；按错误提示核查目标对象后修复或重建任务。Cutover/Rollback 已增加控制面租约，最终 RUNNING 状态中断后失败关闭，仍应避免与 Server 滚动发布重叠，后续需改为持久 Saga。当前修复与剩余门禁见[企业就绪度复查](ENTERPRISE_READINESS.md)。

暂停 CDC 不再删除源端复制槽；全量/校验暂停时 Full+CDC 可以继续缓冲捕获的数据。须监控 WAL/Spool 增长及源日志保留，只有成功割接/回切后才清理对应方向 Checkpoint。清理警告需要人工复查；取消/失败任务的源资源清理策略仍需单独设计，不要直接删除尚需恢复的复制槽。

### Spool CRITICAL

系统会对源端 ACK 施加反压。不要直接删除 Pending 文件或数据库索引。先扩容、修复挂载/S3、降低 Full 并行度或提高 Drain 能力；确认 Metadata 与对象一致后再恢复。

### COMMIT_UNCERTAIN

禁止自动重放。通过目标库事务/业务证据确认：

- 已提交：选择 `COMMITTED`，只推进位点；
- 未提交：选择 `NOT_COMMITTED`，进入受控重放；
- 无法确认：保持阻塞并升级处理。

### MySQL Binlog 校验失败

MySQL 原生 CDC 在每次 binlog 轮转后读取文件的格式描述，自动识别 OFF 或 CRC32。出现 `invalid binlog CRC32` 时，记录日志文件和最近已确认的位点，用 `dts-binlog-inspect -file <binlog副本>` 核对源文件。修复源文件或切换到包含同一事务的健康副本后，从已确认位点恢复，以保留出错事务。

MySQL 压缩事务的解码上限为 128 MiB，单个压缩容器最多包含 100,000 个 binlog 事件。达到错误条件时读取器报错，保留最近已确认的位点。处理超限事务前应核对原事务及目标写入情况。

### 旧版本的分片与行过滤任务

旧版本的 DB2 全量读取曾跳过整数分片末行，也可能读入相邻复合键分片的数据。升级后应按源表重新核对边界键、行数和校验结果。旧报告使用了当时的读取逻辑，不能据此排除漏行。

对已经运行过的 `CUSTOM_SQL` 全量加增量任务，先核对目标表是否包含过滤范围外的数据，并保留最后确认的 CDC 位点。当前行过滤只支持 `FULL`。重新规划前，应由业务方确认目标数据范围。

### CDC 并发冲突与租约丢失

同一任务、方向的 Apply、Spool 排空、死信处置及校验屏障共享持久租约。HTTP 409 携带 `Retry-After: 1` 表示争用：保留源事务并退避重试，不可 ACK。默认租期 300 秒，每 TTL/3 续期；`DTS_CDC_SPOOL_DRAIN_LEASE_SECONDS` 限制在 30–3600 秒。

元数据 Checkpoint 或 Pending Spool 读取失败会阻止 Apply/恢复，不再静默回到初始位点。先恢复元数据/Spool 访问，再重试。OPEN 死信应先重放原事务；RESOLVED 历史死信不能再次重放。

本次补丁升级要求暂停迁移/桥接写入并更新全部 Server/Worker，避免新旧二进制混跑绕过门禁。租约不是目标库原子 fencing，剩余门禁与验证证据见[企业就绪度续查](ENTERPRISE_READINESS.md#6-2026-10-30-续查与修复)。

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
