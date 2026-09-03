# QMigration 使用手册

适用版本：`0.15.0-rc49`

## 1. 使用前须知

QMigration 当前是 RC 版本。建议先在非生产环境完成目标数据库版本、数据类型、CDC 前置条件、长稳和故障恢复验证。

Docker Compose 已强制认证和生产配置校验。新部署管理员账号默认为 `admin` / `Cljslrl0620!`，首次登录后必须立即修改；数据库密码、Master Key、Worker Token 和 Auth Secret 仍须使用互不相同的强随机值，并将私有环境文件排除在版本控制之外。

## 2. 环境要求

### 容器方式

- Docker Engine 或兼容容器运行时；
- Docker Compose v2；
- 可拉取 Go、Node、Nginx、Alpine 和 PostgreSQL 基础镜像；
- File Spool 需要持久卷，多 Server 需要 RWX；或准备 S3-Compatible 对象存储。

### 源码方式

- Go 1.23；
- Node.js 22 和 npm；
- PostgreSQL 17（生产元数据模式）；
- Linux 是正式构建/运行基线；File Spool 同时提供 Linux `statfs` 与 Windows `GetDiskFreeSpaceExW` 实现，便于开发机执行测试。

## 3. 本地快速启动

```bash
cp deployments/.env.example deployments/.env.local
# 编辑 deployments/.env.local，填入所有空的内部 Secret
docker compose --env-file deployments/.env.local -f deployments/docker-compose.yml up --build
```

访问 `http://127.0.0.1:8088`。API 位于 `http://127.0.0.1:8080`。

停止服务：

```bash
docker compose --env-file deployments/.env.local -f deployments/docker-compose.yml down
```

`down -v` 会删除 PostgreSQL 和 Spool 卷，除非明确要清空数据，否则不要使用。

### Linux 离线安装

将 `qmigration-offline-<版本>-linux-amd64.tar.gz` 和同名 `.sha256` 文件复制到 Linux x86_64 主机，先核验外层文件，再安装：

```bash
sha256sum -c qmigration-offline-*.tar.gz.sha256
tar -xzf qmigration-offline-*.tar.gz
cd qmigration-offline-*
sudo sh install.sh
sudo cat INITIAL_ADMIN_CREDENTIALS.txt
```

安装脚本会先核验包内所有文件；Docker 不可用时安装包内 Docker Engine 29.7.2、Compose v2.40.2 并创建 systemd 服务，然后使用 `docker load` 导入三个本地镜像、生成权限为 `0600` 的 Secret 环境文件，最后以 `--pull never` 启动服务。安装过程不访问网络。执行 `sudo sh verify.sh` 可复核文件完整性、容器状态和 Server Readiness。

宿主机无需预装 Docker，但必须提供 Linux x86_64 内核、systemd、cgroup、iptables、`tar` 和 `sha256sum`。安装器不会覆盖已有 Docker，不会自动授予普通用户具有 root 等价权限的 `docker` 组成员资格。默认卸载保留数据卷和容器运行时；只有确认永久删除元数据和 Spool 后才使用 `sudo sh uninstall.sh --purge`。

若使用现有 Kubernetes 多节点集群，应先在每个可调度节点运行 `sudo sh load-images-kubernetes.sh`，再在控制机运行 `sh install-kubernetes.sh`。安装器使用临时 DaemonSet 检查各节点镜像，并支持 Server/Worker/Web 副本数、HPA、LoadBalancer、Ingress 及外部 HA PostgreSQL。包内含 kubectl v1.37.0；可用 `KUBECTL=/path/to/kubectl` 指定兼容客户端。Kubernetes 模式要求集群预先提供 CNI 和 RWX Spool 存储；内置 PostgreSQL 还要求 RWO 存储，且仅适用于测试/小规模环境。

## 4. 安全启动

生产至少应设置以下值：

| 配置 | 要求 |
|---|---|
| `QMIGRATION_AUTH_REQUIRED=true` | 强制 API 认证 |
| `QMIGRATION_BOOTSTRAP_ADMIN_PASSWORD` | 首次启动创建管理员；包内默认 `Cljslrl0620!`，登录后立即修改并移除 |
| `QMIGRATION_AUTH_SECRET` | 长随机值，用于 Session 签名 |
| `QMIGRATION_MASTER_KEY` | 长随机值，用于静态加密；必须独立备份 |
| `QMIGRATION_WORKER_TOKEN` | 长随机值，Server 与 Worker 一致 |
| `QMIGRATION_METADATA_PASSWORD` | 强随机数据库密码 |
| `QMIGRATION_TLS_CERT` / `QMIGRATION_TLS_KEY` | 或在可信 Ingress/Proxy 终止 TLS |

Compose 已声明并强制透传这些配置。`QMIGRATION_PRODUCTION=true` 时，Server 会拒绝 Open Mode、短 Secret、示例值、复用 Secret、非 PostgreSQL 元数据仓库以及通配 CORS。

首次启动后，使用 Bootstrap Admin 登录。日常操作推荐账号登录；CI/CD 或灾备脚本可使用：

```text
QMIGRATION_RBAC_TOKENS=admin:tokenA,dba:tokenB,operator:tokenC,viewer:tokenD
```

## 5. 创建数据源

进入“数据源”页面，填写：

- 名称和数据库类型；
- Host、Port、用户名、密码；
- Database/Schema；
- CDC URL（TiDB、OceanBase、DB2/GBase Provider 等场景需要）；
- TLS Mode、Server Name、CA、客户端证书和私钥；
- 厂商 Driver/DSN（仅资格验证门禁后的达梦/GBase 8s 等场景）。

保存后先执行“连接测试”。连接成功只表示已通过当前 Connector 声明的能力检查；`protocol-probe` 不等于已经具备 Full/CDC 能力。应同时查看 Connector Descriptor 的 `capabilities`、`maturity`、`qualification_required` 和 `note`。

## 6. 创建迁移任务

### 6.1 选择模式

| 模式 | 说明 |
|---|---|
| `FULL` | 只做全量迁移，可选校验后结束 |
| `FULL_AND_INCREMENTAL` | 全量期间持续捕获 CDC，追平后割接 |
| `INCREMENTAL` | 只运行 CDC，要求源/目标和起始状态满足条件 |

### 6.2 常用参数

- `chunk_rows`：默认 100000，控制初始分片目标；
- `batch_rows`：默认 500，Worker 单批行数；
- `parallelism`：默认 4，任务最大并行度；
- `max_retries`：默认 3；
- `auto_create_table`：自动创建目标表；
- `validation_enabled`：全量完成后执行校验；
- `post_load_ddl_mode`：`NONE`、`INDEXES`、`INDEXES_AND_FOREIGN_KEYS`；
- `cdc_ddl_mode`：默认 `REJECT`，`SAME_FAMILY` 只适合同族、同名映射；
- `cdc_conflict_mode`：`SOURCE_WINS` 或 `LAST_WRITE_WINS`。

手工目标吞吐、自动吞吐寻优和完成 SLA 三种模式互斥。限速为 0 表示不配置该限制。

### 6.3 表映射示例

高级表/字段映射当前以 JSON 输入：

```json
[
  {
    "source_schema": "app",
    "source_table": "orders",
    "target_schema": "biz",
    "target_table": "orders_new",
    "columns": [
      {"source_column": "order_id", "target_column": "id"}
    ],
    "split_strategy": "AUTO"
  }
]
```

支持的 Split Strategy：`AUTO`、`PRIMARY_KEY_RANGE`、`UNIQUE_KEY_RANGE`、`HASH`、`PARTITION`、`CUSTOM_SQL`。使用 `CUSTOM_SQL` 时只填写受限制的 `custom_where` 条件，不要拼接完整 SELECT。

留空表映射时，系统按源端默认库/Schema 发现表；正式任务建议显式选择并复核对象范围。

### 6.4 Worker 亲和示例

```json
{"region":"cn-east","zone":"az-a","network":"migration"}
```

`PREFERRED` 在没有匹配 Worker 时允许回退；`REQUIRED` 必须完全匹配标签。

### 6.5 分时限速示例

```json
[
  {
    "start": "08:00",
    "end": "18:00",
    "read_limit_mbps": 100,
    "write_limit_mbps": 80,
    "parallelism": 8
  },
  {
    "start": "18:00",
    "end": "08:00",
    "target_throughput_mbps": 200,
    "parallelism": 16
  }
]
```

设置 `rate_limit_timezone`，例如 `Asia/Shanghai`。时间窗口支持跨午夜。

### 6.6 值转换示例

```json
[
  {
    "source_schema": "app",
    "source_table": "orders",
    "column": "created_at",
    "action": "ZERO_DATE_TO_NULL"
  },
  {
    "column": "customer_name",
    "action": "TRIM"
  }
]
```

支持 `TRIM`、`LOWER`、`UPPER`、`EMPTY_TO_NULL`、`NULL_TO_VALUE`、`REPLACE_LITERAL`、`ZERO_DATE_TO_NULL`、`ZERO_DATE_TO_VALUE`、`JSON_COMPACT`。规则按声明顺序执行，不执行用户脚本或任意 SQL。

## 7. 启动与观察任务

创建后点击“启动”。系统依次执行能力检查、数据库 Precheck、兼容性评估、表/Chunk 规划和数据迁移。

重点关注：

- 状态和 `last_error`；
- 总 Chunk、完成 Chunk、失败 Chunk；
- Rows、Bytes、吞吐和 ETA；
- 有效并行度和 Flow Control；
- Worker 负载和 Lease；
- CDC Lag、Spool 增长、临界 ETA；
- DLQ 的 `OPEN`、`COMMIT_UNCERTAIN`、`REPLAY_REQUIRED`。

任务详情同时展示表、Chunk、Precheck、兼容性评估、Schema 对象、校验、CDC、冲突、日志和 CDC Runtime。

## 8. CDC、割接与回切

### Full + CDC

全量阶段 CDC 事务先进入加密 Durable Spool。只有 Spool 持久化成功后才确认源端位点。全量完成后按 Sequence Drain，并在目标提交后推进正式 Checkpoint。

### 割接建议流程

1. 业务侧进入写入冻结或受控双写窗口；
2. 检查源端 CDC Reader 正常；
3. 确认 Spool Pending 为 0；
4. 确认 CDC Lag 小于门禁值；
5. 确认没有未解决 DLQ/Commit Unknown；
6. 完成最终校验；
7. 执行“进入割接就绪”；
8. 再次核对业务冻结和目标可用性；
9. 执行正式割接；
10. 观察目标业务、错误率和数据一致性。

不要只以页面上的 Lag 数值作为割接依据。Spool、DLQ、校验 Watermark、Schema 对象和业务写冻结都属于门禁的一部分。

### 回切

非 `FULL` 任务完成割接后，可进入回切准备，启动反向 CDC，追平并进入 `ROLLBACK_READY` 后执行回切。回切同样需要业务冻结、Lag、Spool、DLQ 和校验确认。

## 9. 校验和验收报告

校验中心支持 Chunk 行数/Checksum 校验和异常 Chunk 修复。任务详情可以下载：

- HTML 验收报告；
- PDF 验收报告；
- JSON 证据；
- 签名 Manifest；
- Ed25519 验签公钥。

启用 S3/WORM 配置后可将报告归档。离线验签使用 `qmigrationctl verify-report`，公钥轮换场景建议使用本地 Trust Store。

## 10. CLI 示例

```bash
export QMIGRATION_SERVER=http://127.0.0.1:8080
export QMIGRATION_API_TOKEN='viewer-or-operator-token'

qmigrationctl health
qmigrationctl datasources
qmigrationctl migrations
qmigrationctl migration MIGRATION_ID
qmigrationctl start MIGRATION_ID
qmigrationctl logs MIGRATION_ID
qmigrationctl cdc MIGRATION_ID
```

创建数据源或任务：

```bash
qmigrationctl create-datasource datasource.json
qmigrationctl create-migration migration.json
```

报告离线验签：

```bash
qmigrationctl verify-report --public-key public-key.json report-directory
```

## 11. 常见问题

### `/readyz` 返回 503

检查返回 JSON：可能是元数据仓库不可用、Schema 版本与二进制不一致、Spool 不可写或 Spool 达到 CRITICAL 水位。

### Worker 不领取任务

检查 Worker 是否 ONLINE、能力是否匹配 Connector、标签是否满足 `worker_selector`、亲和策略是否为 REQUIRED、有效并行度是否被压力控制降为 0，以及 Worker Token 是否一致。

### 任务停在 PREPARING/VALIDATING

先查任务日志和 Server 日志。Server 每 30 秒运行一次控制面恢复巡检：过期的 `PRECHECKING` 会重启，`VALIDATING` 会清理未完成结果后安全重跑；`PREPARING` 可能已经修改目标对象，系统会在租约过期后失败关闭并给出人工检查提示，不会盲目重复建表。

### CDC 停止推进

检查 CDC Reader Engine Job、源端日志保留、Spool 水位、目标 Apply 错误和 DLQ。`COMMIT_UNCERTAIN` 不允许盲目重放，必须先确认目标事务实际提交结果。

### Windows 无法构建后端

当前代码可在 Windows 执行单元测试，但正式发布仍以 Linux CI 和 Linux 容器为准。GBase 8s 的文件权限/动态库 Provider 是 Linux 能力；Windows 开发时请通过 JSON 环境变量提供 Provider 配置。
