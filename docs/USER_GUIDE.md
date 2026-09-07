# DTS 使用手册

适用版本：`0.15.0-rc49`

## 1. 使用前须知

DTS 当前是 RC 版本。建议先在非生产环境完成目标数据库版本、数据类型、CDC 前置条件、长稳和故障恢复验证。

Kubernetes 部署已强制认证和生产配置校验。新部署管理员账号默认为 `admin` / `Cljslrl0620!`，首次登录后必须立即修改；数据库密码、Master Key、Worker Token 和 Auth Secret 仍须使用互不相同的强随机值，并将私有环境文件排除在版本控制之外。

## 2. 环境要求

### Kubernetes 方式

- 现有 Linux amd64 Kubernetes 集群，节点使用 containerd；
- CNI、RWX Spool 存储；内置 PostgreSQL 需要 RWO 存储；
- 配置好 kubeconfig，包内提供 kubectl；
- Docker 仅在联网构建机制作镜像，运行节点无需 Docker Engine/Compose。

### 源码方式

- Go 1.23；
- Node.js 22 和 npm；
- PostgreSQL 17（生产元数据模式）；
- Linux 是正式构建/运行基线；File Spool 同时提供 Linux `statfs` 与 Windows `GetDiskFreeSpaceExW` 实现，便于开发机执行测试。

## 3. Kubernetes 离线安装

使用 `dts-kubernetes-offline-<版本>-linux-amd64.tar.gz`，核验同名 SHA-256 文件后解压。在每个可调度节点运行：

```bash
sudo sh load-images-kubernetes.sh
```

在配置好 kubeconfig 的控制机执行：

```bash
sh install.sh
./runtime/kubectl -n dts port-forward svc/web 8088:80
```

访问 `http://127.0.0.1:8088`。统一安装入口调用 Kubernetes 安装器，临时 DaemonSet 会检查各节点镜像。副本数、RWX StorageClass、外部 HA PostgreSQL、HPA、Ingress 和 LoadBalancer 参数见包内 README。

`sh verify.sh` 检查包完整性及 Kubernetes 运行状态；`sh uninstall.sh` 移除应用和入口，保留 PostgreSQL、PVC、namespace 和 Secret。重装需使用相同数据库及存储配置。

本包包含应用镜像和 kubectl，不安装 Kubernetes 控制面、containerd、CNI 或 CSI。Docker Engine、Compose 已从运行与安装流程中移除。

## 4. 安全启动

生产至少应设置以下值：

| 配置 | 要求 |
|---|---|
| `DTS_AUTH_REQUIRED=true` | 强制 API 认证 |
| `DTS_BOOTSTRAP_ADMIN_PASSWORD` | 首次启动创建管理员；包内默认 `Cljslrl0620!`，登录后立即修改并移除 |
| `DTS_AUTH_SECRET` | 长随机值，用于 Session 签名 |
| `DTS_MASTER_KEY` | 长随机值，用于静态加密；必须独立备份 |
| `DTS_WORKER_TOKEN` | 长随机值，Server 与 Worker 一致 |
| `DTS_METADATA_PASSWORD` | 强随机数据库密码 |
| `DTS_TLS_CERT` / `DTS_TLS_KEY` | 或在可信 Ingress/Proxy 终止 TLS |

Kubernetes 清单通过 ConfigMap/Secret 传入这些配置。`DTS_PRODUCTION=true` 时，Server 会拒绝 Open Mode、短 Secret、示例值、复用 Secret、非 PostgreSQL 元数据仓库以及通配 CORS。

首次启动后，使用 Bootstrap Admin 登录。日常操作推荐账号登录；CI/CD 或灾备脚本可使用：

```text
DTS_RBAC_TOKENS=admin:tokenA,dba:tokenB,operator:tokenC,viewer:tokenD
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

启用 S3/WORM 配置后可将报告归档。离线验签使用 `dtsctl verify-report`，公钥轮换场景建议使用本地 Trust Store。

## 10. CLI 示例

```bash
export DTS_SERVER=http://127.0.0.1:8080
export DTS_API_TOKEN='viewer-or-operator-token'

dtsctl health
dtsctl datasources
dtsctl migrations
dtsctl migration MIGRATION_ID
dtsctl start MIGRATION_ID
dtsctl logs MIGRATION_ID
dtsctl cdc MIGRATION_ID
```

创建数据源或任务：

```bash
dtsctl create-datasource datasource.json
dtsctl create-migration migration.json
```

报告离线验签：

```bash
dtsctl verify-report --public-key public-key.json report-directory
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
