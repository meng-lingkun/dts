# QMigration Unified Engine Architecture

## 1. 目标

QMigration V0.15 起不再定位为“多个开源迁移工具的管理平台”，而是一个**单一、自研、统一的数据迁移内核**。

运行时只有 QMigration：

```text
Vue Console / CLI
        ↓
QMigration Control Plane
        ↓
QMigration Unified Engine
        ├─ Connector SPI
        ├─ Snapshot Coordinator
        ├─ Split Planner
        ├─ Reader
        ├─ Bounded Pipeline
        ├─ Transformer
        ├─ Writer
        ├─ Backpressure Controller
        ├─ Durable Checkpoint Store
        ├─ Unified CDC Runtime
        ├─ Validation Engine
        └─ Cutover / Rollback Engine
```

DataX、SeaTunnel、Flink CDC、Debezium、Canal **不是运行依赖，也不是可选执行引擎**。QMigration 只吸收它们经过实践验证的设计思想，并用 Go 重新实现自己的运行时。

## 2. 开源能力如何融合

| 参考项目 | 吸收的核心思想 | QMigration 自研实现 |
|---|---|---|
| DataX | Reader/Writer 分层、批量搬运、split key、限速 | Connector DataReader/DataWriter、Range/Keyset/HASH/PARTITION、Batch/Rows/QPS/MBps 限速 |
| SeaTunnel | Source/Transform/Sink 抽象、并行任务、Connector 能力边界 | Connector SPI、MigrationTable/Chunk、Worker Scheduler、Topology/Affinity |
| Flink CDC | 有界缓冲、反压、Snapshot+Log、Checkpoint/状态恢复 | Bounded Pipeline、Task/Chunk 两级 Backpressure、CDC gate、apply-before-checkpoint |
| Debezium | Offset、事务边界、Schema History/Event Envelope | CDCPosition、transactional apply、DDL policy、兼容 JSON envelope parser |
| Canal | 轻量 MySQL Binlog 解析与消费模型 | QMigration MySQL binlog protocol/row-event decoder/GTID reader |

> 融合的是设计能力，不是把第三方源码拼到一起，也不通过 shell/JAR/Python 去启动这些工具。

## 3. Full Load 数据面

```text
Source Connector
      ↓
Chunk Planner
      ↓
Reader ──→ bounded channel ──→ Transformer ──→ Writer
                                          ↓
                                    Target Commit
                                          ↓
                                  Durable Checkpoint
```

关键语义：

1. Reader 可小规模预取，但 Channel 有界，Sink 变慢时自然反压 Source。
2. Writer 成功之前绝不提交 Cursor。
3. Worker 崩溃时只从服务器已持久化的 Cursor 恢复；已预取但未提交的数据允许重放，绝不跳过。
4. Chunk 支持 Range、Bounded Keyset、HASH、PARTITION、CUSTOM SQL。
5. 慢 Chunk 可在安全条件下二次拆分。

## 4. Unified CDC

统一事件模型：

```text
Vendor Log Reader
      ↓
CDC Decoder
      ↓
Transaction Assembler
      ↓
QMigration CDCEvent
      ↓
Schema/Mapping/Conflict Policy
      ↓
Atomic Target Apply
      ↓ success
Durable Position / ACK
```

当前原生 Reader：

- MySQL family：Binlog + GTID，ROW event，事务组装。
- PostgreSQL family：logical replication + pgoutput + LSN。
- SQL Server：TDS + SQL Server CDC change table/LSN（当前仍通过 experimental gate 开放）。

兼容 Debezium/Canal JSON 的 HTTP 接口只是**输入格式兼容层**，不是 Debezium/Canal 引擎，也不需要安装它们。

Oracle LogMiner、SQL Server CDC/LSN、TiDB TiCDC、OceanBase Binlog Service 均接入同一个 CDC Reader SPI，不新增“外部引擎选择”。

## 5. Durable CDC Spool

Full+CDC 不再要求源端 Binlog/WAL/SQL Server CDC retention 覆盖整个 Full Snapshot：

```text
Native Source CDC Reader
          ↓
      Transaction
          ↓
 gzip compress + AES-256-GCM
          ↓
 QMigration Durable CDC Spool
          ↓ durable success
      Source ACK

Full Snapshot continues
          ↓
CDC_CATCHING_UP
          ↓
Drain spool by sequence
          ↓
Atomic Target Apply
          ↓ success
Target CDC Checkpoint
```

核心不变量：

1. Snapshot 阶段：`spool durable -> source ACK`；目标还没有应用，所以不能推进 target apply checkpoint。
2. Catch-up 阶段：严格按 spool sequence 回放；有历史 backlog 时，新 live transaction 必须先 stage，不能越过旧事务。
3. Worker failover 使用 newest pending spool position 作为 source reconnect 起点，避免重复解码已经 durable-stage 的范围。
4. Spool 容量不足时不 ACK source，形成最后一级安全 backpressure；绝不丢事务。
5. Cutover / rollback ready gate 要求 pending spool transaction 为 0。
6. Full+CDC 自动流程先 catch-up durable backlog，再进入 validation。

RC29 增加 predictive pressure loop：在 `FULL_MIGRATING` 时，控制面同时观察数据库 RuntimeLoad、Worker CPU/Memory、Chunk latency、CDC Spool pending bytes、Spool storage level、backlog growth B/s 与 projected critical ETA。Full parallelism 和下一批 batch target 可以提前降低，但 CDC source ACK/order/checkpoint 语义不变。

Batch 控制采用有界 AIMD 风格收敛：目标是把 read/write bottleneck batch latency 保持在可配置窗口内，单轮最多 +25% / -50%，避免大库长跑时的 2x 震荡。

默认安全限制：单事务 16 MiB，pending spool 64 GiB；均可通过环境变量调整。

V0.15 unified-dev7 将 Payload 存储拆成 Metadata Index + 独立加密文件：

```text
Metadata
  sequence / task / position / status / payload_ref
                 │
                 ↓
Encrypted File Spool (default)
  pending/<hash-segment>/<transaction>.blob
                 │ after target commit
                 ↓
  applied/<date>/<hash-segment>/<transaction>.blob
```

文件先 `fsync + atomic rename`，Metadata 索引提交成功后才允许 Source ACK。WARN 水位延迟捕获，CRITICAL 水位拒绝 Stage 并保持源位点未确认。多 Server 必须共享同一 RWX spool 文件系统；PostgreSQL `cdc_spool_drain_leases` 提供跨 Server Drain 互斥。

Validation 使用持久化 Watermark Barrier：spool=0、CDC lag 达标且 checkpoint 静默窗口满足后捕获 durable checkpoint；若扫描期间 checkpoint 变化，本 generation 结果会被删除并回到 catch-up。

## 6. Connector SPI

每种数据库通过 QMigration Connector 实现能力接口：

```text
Metadata
Snapshot Read
Batch Write
DDL
CDC Position
CDC Stream
Topology
Runtime Load
Validation Read
```

上层 Planner/Scheduler/Checkpoint 不感知厂商 SDK，也不感知第三方迁移工具。

## 7. 调度与反压

```text
Source/Target latency + DB runtime load + Worker load
                     ↓
             Pressure Classifier
               /          \
        Chunk Control    Task Control
        batch/pause      parallelism
```

同时结合：

- Worker Lease / failover
- dynamic chunk refinement
- zone/rack/network affinity
- PolarDB-X / TiDB / OceanBase topology hints
- per-task effective parallelism

## 8. 一个工具的产品语义

用户创建任务时只选择：

```text
源数据库
目标数据库
迁移对象
FULL / FULL+CDC / CDC
并发/限速/校验/割接策略
```

用户**不选择迁移引擎**。QMigration 根据数据源类型、表结构、Migration Key、Partition 和 CDC 协议自动生成内部执行计划。

## 9. 当前 Native 覆盖边界

V0.15.0-rc18 已形成统一运行时，当前真正的数据通道覆盖：

- MySQL / MariaDB / PolarDB MySQL / PolarDB-X：Native Full Load + MySQL-compatible Binlog CDC（产品前置条件由 Precheck 兜底）。
- TiDB：Native Full Load + 独立 TiCDC OpenAPI/Kafka Canal-JSON CDC；不走 COM_BINLOG_DUMP。
- OceanBase MySQL：Native Full Load + 显式 tenant ODP/Binlog Service CDC；SQL endpoint 与 CDC endpoint 分离，GTID/file-position 可恢复。
- PostgreSQL / PolarDB PostgreSQL：Full Load + pgoutput CDC。

SQL Server 已进入 QMigration Native TDS/TLS + experimental Full/CDC 数据面，并已接入 ordered keyset boundary、partition split、runtime-load 与 CDC retention/durable-LSN hardening；Oracle 已形成 Native TNS/TCPS/TTC Full/target/Schema/LogMiner 软件面并保留资格验证 gate；DB2 LUW 已形成 QMigration Native DRDA/DDM Metadata、Full Reader、ordered keyset boundary、Prepared SQLDTA/EXTDTA target writer 与 experimental target/schema apply；RC7 起通过源端 QMigration Log Agent 调用 IBM `db2ReadLog`，用 `DB2_LRI` 接入 Unified CDC Runtime；RC8-RC12 继续补齐 VALUE COMPRESSION、LOB/XML、multi-insert/补偿、relocation/decomposed update 与 VECTOR Full/CDC/target 软件路径。RC13 新增 Dameng/DM8 qualification-gated Connector：Metadata、keyset Full Read、Schema、Prepared Full Write 与 transactional target CDC Apply 均由 QMigration 实现，DM 官方 Go `database/sql` 驱动仅作为可替换 SQL transport provider，并可在 Linux 通过 provider plugin 运行时注册；Dameng source CDC 与 provider TLS 仍未声明。RC14 将 GaussDB 从 probe-only 提升为 qualification-gated PostgreSQL-wire Full/target 数据面并接入 LSN-based `mppdb_decoding` CDC；RC15 将源端值通道切换到官方 binary logical functions，按 B/C/I/U/D 长度帧保留 NULL/empty、NUL、非 UTF-8 与 bytea 精确字节，同时保持 peek -> target commit/durable checkpoint -> get_binary_changes ACK 顺序；RC16 增加独立的 DDL text-classification pass，在同一 commit-LSN 边界继续以 binary path 获取 DML，并只在显式 DDL-only source policy 下回放 selected-table ALTER/TRUNCATE/CREATE INDEX；GaussDB 官方不完整解码的 hybrid DDL/DML 与 multi-primary 继续 fail-closed。RC17 将 GBase 8a MPP 从 probe-only 提升为 qualification-gated Native Full 数据面：复用 QMigration 已审计的 MySQL/GBase-compatible packet transport，但使用独立 GBase Factory/能力面，提供 information_schema Metadata、稳定键 Full Read、EXPRESS Schema 与基于 staging+MERGE 的 keyed Full Write；不继承 MySQL Binlog CDC，不声明 transactional target CDC apply/FK replay，GBase 8s/8c 也不在该 Connector 范围内。 RC18 修正 GBase 8a MERGE 的分布前提：自动目标改为从稳定 migration key 选择可用列建立 HASH 分布，并在写入前以 SHOW CREATE TABLE 校验实际 HASH 列全部属于 MERGE key；random/REPLICATED 或分布键不匹配的预建目标 fail-closed。 RC19 将 GBase 8s V8.8 作为完全独立产品族接入：厂商 Client-SDK ODBC 仅提供 SQL transport，QMigration 自己实现 catalog/keyset Full、target schema、稳定键 prepared replay 与 transactional target apply；8s source CDC 在形成可证明的 durable position/transaction/ACK API 之前保持关闭。 RC20 在确认 syscdcv1 CDC session + CSDK smart-LOB 数据流和 restart sequence 语义后，新增 datasource-local CDC provider agent；QMigration 用 `GBASE8S_CDC_SEQ` 的 restart/commit 双水位处理未完成长事务，并保持 target/spool durable apply 后才推进本地 commit watermark。


## Native Connector 扩展规则（dev3）

新数据库不能通过“执行外部迁移程序”进入 QMigration。数据库厂商提供、只负责连接/SQL 传输的 client/driver 可以作为 Connector transport provider，但 Metadata、Full、CDC、Schema、checkpoint 与 failover 语义必须由 QMigration 自己实现。接入顺序固定为：

1. `protocol-probe` / transport qualification：QMigration 自有 wire，或经过资格验证的厂商连接驱动；
2. `metadata`：实现由 QMigration 控制的 catalog 查询；
3. `full-read/full-write`：进入统一 Reader/Transform/Writer Pipeline；
4. `cdc-read/cdc-apply`：进入统一 CDC Runtime；
5. 真实数据库 E2E/长稳验证后再解除 experimental gate。

Oracle 已完成传输、TTC 协商/认证、Data Dictionary/Full Reader 与 LogMiner/SCN CDC 的实验实现，但真实 Oracle E2E/长稳验证完成前仍不解除 gate；target bind/LOB DML 未完成前不暴露 FullWrite/schema/DDL apply。SQL Server 已实现步骤 1~4 的实验代码，其中 Native Full 与 Native CDC 分别通过显式环境开关开放；真实数据库 E2E/长稳验证完成前不解除 gate。

### S3-Compatible Spool

V0.15 unified-dev8 增加 QMigration 原生 S3-Compatible 后端：

```text
CDC Transaction
  ↓
gzip + AES-256-GCM
  ↓
SigV4 PUT
  ↓
S3 / MinIO / Ceph RGW
  ↓
Metadata opaque reference
```

对象存储仍只是 QMigration Durable Spool 的存储介质，不是迁移引擎。Source ACK 依旧只发生在加密对象持久化成功且 Metadata sequence/index 提交成功之后。
