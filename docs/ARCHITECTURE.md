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

兼容 Debezium/Canal JSON 的 HTTP 接口只是**输入格式兼容层**，不是 Debezium/Canal 引擎，也不需要安装它们。

Oracle LogMiner、SQL Server CDC/LSN、TiDB TiCDC、OceanBase Binlog Service 均接入同一个 CDC Reader SPI，不新增“外部引擎选择”。

## 5. Connector SPI

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

## 6. 调度与反压

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

## 7. 一个工具的产品语义

用户创建任务时只选择：

```text
源数据库
目标数据库
迁移对象
FULL / FULL+CDC / CDC
并发/限速/校验/割接策略
```

用户**不选择迁移引擎**。QMigration 根据数据源类型、表结构、Migration Key、Partition 和 CDC 协议自动生成内部执行计划。

## 8. 当前 Native 覆盖边界

V0.15.0-unified-dev2 已形成统一运行时，当前真正的数据通道覆盖：

- MySQL / MariaDB / PolarDB-X / PolarDB MySQL：Full Load + MySQL-compatible Binlog CDC；TiDB 使用 TiCDC；OceanBase MySQL 使用显式 ODP/Binlog Service CDC endpoint。
- PostgreSQL / PolarDB PostgreSQL：Full Load + pgoutput CDC。

Oracle、SQL Server、DB2、达梦、Kingbase、openGauss 等在旧版本中的 Generic/JDBC 元数据入口**不再自动退回第三方工具执行**。在对应 QMigration Native Connector/CDC Reader 完成前，任务会明确拒绝启动，而不是伪装成“已支持”。
