# 开源迁移工具能力融合原则

QMigration 的目标不是管理 DataX、SeaTunnel、Flink CDC、Debezium、Canal，而是把这些项目中成熟的工程思想重新实现为一个统一的 QMigration Runtime。

## 融合映射

| 来源 | 吸收 | 不保留 |
|---|---|---|
| DataX | Reader/Writer、批处理、split key、流控 | `datax.py`、DataX Job JSON、DataX Worker runtime |
| SeaTunnel | Connector 抽象、Source/Transform/Sink、并行分片 | SeaTunnel Adapter、HOCON Job、Zeta runtime |
| Flink CDC | 反压、Checkpoint、Snapshot/CDC 状态衔接 | Flink SQL、JobManager/TaskManager、Connector JAR 依赖 |
| Debezium | Offset/transaction/schema-history/event envelope 思路 | Kafka Connect/Debezium runtime 依赖 |
| Canal | MySQL Binlog 轻量解析思路 | Canal Server/Adapter runtime 依赖 |

## QMigration 统一实现

```text
Connector
  ↓
Planner → Chunk → Worker
  ↓
Reader → Bounded Pipeline → Transform → Writer
  ↓                                 ↓
Backpressure                    Target Commit
                                      ↓
                               Durable Checkpoint

CDC Reader → Tx Assembler → CDCEvent → Atomic Apply → Position
```

从 V0.15 起，任务中的历史 `full_engine/cdc_engine/rollback_cdc_engine` 字段只用于兼容旧元数据，服务端统一归一化为 `qmigration`。Web 不再提供引擎选择页。

实现原则：借鉴公开的架构思想和协议行为，自研代码实现，不把第三方源码直接拼装成一个仓库。
