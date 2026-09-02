# QMigration V0.6.0 Release Notes

## 重点升级

### 1. Native MySQL CDC 优先 GTID

- 支持 `COM_BINLOG_DUMP_GTID`。
- 捕获 `Executed_Gtid_Set` 作为迁移起点。
- 每个已成功 Apply 的源事务推进完整 GTID Set。
- Worker/Reader 重启时从最新持久化 GTID Checkpoint 重新渲染。
- 不支持 GTID 的 MySQL 兼容数据库自动回退 `file:position`。

### 2. 安全 DDL CDC

默认 `cdc_ddl_mode=REJECT`。只有源/目标属于同一数据库家族，并且 Schema/Table/Column 均为完全同名映射时，`SAME_FAMILY` 才会原样执行 DDL。任何不安全 DDL 都不会推进 CDC Checkpoint。

### 3. 字符串/复合主键 Native Full Load

- 整数单主键：`PRIMARY_KEY_RANGE`，支持多 Chunk 并行。
- 字符串/复合主键：`PRIMARY_KEY_KEYSET`，使用 lexicographic tuple keyset，不使用 OFFSET。
- 每批目标 UPSERT 成功后持久化 JSON Tuple Cursor。
- Cursor 与累计 Rows/Bytes 同步持久化，Worker 漂移后数据与指标均可恢复。
- 内置 Validation 支持 Generic Keyset。

### 4. E2E 测试框架

`make e2e` 会启动 MySQL 8.4（GTID + ROW/FULL）与 PostgreSQL（logical WAL），验证复合 PK Full Load、MySQL GTID replication transport 和 PostgreSQL logical replication transport。

## 已知边界

- MySQL binary JSON OPAQUE / partial-update 尚未 Native 支持。
- PostgreSQL DDL 不通过 pgoutput 自动捕获。
- 无主键表继续使用 SeaTunnel/DataX。
- Generic Keyset 当前为单 Chunk 可恢复执行，尚未按采样边界并行切分。
- Oracle/SQL Server Native CDC 与 Active-Active 冲突处理仍在后续版本。
