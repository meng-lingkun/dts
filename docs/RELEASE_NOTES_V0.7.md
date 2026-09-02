# QMigration V0.7.0 Release Notes

## 主题：CDC Recovery / DLQ / Conflict Control

V0.7 在 V0.6 Native CDC 与 Generic Keyset 的基础上，补齐 CDC 失败事务恢复和双向同步冲突决策的第一层生产能力。

### CDC Dead Letter Queue

- 托管 CDC Apply 失败时创建可恢复 DLQ 记录。
- DLQ 使用源端 Durable Position 生成稳定恢复上下文；失败位点不会在源端 ACK。
- 支持 Admin/DBA 从 API / Vue 精确 Replay。
- 已确认的相同 GTID/Binlog/LSN 再次提交会直接去重，避免“目标已提交但响应丢失”造成重复业务写入。
- DLQ row-image payload 使用现有 AES-256-GCM Master Key 单独加密；底层 state file / PostgreSQL Metadata Repository 不保存业务明文。
- Prometheus 新增 `qmigration_cdc_dlq_open`。

### CDC Conflict Control

新增任务参数：

```text
cdc_conflict_mode = SOURCE_WINS | LAST_WRITE_WINS
cdc_conflict_column = updated_at / version / ...
```

`SOURCE_WINS` 保持 V0.6 的幂等源端覆盖语义。

`LAST_WRITE_WINS` 对 INSERT/UPDATE：

1. 按映射后的主键在目标事务内 `SELECT ... FOR UPDATE`。
2. 比较源事件与目标当前行的版本列。
3. 源版本更新：Apply，并记录 `SOURCE_APPLIED`。
4. 目标版本更新或相同：跳过目标写入、继续推进源 CDC Checkpoint，并记录 `TARGET_KEPT`。
5. Conflict Record 只保存 Primary Key SHA-256 指纹，不保存业务主键明文。

版本比较支持 RFC3339/常见数据库时间格式、任意精度数值版本以及字符串回退比较。

DELETE 暂时保持幂等 Source-Wins；无 tombstone/version 的已删除行无法安全执行 LWW 比较。

### CDC UPDATE Primary-Key Move

修复 UPDATE 修改主键时旧目标行残留的问题。Native Apply 现在在同一个目标事务中：

```text
DELETE old mapped PK
        ↓
UPSERT after image with new PK
        ↓
COMMIT
```

任何步骤失败都会整体回滚，并且不会推进 CDC Checkpoint。

### API / Console

新增：

```text
GET  /api/v1/migrations/{id}/cdc/dlq
POST /api/v1/migrations/{id}/cdc/dlq/{dlq_id}/replay
GET  /api/v1/migrations/{id}/cdc/conflicts
```

Vue 任务页新增：

- CDC DLQ Tab
- CDC Conflict Tab
- `SOURCE_WINS / LAST_WRITE_WINS` 创建参数
- Conflict Version Column 输入

### Metadata Upgrade

- `007_v07_cdc_dlq.sql`
- `008_v07_conflict_policy.sql`

PostgreSQL Repository schema 也包含对应 `IF NOT EXISTS` 升级逻辑。

## 安全边界

- DLQ row image 仅 Admin / DBA 可在线读取与重放。
- Conflict Record 不保存主键明文。
- LAST_WRITE_WINS 依赖两端维护同一个可比较、单调更新的版本字段。
- LWW 不等价于通用 CRDT；多列独立 merge、业务语义冲突和无版本 DELETE 仍需要业务规则。

## 仍保留的边界

- MySQL binary JSON OPAQUE / Partial JSON Update 尚未 Native 解码。
- PostgreSQL pgoutput 不传递通用 DDL。
- Oracle LogMiner / SQL Server Native CDC 尚未完成。
- 无主键表仍使用 SeaTunnel/DataX 等外部 Engine。
- 当前环境没有 Docker，真实数据库 E2E suite 可编译但本机未执行容器测试。
- Vue production build 仍取决于 npm registry 可用性。
