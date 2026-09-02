# QMigration V0.8.0 Release Notes

## 主题：Schema Object Migration / Sequence Cutover Safety

V0.8 将非表对象从只读兼容性评估推进为可确认执行的 Schema Object Plan。

### Schema Object Plan

支持发现并分类：

- VIEW
- SEQUENCE
- TRIGGER
- FUNCTION
- PROCEDURE

动作：

- `APPLY_SAFE`：同数据库家族、Schema/Table/Column identity mapping 的 View。
- `SYNC_SEQUENCE`：PostgreSQL-family Sequence 创建与状态同步。
- `SKIP_EXISTING`：目标已有同名 View，默认不覆盖。
- `MANUAL`：Trigger / Routine / 异构 View / 模糊 Schema Mapping。

### Sequence Cutover Safety

PostgreSQL Sequence 同步保存 `sequence_synced_at`。FULL_AND_INCREMENTAL 进入 `READY_FOR_CUTOVER` 前，如果存在可自动同步 Sequence，则最近一次同步必须在允许窗口内。

默认：

```text
QMIGRATION_SEQUENCE_SYNC_MAX_AGE_SECONDS=60
```

源端仍有写入时可以重复执行 Sequence Sync，推荐在业务停写、CDC 追平后立即同步并进入割接。

### API / RBAC

```text
GET  /api/v1/migrations/{id}/schema-objects/plan
POST /api/v1/migrations/{id}/schema-objects/apply
```

Apply 要求 `confirm=true`，仅 Admin/DBA 可执行。所有执行通过现有 Audit Event 记录。

### 安全边界

- Trigger / Function / Procedure 不自动执行。
- 异构 View 不自动翻译。
- PostgreSQL Sequence 跨 Schema 自动重写暂不启用。
- SERIAL/IDENTITY ownership/default 绑定仍需人工确认。
- Vue production build 受当前 npm registry 可达性限制。
