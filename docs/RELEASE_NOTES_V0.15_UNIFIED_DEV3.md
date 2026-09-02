# QMigration V0.15.0-unified-dev3 Release Notes

## 目标

继续把开源迁移工具的优秀设计融合进单一 QMigration Unified Engine，而不是恢复多引擎管理模式。本轮重点是：可审计的值转换策略，以及 Oracle/SQL Server 原生协议 Connector 的第一阶段。

## Transform Policy DSL

迁移任务新增持久化 `transform_rules`。规则在 Worker 的统一 Full Pipeline 中执行：

`Reader -> bounded channel -> Transform Policy -> built-in type transform -> Writer -> durable checkpoint`

首批动作：

- `TRIM`
- `LOWER`
- `UPPER`
- `EMPTY_TO_NULL`
- `NULL_TO_VALUE`
- `REPLACE_LITERAL`
- `ZERO_DATE_TO_NULL`
- `ZERO_DATE_TO_VALUE`
- `JSON_COMPACT`

规则可以按 source schema/table/column 精确匹配，也可留空 schema/table 作为受控通配。规则按声明顺序执行，不支持任意 SQL、脚本或代码执行。

MySQL zero-date 等无法无损转换的值仍默认 fail-safe；只有显式声明 `ZERO_DATE_TO_NULL` / `ZERO_DATE_TO_VALUE` 才允许改变语义。

## Oracle Native TNS Foundation

新增 QMigration 自研 Oracle Net/TNS CONNECT 协议层：

- 发送 TNS CONNECT packet
- 识别 ACCEPT / REFUSE / REDIRECT
- 以 Oracle service name 构造 CONNECT_DATA
- 默认只声明 `protocol-probe`

本版本尚未声明 Oracle `metadata/full-read/full-write/cdc-read`。Oracle 认证、数据字典、批量数据通道以及 Redo/LogMiner Reader 仍待后续 Native 实现。

## SQL Server Native TDS Foundation

新增 QMigration 自研 TDS 层：

- PRELOGIN
- LOGIN7
- SQL authentication password obfuscation
- SQL Batch
- LOGINACK / ERROR / INFO / ENVCHANGE / DONE token
- COLMETADATA / ROW
- NVARCHAR / NCHAR / VARBINARY / BINARY
- PLP (`nvarchar(max)` / `varbinary(max)`) 基础解码

默认模式只执行认证前 PRELOGIN probe，不发送数据库凭据。

当显式设置：

```bash
QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE=1
```

Connector 才会开放实验性 `metadata/full-read/full-write/schema-create/cdc-apply/...` target-side 能力，并进入 LOGIN7 + SQL Batch 数据面。SQL Server source CDC 仍未开放。

如果 SQL Server PRELOGIN 要求 TLS (`ENCRYPT_ON/ENCRYPT_REQ`)，当前实现 fail closed，不发送 LOGIN7；Native TDS TLS 仍是下一阶段工作。

## Worker

实验开关开启时 Worker 额外声明：

`qmigration:sqlserver-full-experimental`

默认 Worker 不声明该能力。

## Schema Engine

Universal type mapper 新增 Oracle / SQL Server target type rendering，为后续 Native Full Connector 做准备。

## 元数据

新增 migration `019_v015_transform_policy.sql`：

- `migration_tasks.transform_rules_json`
- metadata schema version -> `0.15.0-unified-dev3`

## 验证边界

- Go unit/integration(fake-wire) tests: required
- `go vet ./...`: required
- SQL Server: fake TDS wire qualification only; no real SQL Server E2E in current environment
- Oracle: fake TNS listener qualification only; no authentication/full/Redo qualification yet
- Frontend production build depends on npm registry availability and must not be reported PASS unless actually completed
