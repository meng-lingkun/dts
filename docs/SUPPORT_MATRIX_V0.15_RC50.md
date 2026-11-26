# DTS 0.15.0-rc50 兼容性与验收范围

| 数据库 | 已有数据路径 | RC50 验收与使用限制 |
| --- | --- | --- |
| MySQL 8.4.7 / PostgreSQL 16.16、17.11 | 全量、Binlog/pgoutput CDC、事务目标围栏 | 两个 PostgreSQL 版本的隔离实例均通过双向全量、增删改同步和结果比较。MySQL 目标表需要 InnoDB。PostgreSQL 已验证选定表冻结和另一控制面实例核验。 |
| PolarDB MySQL / PolarDB PostgreSQL | 两种原生 Connector 提供对应协议和事务围栏 | 云产品的版本、权限、主备切换及长稳需要单独验收。 |
| MariaDB / PolarDB-X | 原生 MySQL 协议及相应 Binlog 读取 | RC50 的目标事务围栏仅接受 MySQL、PolarDB MySQL 两种类型；使用这些派生库作目标时须按执行模式确认。 |
| TiDB / OceanBase MySQL | TiCDC + Kafka 或独立 Binlog Service | 实验性。需要对应的 CDC 服务、正确的源端点及重启、保留期验证。 |
| openGauss / KingbaseES / GaussDB | PG 协议及各自逻辑解码插件 | 按产品开关启用 CDC。GaussDB 原生数据路径也需开关。需要逐版本检验解码、DDL 与恢复。 |
| Oracle / SQL Server / DB2 LUW / 达梦 | 有原生全量和各自的日志/CDC 实现 | 实验性。DB2 日志 Agent、达梦 SQL Provider 等依赖按源库和宿主版本配置。缺少本轮厂商真库认证。 |
| GBase 8a / 8s | 8a 有实验性全量和目标应用；8s 有 CSDK、Agent 和源 CDC 接口 | 8a 缺少合格的通用源 CDC，8s 需实际厂商 Provider 和对应版本的验证。 |

运行实例的 `GET /api/v1/connectors` 返回已启用能力和资格验证标记。割接前还需核实当前执行方向的原生水位比较、最终停写及目标提交保障。RC50 从 RC49 升级需要统一停旧进程，跨库 DDL 的不确定结果进入人工核查。

本轮双向数据测试覆盖固定夹具，底层数据库运行在同一台 Windows 宿主。生产主备切换、机房网络分区、10–40 TB 规模和 24–72 小时连续 CDC 另行验收。
