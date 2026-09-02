# QMigration V0.15.0-unified-dev2

## 目标

继续把“融合开源工具思想”落实为一个单一自研迁移内核，而不是多运行时管理平台。

## 新增

1. **Unified Connector Capability SPI**
   - Connector 显式声明 Full Read/Write、Keyset、Partition、Schema、CDC Read/Apply、Topology、Runtime Load 等能力。
   - Planner/Precheck/Compatibility Assessment 按能力判断，而不是按第三方 Engine 路由。

2. **Unified Transform Runtime**
   - Full Load 的 bounded pipeline 中加入编译式 Value Transform Plan。
   - Boolean/JSON/zero-date 等跨库差异有明确、可测试的安全行为。

3. **Unified Native CDC Runtime**
   - MySQL Binlog 与 PostgreSQL pgoutput 共用 Reader -> Apply -> ACK 生命周期。
   - 目标 Apply/Checkpoint 失败时绝不确认源端位点。

4. **openGauss / Kingbase Native Full Load**
   - 直接复用 QMigration 自研 PostgreSQL frontend/backend Wire Protocol。
   - 不依赖 JDBC/DataX/SeaTunnel/Flink。
   - 当前不声明 pgoutput CDC 能力。

5. **Connector Diagnostics API**
   - `GET /api/v1/connectors` 返回当前 Native Connector 能力矩阵。

## 兼容性

- 旧 `engine` 字段继续归一化为 `qmigration`。
- Oracle/SQL Server/DB2/DM/GaussDB/GBase 在 Native Connector 完成前仍会 fail-safe 拒绝迁移，不回退第三方 runtime。
