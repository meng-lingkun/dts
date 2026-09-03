# QMigration 架构评估与缺失功能

评估基线：`0.15.0-rc49`  
评估日期：2026-09-02

## 1. 结论

整体架构方向合理，但生产就绪度是“有条件成立”。

数据面在正确性设计上较成熟：统一 Connector SPI、能力门禁、有界 Pipeline、写后 Checkpoint、CDC 事务顺序、加密 Durable Spool、DLQ/Commit Unknown 和 Watermark Validation 都是正确方向。后端、Worker、Repository 和部署形态也已经形成完整产品骨架。

本轮已修复默认安全、版本漂移、基础 CI/可复现构建、跨平台编译和 Precheck/Validation 启动恢复。主要剩余问题是 Cutover/Rollback 尚未持久 Saga 化、元数据访问并发模型、超大模块、企业身份/审批、真实数据库资格验证和前端产品化。当前仍适合受控 RC 和经过专项资格验证的迁移项目，不应把代码中存在的实验能力等同于生产支持。

## 2. 评估维度

| 维度 | 判断 | 说明 |
|---|---|---|
| 领域与分层 | 基本合理 | API、Service、SPI、Repository、Worker 边界明确 |
| 数据正确性 | 较好 | Apply-before-checkpoint、Lease、事务 CDC、Spool、Validation Barrier |
| 扩展性 | 中等 | Connector SPI 良好，但核心 Service/Repository 接口过大 |
| 高可用 | 部分合理 | Worker Lease 完整；Precheck/Validation 有持久租约和恢复，Cutover/Rollback 仍缺 Saga |
| 安全 | 默认已加固 | 生产启动校验、强制认证/Secret、非 root 容器已补；企业身份和网络策略仍缺 |
| 前端 | 可用但偏工程台 | 功能覆盖广，状态管理、表单体验、测试和错误恢复不足 |
| 可观测性 | 较好 | 指标丰富、Readiness 合理；通知路由和追踪不足 |
| 发布与可复现 | 基础已建立 | Linux CI、Lockfile、版本检查和生产构建已补；E2E/镜像供应链门禁仍缺 |
| 文档 | 数量多但事实源混乱 | 大量版本碎片，当前口径相互冲突 |

## 3. 做得合理的部分

### 3.1 统一引擎和 Connector 能力门禁

用户不再选择第三方执行引擎，避免了多个运行时在 Checkpoint、失败语义和运维方式上的割裂。Connector Descriptor 显式声明能力、成熟度和资格验证要求，未支持能力在启动前失败关闭。

### 3.2 全量数据正确性

Reader 允许有限预取，Writer 成功后才提交 Cursor。Worker 崩溃可能造成幂等重放，但不会因预取而跳过数据。这比简单的“读取即记进度”安全。

### 3.3 CDC Spool 和事务顺序

Full + CDC 使用 Durable Spool 缩短对源端日志保留窗口的要求，并明确区分源端 ACK 与目标 Apply Checkpoint。File/S3 都在应用层加密，Spool 满时通过不 ACK 源端实现安全反压。

### 3.4 Worker Lease 与能力调度

Chunk 和 Engine Job 都有 Lease、Owner 校验、续租和重领。Worker 上报 CPU、内存、网络、能力和标签，支持拓扑亲和与压力控制。

### 3.5 校验和验收证据

校验不仅有 Row/Checksum，还引入稳定 Watermark、不可变 Archive、JSON/HTML/PDF、哈希、Ed25519、公钥轮换/撤销、TSA 和 WORM，适合审计要求较高的迁移场景。

## 4. 架构问题与风险

### P0：上线前必须处理

#### 4.1 Compose 默认开放管理员模式（已修复）

Compose 现在启用 `QMIGRATION_PRODUCTION=true` 和强制认证，数据库、加密和 Token Secret 必须由私有 env 文件提供，API/Web 仅绑定回环地址。Bootstrap Admin 使用明确的初始默认值并要求首次登录后轮换。Server 会拒绝 Open Mode、短/示例/复用内部 Secret、通配 CORS 和非 PostgreSQL 生产仓库。镜像以非 root、只读根文件系统（Kubernetes）和最小 Capability 运行。

剩余：Compose 仍是单机模板；TLS、Secret Manager、NetworkPolicy 和外部身份应由生产平台配置。

#### 4.2 控制面缺少启动恢复 Reconciler（部分修复）

新增 PostgreSQL/Memory 持久控制面租约、心跳续租和 30 秒恢复巡检。`PRECHECKING` 可安全重启，`VALIDATING` 会清理本轮结果后重跑；`PREPARING` 可能已修改目标 Schema，因此租约过期后失败关闭并要求人工核查。

剩余：Cutover/Rollback 仍需显式 Saga/操作日志和幂等步骤；目标 Prepare 需设计可证明的补偿/续跑语义后才能自动重放。

#### 4.3 发布基线不一致（已修复当前基线）

根 `VERSION`、Go、npm、Kubernetes 镜像、Schema Marker、README 和 Web 显示已统一为 `0.15.0-rc49`，CI 执行 `scripts/check-version.mjs`。旧 Release Notes 的历史版本号按档案保留。

剩余：应继续收敛重复的历史架构/支持矩阵入口，明确“当前事实文档”和“历史档案”。

#### 4.4 构建基线（已修复基础门禁）

File Spool 已拆为 Unix/Windows 实现，GBase 8s 测试按平台语义处理，SQL Server Fake TDS 会完整读取请求后响应。已提交 npm Lockfile 和 Ubuntu CI；本次 `go test ./...`、`go vet ./...`、Linux/amd64 `go build ./...`、`npm ci`、`vue-tsc --noEmit`、Vite Build 均通过。

剩余：Race、前端单元/E2E、真实 PostgreSQL/数据库集成、容器构建、SBOM 和漏洞扫描应进入 Release Gate。

### P1：近期应处理

#### 4.5 核心应用服务过度集中

`internal/migration/service.go` 约 7525 行，同时处理创建、规划、调度、流控、CDC、校验、Schema、割接和回切；`internal/api/server.go` 约 2345 行；多个 Connector 也超过千行。

影响：变更耦合、Code Review 困难、并行开发冲突、局部不变量难以验证。

建议：按 Planning、FullLoad、CDC、Validation、Cutover、Performance Control 拆分应用服务；按资源拆 API Handler；Connector 内再分 Transport、Metadata、Reader、Writer、CDC。

#### 4.6 Repository 接口过大

单一 Repository 接口覆盖全部资源，Memory、PostgreSQL、Secure、File/S3 装饰器都要实现或转发大量方法。

影响：接口隔离不足，新增资源修改面大，Mock 和装饰器维护成本高。

建议：拆分 DataSourceStore、MigrationStore、ChunkLeaseStore、CDCStore、ValidationStore、IAMStore、AuditStore，并在应用层按需组合。

#### 4.7 PostgreSQL 元数据访问串行化风险

PostgreSQL Repository 复用自研 PostgreSQL Connector；Connector 持有一个 Client，并在 `ExecSQL/QuerySQL` 外层加互斥锁。一个 Server 实例内的元数据请求因此可能被单连接串行化。

影响：高并发 Worker 心跳、Chunk Checkpoint、指标抓取、日志和 CDC 状态写入会竞争同一连接；长查询可能拖慢热路径。

建议：元数据仓库使用成熟驱动和连接池，热路径使用参数化语句/事务，分离监控查询与调度写入，并做规模压测。

#### 4.8 两套 Schema 迁移事实源

Server 自动应用嵌入 `schema.sql`，运维脚本又顺序应用 `backend/migrations/*.sql`。二者都写 Schema Version。

影响：长期维护可能出现最终 Schema 不一致、升级路径未经验证或 Marker 被提前更新。

建议：选择一个迁移引擎/事实源；启动时只验证或受控迁移；每次 Release 从 Migration 自动生成并比对基线 Schema。

#### 4.9 API 契约和后台任务缺少统一规范

API 使用手写 `net/http` Route，缺少 OpenAPI、统一分页/过滤、请求幂等键、异步操作 Resource 和标准错误码。部分操作启动 Goroutine 后立即返回，客户端只能通过状态猜测进度。

建议：生成 OpenAPI；对 Create/Start/Cutover/Archive 引入 Idempotency Key 和 Operation API；统一 Cursor Pagination、错误 Code 和 Correlation ID。

#### 4.10 前端架构未匹配业务复杂度

`Migrations.vue` 将创建表单、任务列表、实时更新和十多个详情域集中在一个组件；高级配置依赖手写 JSON；WebSocket 异常被静默忽略且没有指数退避重连；Pinia 未实际使用。

建议：拆页面/组件和 composable；集中 Auth/Permission/Task Store；增加 Schema/Table 可视化选择、JSON Schema 校验、断线状态和重连策略。

### P2：中期改进

#### 4.11 安全纵深仍不足

当前没有登录限流/锁定、MFA、OIDC/SAML/LDAP、服务端 Session 撤销、细粒度数据范围权限、通用 Secret Manager/KMS 数据源凭据集成。开发模式仍可配置 CORS `*`，Metrics 默认无认证，前端静态 Token 存在 LocalStorage；生产校验已禁止通配 CORS。

建议：增加生产安全 Profile、OIDC、MFA、Token Hash/轮换、登录防护、Metrics 独立监听/鉴权、CSP/HSTS/安全响应头和 Secret Provider SPI。

#### 4.12 缺少分布式追踪和统一日志上下文

日志主要是标准文本，虽有 Task Log 和 Prometheus，但没有 Trace/Span、Request ID、结构化日志和跨 Server/Worker/CDC Reader 关联。

建议：引入 OpenTelemetry，统一 `request_id/task_id/chunk_id/job_id/worker_id`，输出结构化日志。

#### 4.13 Kubernetes 多节点与生产加固（部分修复）

已移除清单内的运行 Secret，新增跨主机 Topology Spread、Pod Anti-Affinity、Server/Worker/Web PDB、可配置副本/HPA、全节点离线镜像 DaemonSet 预检、外部 HA PostgreSQL、LoadBalancer 和 Ingress/TLS 安装入口。Pod 已启用非 root、Seccomp、只读根文件系统、禁用 ServiceAccount Token、Drop ALL Capabilities 和临时目录挂载。

剩余：仍缺 Helm/Kustomize Profile、NetworkPolicy、External Secrets/CSI Secret Store、自动证书管理和真实多节点故障演练。当前安装器使用 RWX File Spool；大规模生产可改用 S3 Spool，但尚未提供完整 Kubernetes Overlay。

## 5. 缺失功能清单

以下是相对于完整企业迁移平台仍缺少或不完整的功能，不代表所有功能都应立即实现。

### 5.1 用户与安全

- OIDC/OAuth2、SAML、LDAP/AD 登录；
- MFA、密码策略、登录失败锁定和验证码/限流；
- 用户自助修改密码、Session 列表和强制注销；
- API Token 创建、Hash 存储、到期、轮换、撤销和使用审计；
- 项目/租户/数据源级权限，而非只有全局四角色；
- 数据源凭据接入 Vault/KMS/云 Secret Manager；
- 审批流和双人复核，尤其是 Cutover、Rollback、DLQ Commit 决策。

### 5.2 迁移任务产品能力

- 任务配置编辑、克隆、导入/导出、归档和删除；
- 可视化 Schema/Table/Column 选择和映射向导；
- 对象范围预览、影响分析和迁移计划 Diff；
- 任务模板、批量创建、标签、文件夹和项目空间；
- 定时启动、维护窗口、依赖编排和批量暂停；
- Dry Run、容量估算和源/目标资源基线；
- 可视化 Transform Builder 和样例数据预览；
- 失败 Chunk 的选择性重试、跳过审批和批量处置；
- Cutover Checklist、审批记录、业务冻结确认和自动化钩子；
- 更明确的 Cancel 后清理/保留策略和任务归档策略。

### 5.3 数据与数据库能力

- 多数非 MySQL/PostgreSQL 数据库仍是资格验证门禁或 Full Only；
- 存储过程、函数、Trigger 等复杂对象多为人工处理；
- 跨数据库 DDL 自动转换仍保守；
- 更完整的数据类型、空间、向量、LOB、加密列和厂商特性矩阵；
- 在线 Schema Change 与长事务的跨厂商一致处理；
- 数据脱敏、Tokenization、外部字典/表达式转换；
- 全库/跨库一致性快照编排和多任务事务边界；
- Schema Drift 持续检测和自动兼容策略。

### 5.4 Worker 与调度

- Worker Drain、Disable、Maintenance、Cordon/Uncordon API；
- Worker 证书身份或短期凭证，替代全局共享 Token；
- 管理台远程升级、版本/镜像一致性和滚动维护；
- 资源配额、租户公平调度、优先级和抢占；
- Worker 磁盘、进程、CDC Binary 完整性和版本健康检查；
- Cutover/Rollback 的持久 Saga、操作审计和幂等恢复。

### 5.5 监控、告警与审计

- 告警规则管理、通知渠道、Webhook、邮件、短信和值班平台；
- 告警静默、抑制、去重、升级和恢复通知；
- Dashboard 自定义、历史趋势、容量预测和跨任务对比；
- OpenTelemetry Trace 和结构化集中日志；
- 审计筛选、分页、导出、不可篡改归档和外部 SIEM；
- SLO/SLA 报表与迁移后复盘报告。

### 5.6 API、集成与生态

- OpenAPI/Swagger 和生成式客户端 SDK；
- Webhook/Event Bus，通知任务状态和审批事件；
- Terraform/Operator/Helm 等声明式管理；
- Idempotency Key、Operation API、统一 Pagination 和 Error Code；
- GitOps 配置和 Secret 引用；
- 工单、CMDB、审批平台和值班系统集成。

### 5.7 前端工程

- 单元测试、组件测试和 Playwright/Cypress E2E；
- ESLint、Prettier 和持续 Bundle 预算分析；
- 统一会话/权限 Store 和 Route Guard；
- WebSocket 重连、退避、离线提示和事件补偿；
- 表单 Schema 校验、草稿、离开保护和高风险操作二次复核标准化；
- 国际化、无障碍、响应式布局和大数据列表虚拟化。

### 5.8 交付与运维

- CD、SBOM、签名镜像、依赖/容器漏洞扫描；
- Helm Chart/Kustomize Overlay 和生产安全基线；
- 自动升级/回滚验证、Migration Compatibility 测试；
- 多区域灾备和 Metadata + Spool 一致性备份编排；
- Linux 容器/发行版矩阵和厂商动态库兼容性验证。

## 6. 推荐整改路线

### 第一阶段：可信发布基线（本轮已完成基础项）

1. 已修复 Compose 安全默认值并阻止示例 Secret 用于生产；
2. 已统一版本、镜像 Tag、Web 页头和 Schema Marker；
3. 已增加 Linux CI、前端 Lockfile、类型检查和生产构建；
4. 已将 File Spool 磁盘统计拆成 Unix/Windows 平台文件；
5. 已增加控制面租约、恢复巡检和人工处置说明；
6. 仍需归档/标记过时的历史事实文档，并增加真实数据库 Release Evidence。

### 第二阶段：一至两个月，补控制面韧性

1. 将现有 Operation Lease 扩展为 Cutover/Rollback 幂等 Saga；
2. 将 Prepare 拆为持久、可补偿步骤；
3. 元数据 Repository 引入连接池并完成压力测试；
4. 统一 Migration 事实源；
5. 拆分 Migration Service/API Handler；
6. 增加 OpenAPI、Idempotency Key、Pagination 和 Error Code。

### 第三阶段：三至六个月，产品化和企业能力

1. 可视化任务向导、对象选择、模板和审批；
2. OIDC/MFA、Token 生命周期和 Secret Provider；
3. Worker Drain/升级/证书身份；
4. 告警通知、Trace、结构化日志和 SIEM；
5. Helm/Kustomize 生产 Profile、供应链安全和灾备演练；
6. 按真实客户数据库版本推进实验 Connector 的资格验证，而不是一次性放开。

## 7. 本次检查证据

本次执行了项目目录盘点、关键代码静态阅读、Route/前端 API 对照、配置和部署清单检查，并在 Windows 工作区运行：

```text
go test ./...                     PASS（Windows 开发主机）
go vet ./...                      PASS（Windows 开发主机）
GOOS=linux GOARCH=amd64 go build ./...  PASS
npm ci                            PASS
npm run build                     PASS（vue-tsc + Vite）
node scripts/check-version.mjs    PASS
```

这证明基础代码和前端构建成立，但不等于真实数据库/容器生产资格验证。正式发布仍需在 Linux CI 执行测试，并补真实 PostgreSQL、源/目标数据库、镜像、安全扫描和长稳故障注入证据。
