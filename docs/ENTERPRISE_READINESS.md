# DTS 企业就绪度复查与整改记录

> 本文为 rc49 复查基线。用户提出的四项后续整改已进入 rc50；最新实现、验证结果及仍未完成的验收见 [RC50 实施记录](ENTERPRISE_HARDENING_RC50.md)。下文保留历史发现，不代表 rc50 所有缺口仍未实现。

首次复查：2026-10-29；续查：2026-10-30（第 6 节）。基线：提交 `89ea243`，版本 `0.15.0-rc49`，以及当前工作区补丁。保留了续查开始前已有的未提交改动。版本号未提升，旧 `dist/` 安装包未重新制作。

## 1. 当前做到哪一步

项目已达到“功能较完整、需要专项验收的 RC”阶段。已有 API 控制面、分布式 Worker、Vue 控制台、PostgreSQL 元数据、统一 Connector/Engine、分片全量迁移、CDC、加密 File/S3 Spool、死信与提交结果不确定处置、校验/报告归档以及 Kubernetes 离线部署。

代码和测试体现了写后 Checkpoint、有界 Pipeline、Worker Lease、事务 CDC 和恢复设计。当前不能据此认定所有数据库、双 Server 并发和故障场景均已达到生产要求；普通单元测试通过也不构成真实数据库资格验证。

| 范围 | 当前判断 | 依据与边界 |
| --- | --- | --- |
| MySQL/PostgreSQL 主路径 | 有原生 Full/CDC 实现与测试 | 仍须按具体数据库版本、权限、TLS、数据类型、负载验收 |
| Oracle/SQL Server/DB2/达梦/GBase 等 | 有不同程度的实验数据面 | 保持 Connector Descriptor 的能力门禁，不能把 Probe 成功解释为支持迁移 |
| 控制面恢复 | Precheck/Validation 可重跑；Prepare 和最终割接中断失败关闭 | Cutover/Rollback 尚无可自动恢复的持久 Saga |
| 高可用 | 有分布式租约与多副本清单 | 还缺跨副本事务边界、物理故障演练及规模压测证据 |
| 安全与运维 | 有 RBAC、加密、审计及离线部署 | 企业身份、审批、Secret Provider、灾备与供应链门禁尚不完整 |

## 2. 本次已修复的问题

### P0：默认部署的控制面租约被 Spool 包装层隐藏

文档要求生产通过持久租约协调 Precheck/Validation，实际启动顺序是 `Secure(File/S3(PostgreSQL))`。File/S3 原来只嵌入核心 Repository，没有转发可选的 `ControlOperationLeaser`；Secure 因此返回“不支持控制面租约”，可能导致任务启动/恢复失败。

已补齐 File/S3 的 Acquire/Renew/Release 转发及编译断言。新增组合测试首先在旧实现上复现 File、S3 两个失败，再验证获取、互斥、续租和释放确实到达底层仓库。

实现：`backend/internal/repository/{spoolfile,spools3}/control_leases.go`。回归：`backend/internal/repository/control_leases_test.go`。

### P0：回切过早成功、尾部缓冲未排空、死信没有最终门禁

原 Rollback 连续写入 RUNNING/ROLLED_BACK，最后才尝试停止 reverse CDC，且忽略停止错误。READY 后新产生的 Spool、失败 Reader 或未决 COMMIT 都可能被“完成”状态掩盖。

现在先请求停止、等待 Reader 停止、排空对应方向已持久化的 Spool，再写最终状态。等待可被请求取消，失败时保留 READY 供处置；Cutover 和 Rollback 在准备及最终排空时都检查该方向未解决死信。停止结果中明确上报的错误不能再被 StopRequested 状态覆盖为成功。

边界：这证明 DTS 已保留数据的收尾顺序，**没有实现业务写入冻结和源端最终水位追平证明**。割接/回切前仍须由操作方冻结相应写入，见第 3 节。

实现：`backend/internal/migration/cutover.go`、`service.go`。回归：`cutover_test.go`。

### P0：暂停 CDC 时过早删除复制槽

原 CompleteEngineJob 对任何 StopRequested 任务执行源 Checkpoint 清理，暂停也会触发，可能破坏 PostgreSQL 恢复所需的 WAL 保留。

现在停止进程保留 Checkpoint，只在割接/回切成功持久化后清理对应方向资源；清理失败记录任务警告。暂停 Full+CDC 的全量或校验阶段允许继续捕获到 Spool，目标 Apply 保持冻结。暂停期间应监控源 WAL/Spool 容量。

回归覆盖暂停、恢复、成功割接的完整资源生命周期，以及暂停期间只缓冲不写目标。

### P1：控制面并发、租约过期和中断状态

- Cutover、PrepareRollback、Rollback 增加任务级持久互斥租约及续租。
- Start 取得租约后重新读取状态，防止拿旧 CREATED 快照覆盖已推进的任务。
- Memory/PostgreSQL 不再允许已过期 Owner 通过 Renew 复活旧租约。
- 结束操作时等待续租协程退出，再释放租约。
- 恢复巡检对中断的 CUTOVER_RUNNING/ROLLBACK_RUNNING 失败关闭，提示人工核查；仍在活跃租约内的操作不受影响。
- 恢复处理取得租约后复查状态，避免使用过期列表快照覆盖更新后的状态。
- 托管 CDC 拒绝在已完成、取消、失败或最终割接阶段继续确认并缓冲新事件。

这些是保守恢复措施，**不等于数据库副作用已具备完整 fencing 或 Saga 补偿语义**。

### P1：密码重置后旧会话仍有效，登录可能覆盖并发改密

Session 原来只校验用户是否启用、角色是否相同，改密后旧 Token 可继续使用。现在 Session 用 HMAC 绑定持久化密码哈希；后续请求通过当前账号状态校验，跨副本/重启均不依赖内存撤销表。旧格式 Session 要求重新登录，静态自动化 API Token 仍按独立配置管理。

WebSocket 在每次推送周期重新认证，改密、禁用、过期后关闭已有连接。登录记录改用仅更新 `last_login_at` 的仓库方法，避免读出旧用户后整行回写，覆盖管理员刚修改的密码、角色或启用状态。

回归覆盖改密后 REST/WS 拒绝、已有真实 WebSocket 关闭、SessionManager 重建、以及登录与改密/降权交错场景。

### P1：新安装使用公开固定管理员密码

离线安装器改为生成独立随机管理员密码，立即写入权限 600 的 `INITIAL_ADMIN_CREDENTIALS.txt`，即使随后 rollout 失败也能保留凭据；该文件已加入 Git 忽略规则。也可使用 `DTS_ADMIN_PASSWORD` 自行指定。

生产 Bootstrap 对新建管理员拒绝短密码、示例值和内部 Secret 复用；示例配置不再提供可用的固定密码。重复部署保留既有 Secret/账号，不自动重置已有密码。**历史部署仍需主动轮换原默认密码。**

## 3. 距离企业生产仍未完成的工作

以下是后续实际发布门禁，优先顺序高于扩展更多实验数据库。

| 优先级 | 未完成项 | 可验收的完成条件 |
| --- | --- | --- |
| P0 | 最终割接水位和写入冻结 | 持久化冻结确认/审批、源端最终位点、目标已应用位点；证明两者相等后才允许成功；覆盖停止过程中新增事务、长事务和外部 CDC |
| P0 | 跨 Server 的 CDC Apply/Validation fencing | 10-30 已统一各 CDC 入口的持久租约、续期及提交前检查，补齐两个 Service 共享仓库的争用测试；仍需目标库侧原子 fencing、元数据 CAS/事务及物理故障验证，证明租约检查与 COMMIT 之间发生网络分区时旧执行者不能提交或覆盖新状态 |
| P0 | Cutover/Rollback/Prepare 的持久 Saga | 每步保存输入、幂等键、执行结果与副作用证据；任意一步 kill/restart 能自动续跑或明确转人工，不能猜测 DDL/提交结果 |
| P0 | 真库及多节点发布证据 | MySQL/PostgreSQL 首先完成 TLS、类型、事务、重连、崩溃、对账、升级与长稳；再按厂商版本逐个解锁实验 Connector |
| P1 | 元数据连接池与原子更新 | 当前自研 PostgreSQL Connector 单连接互斥；引入连接池、参数化 SQL、CAS/事务及负载测试，避免心跳/Checkpoint 被长查询阻塞 |
| P1 | Schema 升级事实源统一 | 统一嵌入 schema.sql 与 migrations/*.sql，验证空库/历史版本升级、失败重试、Marker 更新时机和回滚兼容 |
| P1 | 企业身份与权限 | OIDC/LDAP/MFA、登录限流、Session 列表/单会话撤销、自助改密、项目/数据源级权限、割接/DLQ 双人审批及 API Token 生命周期 |
| P1 | 控制 API 契约 | OpenAPI、请求幂等键、Operation Resource、统一错误码与分页；Pause/Resume/Validate 等控制面变更也需统一原子协调 |
| P1 | 生产运维交付 | NetworkPolicy、Secret Manager、镜像签名/SBOM/扫描、Metadata+Spool 一致性备份、恢复与滚动升级演练 |
| P2 | 控制台产品化 | 拆分 Migrations 大组件；对象/映射向导、表单校验、断线恢复、组件测试与浏览器 E2E；当前打包仍有大 Chunk 提示 |
| P2 | 规模可观测性 | 告警通知与抑制、结构化日志/Trace、SLO、容量基线和多租户公平调度 |

## 4. 本次验证与限制

Windows 开发主机，Go `1.26.2`；未连接用户源库/目标库，未执行真实迁移或改写外部数据库。

| 检查 | 结果/含义 |
| --- | --- |
| `go test ./...` | 通过；普通测试集，不包含 integration build tag |
| 新增迁移/仓库/认证回归 | 通过；包含 File/S3 生产包装顺序、故障门禁、暂停复制槽生命周期和真实本地 WebSocket |
| `go vet ./...` | 通过 |
| `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` | 通过；交叉编译，不是 Linux 运行验证 |
| `go test -tags integration ./integration -run '^$'` | 通过；仅编译集成测试，未执行真库用例 |
| `npm run build` | 通过，含 vue-tsc；仍有大 Chunk 警告 |
| `node scripts/check-version.mjs` | 通过 |
| `sh scripts/test-kubernetes-lifecycle.sh` | Git Bash 下 Mock 验证通过；含随机管理员密码、rollout 失败仍保留凭据、短密码拒绝；不是真实集群部署 |
| `go test -race` | 本机 CGO 关闭且无可用 C 编译器，未运行成功；已新增 Ubuntu CI Race Job，结果须以远端 CI 为准 |
| 真库 Integration/资格验证、Docker/Kubernetes 实机、长稳/故障演练 | 本轮未执行，不能标记已完成 |

## 5. 使用这批补丁

本次仅修改源码、测试、CI 与文档，不修改元数据 Schema，不自动连接或升级已有部署。发布前需重新制作并验证镜像/安装包。升级后浏览器原有 Session 需要重新登录；自动化静态 Token 不因用户改密而轮换。

操作过程中若收尾失败，先核查 Reader 状态、Pending Spool、DLQ、最终位点和源复制槽，再恢复任务。不要通过手改 FINISHED/ROLLED_BACK、删除 Pending Spool 或清理仍需恢复的复制槽来绕过门禁。

## 6. 2026-10-30 续查与修复

### 6.1 已修复的具体缺陷

| 缺陷 | 触发方式与影响 | 当前行为 |
| --- | --- | --- |
| CDC 写入和校验只共享进程内锁 | 两个 Server 同时直接 Apply、Worker Apply、重放或捕获校验屏障，可能并发写目标或使屏障失真 | 同一任务、方向统一使用本地互斥和 `cdc_spool_drain_leases`；每次执行有独立 Owner，持续续期，COMMIT/Checkpoint 前复核；过期租约不能续活 |
| 直接 Apply 越过历史 Spool、OPEN 死信 | 新事件在更早保留的事务之前写入；随后重放旧值覆盖新值 | 直接 Apply 先按序排空 Spool；未解决的早期死信阻止后续事务，原事务重试或显式重放可恢复；RESOLVED 历史死信禁止再次重放 |
| Checkpoint 读取错误被忽略 | 元数据查询失败仍写目标，或 Worker 恢复悄悄回到初始位置 | Apply 和恢复均失败关闭；区分空 Spool 的 `(nil, nil)` 与真实读取/解密/对象存储错误 |
| 最新位点查询受历史条数限制 | 前向位点被 50/200 条反向记录挤出；空方向旧记录无法正确去重 | 新增按任务、方向查询最新位点的仓库方法，不依赖历史页大小；兼容旧空方向为 forward |
| 去重没有包含 Resource | 不同流的相同位置字符串被误判为已提交 | 去重同时核对位置类型、值和 Resource，并兼容默认 EVENT_BRIDGE 类型 |
| 割接保存旧任务快照 | 排空尾部事务更新 Lag 后，最终状态又写回排空前的数据 | 收尾在 CDC 门禁内重读任务，再检查空 Spool/DLQ、更新最终状态 |
| 争用被当作普通坏请求 | 调用方无法区分并发冲突与请求参数错误 | CDC 租约争用返回 HTTP 409 和 `Retry-After: 1`，不推进源位点 |

入口与租约逻辑位于 `backend/internal/migration/cdc_execution.go`；Memory/PostgreSQL 及 Secure、File、S3 包装层均保留续租能力。沿用现有表，无新增 Schema 或外部依赖。

### 6.2 验证证据

- 续查前 `go test ./...` 已通过；修改后完整后端测试与 `go vet ./...` 通过。
- 新增回归覆盖双 Service 争用的直接/托管 Apply、重放、提交确认、进度上报、校验及 READY 入口；COMMIT 前租约丢失时无提交、无 Checkpoint，并保留恢复证据。
- 覆盖读失败不触碰目标、不静默回退；250 条反向记录后的前向去重；Resource 隔离；Spool 顺序；OPEN 死信门禁；已解决死信禁止重放；最终 Lag 不被旧快照覆盖；本地等待取消；File/S3/Secure 组合；持久 Memory 租约重启恢复。
- 新增 HTTP 回归验证 409、重试提示和 Checkpoint 不变。
- `npm run build`（含 vue-tsc）和版本一致性检查通过；前端仍有超过 500 kB 的 Chunk 提示。
- Linux/amd64、CGO 关闭的 `go build ./...` 通过；全部 integration-tag 测试编译通过（`go test -tags integration ./... -run '^$'`），均不构成真库运行验证。
- 新增 PostgreSQL 真库测试 `cdc_execution_integration_test.go` 及 CI `metadata-integration` Job：独立临时 Schema、两条连接、重复 Bootstrap、跨连接互斥、过期拒绝续租、旧 Owner 释放隔离和按方向位点查询。本机无测试 PostgreSQL，测试已编译但明确 SKIP；远端 CI 尚未运行，不能记为真库通过。
- 本机 `CGO_ENABLED=0` 且无 gcc/clang，未运行本地 Race；由 Ubuntu CI 执行。未连接用户源库/目标库，未执行实际迁移或部署。

### 6.3 升级与仍未关闭的边界

`DTS_CDC_SPOOL_DRAIN_LEASE_SECONDS` 默认 300 秒，限制在 30–3600 秒，每 TTL/3 续期。争用请求应保留源事务、退避重试；不能把 HTTP 409 当作成功并 ACK。租约丢失后的 COMMIT_UNCERTAIN 仍须依靠目标库证据处置。

此补丁需要所有 Server 使用相同实现。旧 Server 的直接 Apply 不经过新增门禁，因此**不能把新旧二进制混跑视为受保护的滚动升级**；应暂停迁移/桥接写入，停止旧 Server/Worker，更新全部实例后再恢复。

统一租约降低正常多副本并发风险，但不是目标库原子 fencing。仍未实现：业务写入冻结及最终水位追平证明、目标库提交 fencing、全量 Worker 的完整代际隔离、所有元数据状态更新的 CAS、割接/回切持久 Saga，以及真实故障/长稳资格验证。DDL 或非事务目标的副作用仍须专项验收。当前结论保持“受控 RC”，不能标记为企业生产验收完成。

## 7. 后续代码审核

2026-10-30 新一轮已修复 PostgreSQL 协议错误后的连接失同步、Worker 认证旁路、Operator 权限缺口、畸形生命周期请求被当作成功、前端凭据竞态及详情错误语义，并新增后端/前端回归和实际进程 HTTP 冒烟。逐项证据、兼容性变化、浏览器自动化环境限制与剩余 IAM/Worker 原子性风险见[代码审核报告](CODE_REVIEW_2026-10-30.md)。这些修复不改变本文的企业发布门禁结论。
