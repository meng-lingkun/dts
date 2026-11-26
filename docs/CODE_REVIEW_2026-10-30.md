# DTS 代码审核、测试与整改报告

> 后续 rc50 的停写证明、执行代次与目标围栏、IAM 并发、持久 Saga 及真库验收入口，见 [RC50 实施记录](ENTERPRISE_HARDENING_RC50.md)。

日期：2026-10-30。版本：`0.15.0-rc49`。审核对象为提交 `89ea243` 加当前工作区；保留了前两轮尚未提交的源码、测试、部署和文档改动。本报告只记录本轮新增的发现与修复，不把既有补丁重复计算为新增交付。

## 1. 结论与范围

项目具备控制面、统一迁移引擎、分布式 Worker、Full/CDC、持久 Spool、校验及离线部署的完整骨架。当前结论仍是“受控 RC，企业发布门禁未全部关闭”。

本轮覆盖了 PostgreSQL 协议客户端的错误/取消恢复、HTTP 认证与 RBAC、生命周期请求解析、前端身份状态、角色操作入口、详情数据错误语义及 CI。对发现的问题先补失败回归，再修改实现；没有连接用户源库或目标库，没有执行真实迁移、割接、回切或部署。

## 2. 已修复问题与证据

| 优先级 | 问题和影响 | 复现证据 | 修复及实现入口 |
| --- | --- | --- | --- |
| P0 | 用户认证开启而 `DTS_WORKER_TOKEN` 缺失时，Worker POST 绕过 API 认证；匿名访问者能注册 Worker，进一步可能触及调度和含凭据的作业路径。生产启动校验通常要求机器 Token，但单独开启认证的部署仍受影响 | 新测试在旧代码上实际收到 201，并在仓库中读到匿名注册的 `intruder` | 只有配置独立机器 Token 时才走 Worker 专用认证；否则走用户认证/RBAC。`api/server.go: apiAuth` |
| P0 | PostgreSQL 收到 ErrorResponse 后立即返回，遗留 ReadyForQuery；下一次查询消费旧 Z，可能错误地成功并返回空结果 | 脚本连接先返回重复键错误，再返回 SELECT 7；旧实现第二次返回零行 | 保留 SQL 错误，读到 Z 后再返回；只有已同步的连接可复用。`connector/postgres/protocol.go: query` |
| P1 | PostgreSQL 网络中断/协议截断后复用失同步连接，取消无截止时间的查询也不能及时中断读写 | 截断帧后旧连接未关闭；真实 `net.Pipe` 上取消后仍阻塞 | 传输/协议失败关闭并标记连接；后续操作重连，不自动重放原 SQL；查询级截止时间和取消回调共同中断读写，回调退出后再复用连接。`protocol.go`、`connector.go: connectLocked` |
| P0 | 畸形生命周期 JSON 被忽略，空值被当作成功结果；可误标 Worker STOPPED、重新排队任务或推进回切状态 | 旧代码对 `{"error":` 返回 200；状态分别变为 STOPPED、PENDING、ROLLBACK_SYNCING | 六处可选请求体解析不再忽略错误；空体保持兼容，部分 JSON 必须 400。`api/json_request.go`、`server.go` |
| P1 | 请求接受第二个 JSON 值/尾随内容，且通用 DTO 解析没有大小边界 | 旧解析接受 `{} {}` 和 `{} trailing` | 单个完整 JSON；64 MiB 上限，超限 413；CDC 生产方需把更大事务的接入方案单独资格验证。`api/json_request.go` |
| P0 | Operator 的字符串前缀策略漏掉 `ready-cutover`，并允许任意 CDC 事件/位点注入及未来新增的迁移写接口 | 旧 RBAC 回归实际允许 ready-cutover、cdc/events、cdc、cdc/started、debezium、canal 及未知写动作 | Operator 改为明确的普通操作白名单；未知角色拒绝；Admin/DBA 保留相应数据库权限。`auth/rbac.go` |
| P1 | 旧 HTTP 401 能清掉随后建立的新会话；下载路径也有同样竞态；输错登录密码会撤销既有会话 | 前端旧实现 8 项测试中有 6 项失败，包括新 Token 变为空字符串 | 按请求所用凭据及变更代次处理 401；登录失败不全局登出；切换凭据同步清理旧身份。`web/src/api/client.ts`、`stores/auth.ts`、`main.ts` |
| P1 | 身份刷新遇到断网/5xx 就销毁凭据；登录未确认身份刷新结果仍提示成功；设置页身份与全局角色不一致 | `stores/auth.ts` 原 catch 无条件清凭据；设置页单独维护身份 | 网络错误保留凭据，登录页提供重新连接；成功登录要求身份验证成功；设置页更新同一身份 Store。`Login.vue`、`Settings.vue` |
| P0（条件性） | 使用 `VITE_DTS_API_TOKEN` 时，Vite 会将这个 Token 写入公开前端资源 | 源码读取了该构建变量；本轮未发现或提取任何实际部署密钥 | 删除构建时密钥回退，Token 只能运行时输入；新登录清除旧静态 Token。`web/src/api/client.ts` |
| P1 | Header 对象/元组被当作普通对象展开导致请求头丢失；HTTP 200 非 JSON 响应被伪装为空对象 | Headers/tuple 测试丢失 X-Trace；HTML 200 没有被拒绝 | 使用标准 Headers；成功响应必须符合 JSON 契约，204 单独处理；JSON/Blob 共用认证失败处理。`client.ts` |
| P1 | 页面向无权限角色展示写操作；割接中心任何状态都能点割接；迁移列表割接缺少同等确认与重复提交保护 | 前端入口与服务端 RBAC/状态机对照 | 按角色隐藏数据源、迁移、校验和割接写入口；割接匹配准确状态；待处理任务禁止重复提交；高风险确认固定原任务 ID。`lib/access.ts` 及相关页面 |
| P1 | 详情接口失败被伪装成 Spool=0/水位正常；较慢的旧任务详情可能覆盖新选择；低权限页面仍请求 DLQ 镜像 | 原实现把失败 Spool 转成全零对象，并无响应代次校验 | 加载器保留 unknown/null 和错误列表；明确显示未知；只接受当前选择的响应；无权限不请求 DLQ。`lib/migration-details.ts`、`Migrations.vue` |
| P2 | 异步初始加载结束前卸载页面，仍可能创建轮询定时器/Socket；开发代理未启用 WebSocket | 组件生命周期和 Vite 配置审核 | 卸载标记阻止创建新后台任务；Vite `/api` 代理启用 ws；静态侧栏不再宣称“控制面在线”。相关页面、`vite.config.ts` |

权限意图来自使用手册及设置页的 Admin/DBA/Operator/Viewer 职责：Operator 负责普通生命周期，DBA 负责数据库写入、割接和回切。HTTP 认证开启后，未认证请求不应通过 Worker 路径取得调度能力。前端隐藏按钮只是产品反馈；真实权限仍在后端强制执行。

## 3. 新增测试与运行结果

新增后端 9 个测试函数，含多个子场景：

- `connector/postgres/query_recovery_test.go`：SQL 错误后的第二次查询、截断协议连接废弃、无调用方截止时间时的取消。
- `auth/rbac_test.go`：Operator 特权及未知动作拒绝、普通动作放行、未知角色拒绝。
- `api/request_validation_test.go`：畸形请求不改变状态、单 JSON 契约、HTTP 权限边界、未配置机器 Token 时匿名 Worker 拒绝、超限与空体兼容。

新增前端 12 项测试，使用 Node 原生测试工具和已有 TypeScript 支持，不新增依赖：

- `web/tests/client.test.mjs`：旧 401、新会话、下载、错误登录、请求头、网络失败、无效 JSON、静态 Token 清理。
- `web/tests/access.test.mjs`：角色权限及割接状态矩阵。
- `web/tests/migration-details.test.mjs`：读取失败保持未知、受限角色不请求业务行镜像。

| 检查 | 本轮结果 |
| --- | --- |
| 修改前 `go test ./...` | 通过，说明原测试集未覆盖本轮缺陷 |
| 失败回归复现 | PostgreSQL 3 项、RBAC、畸形请求/状态、前端凭据竞态均在修复前失败 |
| 修改后 `go test ./...` | 通过 |
| `go vet ./...` | 通过 |
| Linux/amd64、CGO=0 `go build ./...` | 通过；仅交叉编译 |
| `go test -tags integration ./... -run '^$'` | 通过；只编译，不等于真库运行 |
| `npm test` | 12/12 通过；已接入 CI frontend Job |
| `npm run build` | vue-tsc 与 Vite 通过；主 Chunk 仍约 805 kB，有大包警告 |
| 版本检查、`git diff --check` | 通过；版本未提升 |
| 实际进程 HTTP 冒烟 | 通过，见下表；隔离的内存仓库，未使用既有状态文件 |
| 浏览器 E2E | 未完成：创建测试标签及浏览器通道库存均报告 `The browser access host is unavailable`；不能据 HTTP 200 判断组件交互已验收 |
| 本地 Race | 未运行：CGO=0 且无可用 C 编译器；仍由 Ubuntu CI 执行，远端结果未取得 |
| 真库迁移、TLS 版本矩阵、Kubernetes 故障/升级/长稳 | 未执行 |

实际 Server 二进制和 Vite 仅绑定 `127.0.0.1`，使用临时测试凭据；完成后已核对可执行文件/命令行并停止两个测试进程。

| 运行时断言 | 实际结果 |
| --- | --- |
| `/healthz` | 200 |
| Admin 登录后 `/auth/me` | 身份为 admin |
| Viewer 读取迁移列表 | 200 |
| Viewer 读取用户管理 | 403 |
| Operator 调用 ready-cutover | 403 |
| Operator 注入 CDC 事件 | 403 |
| 无机器 Token 时匿名 Worker 注册 | 401 |
| 畸形 rollback/cdc-started 请求 | 400 |
| Vite 首页 HTTP | 200，包含入口模块；不代表浏览器执行成功 |

## 4. 仍未关闭的发布风险

1. **P0 数据正确性**：最终业务写入冻结/水位证明、目标库原子 fencing、所有元数据状态更新的 CAS、全量 Worker 的持久代次隔离以及割接/回切 Saga 仍未完成，不能把租约当作跨数据库原子提交证明。
2. **P1 IAM 原子性**：用户改密/资料更新仍走读取后整行 `UpdateUser`，并发改密/改角色可能覆盖彼此；`ensureEnabledAdminRemains` 的计数和更新没有事务，两个管理员并发停用可能跨过最后管理员门禁。见 `api/server.go: updateUser/changeUserPassword/ensureEnabledAdminRemains`。需要仓库级字段更新、CAS 和跨副本事务，而非只加单进程锁。
3. **P0 Worker 故障恢复**：Chunk/Engine 续租仍需统一到“代次 + 未过期 Owner + 原子完成”协议；现有 `RenewChunkLease`、`RenewEngineJobLease` 只检查状态与 Worker ID，不能证明旧进程不能复活或覆盖新的执行。需要一起升级 Worker/API/Repository，并用真库、多进程故障测试验收。
4. **P1 元数据与发布**：单连接串行访问、参数化 SQL、Schema 升级事实源与一致性备份恢复仍需整改。协议恢复修复不等于连接池或迁移事务升级完成。
5. **P2 前端与可维护性**：迁移 Service/API Server/迁移页面仍承担过多职责；浏览器组件/E2E、请求取消与超时策略、Socket 重连和可访问性仍需补全。完成 FULL/CDC 后的再校验需要独立校验运行与不可变归档版本设计，不能直接开放状态转换重启 CDC。
6. **资格证据**：数据库版本、类型/LOB、真实源目标、TLS、网络分区、杀进程、升级回滚、容量及长稳矩阵尚未验收；实验 Connector 不解锁生产能力。

## 5. 行为兼容与交付限制

- Operator 自动化若曾调用 CDC 写入/位点或割接准备接口，升级后会得到 403；应使用明确授权的数据库管理身份或受保护的 Worker 通道。
- 开启用户认证而未配置 `DTS_WORKER_TOKEN` 时，Worker 请求必须经过用户认证；企业部署仍应独立配置机器 Token。
- 请求必须为单个完整 JSON；可选空生命周期请求保留。通用 DTO 请求大于 64 MiB 返回 413，不自动截断或忽略尾随数据。
- 不再从 `VITE_DTS_API_TOKEN` 编译密钥；需要在登录或访问设置中运行时输入。没有读取、轮换或暴露实际部署密钥。
- 未提交 Git、未提升版本、未重制 `dist/` 离线安装包、未发布镜像或部署。新旧 Server 混跑限制和剩余门禁仍以 [企业就绪度记录](ENTERPRISE_READINESS.md) 为准。
