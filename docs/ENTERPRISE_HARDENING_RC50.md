# RC50 企业迁移门禁实施与待验收项

代码版本为 `0.15.0-rc50`。初审于 2026-10-30，本文于 2026-11-23 更新本地回归结果。

## 四项工作的实际状态

| 用户要求 | 本轮代码实现 | 尚未取得的证据 |
| --- | --- | --- |
| 最终停写、源目标水位追平 | 外部停写确认、操作者/依据/幂等号/15 分钟有效期；数据库身份、活跃/预备事务检查；原生水位比较；未追平保持 CDC；停止和排空后再次核验；前端录入与证据查询 | 真实业务停写联动与真实数据库割接/回切验收 |
| Worker 代次、原子元数据、目标 fencing | 每次领取递增代次；报告必须携带领取时的代次；所有者/代次/有效租约的条件更新；任务 Revision CAS；CDC 执行 epoch 持久保留；位点/Spool/DLQ 写入与所有者检查同事务；MySQL/PostgreSQL 全量批次、CDC 事务目标围栏 | 真实节点暂停、网络分区、数据库切主、旧进程复活的物理故障验收 |
| IAM 并发、持久 Saga | 用户 Revision CAS；事务内序列化管理员变更；最后一个可用管理员保护；Memory 快照保留密码哈希；割接/回切步骤意图与结果、反向起点、清理重试持久化；安全步骤恢复、不确定 DDL 转人工核查 | 真实多副本 IAM 竞争、升级中断和恢复演练 |
| 真库、多节点、升级恢复、长稳 | 在临时 MySQL 8.4.7、PostgreSQL 16.16 上通过全量迁移、CDC、事务围栏及元数据升级用例 | 麒麟 V10 SP3、4.19 内核的物理单节点和多节点部署，节点故障恢复与 24–72 小时长稳仍待验收 |

当前结论是“完成一轮核心安全补强的 RC”，不能标为企业生产 GA，不能用上述自动化入口代替验收结果。

## 停写与水位的具体契约

`POST /api/v1/migrations/{id}/cutover/freeze` 与 `rollback/freeze` 要求 Admin/DBA 提交 `writes_frozen=true`、10–2000 字停写依据和 8–128 字幂等号。`GET .../operations` 可查询操作、冻结水位、已应用水位和步骤结果。幂等号也可用 `Idempotency-Key` 请求头。

业务方或 DBA 须停写并保持到操作完成。默认流程检查可见的活跃写事务、预备事务和账号权限。支持表锁的 PostgreSQL 源可额外提交 `hold_table_locks=true`，由专用连接持有选定表的锁，并在最终核验时检查锁证明。确认有效期为 15 分钟。

MySQL 检查 `innodb_trx` 和 `XA RECOVER`，要求直接授予 PROCESS 及版本对应的 XA 可见性权限。MySQL 8 及以上还需启用 Performance Schema 并向验收账号授予 `SELECT ON performance_schema.data_locks`，以核对实时写锁。PostgreSQL 要求 `pg_read_all_stats` 或 superuser 事务可见性，并能读取 `pg_control_system()` 的集群身份、执行逻辑消息函数。

水位按 PostgreSQL LSN 数值、MySQL binlog 文件序号/偏移或 GTID 集合包含关系比较，不使用字符串大小或 lag=0 作为追平证明。不支持语义比较的外部/实验位点拒绝最终收尾。端点、映射或数据库实例身份变化会使确认失效。

PostgreSQL 确认时发出事务性 `dts.freeze` 逻辑消息。Reader 开启 `messages=true`，在消息所属事务提交后生成 checkpoint，使空闲表也能推进位点。默认模式会记录并比较物理 WAL 头，同集群的其他写入也可能使确认失效。启用表锁时，最终核验会检查选定表的锁证明和迁移水位。

新增 `TRUNCATE` 拒绝，避免原 pgoutput 解码器忽略后继续确认导致漏同步。自动 CDC DDL 目前进入人工处理门禁，不能依赖 MySQL 会隐式提交的 DDL 来维持事务围栏。

## 代次及目标围栏的保证范围

所有 Worker 任务变更请求都必须携带领取结果中的 `X-DTS-Lease-Generation`。缺失返回 428；旧代次、过期租约或所有者不符返回冲突。注册、心跳、领取不需要代次。每次执行绑定自己的不可变代次，CDC 子进程从 `DTS_LEASE_GENERATION` 继承，不通过任务 ID 的共享缓存取当前值。

CDC 持久 epoch 按任务和方向递增，释放后保留计数。Memory 中校验和变更共用互斥锁；PostgreSQL 中通过同一事务的 lease 行锁与元数据写入，阻止接管后旧执行者补写位点/Spool/死信结果。

原生目标实现为 MySQL/PolarDB MySQL 的 `__dts_execution_fence` 和 PostgreSQL/PolarDB PostgreSQL 的 `dts_internal.execution_fence`。这些名称保留给 DTS；元数据枚举会隐藏它们。MySQL 业务表必须为 InnoDB；PostgreSQL 必须为普通表或分区表。表的元数据锁与围栏行锁保持到业务事务结束。其他目标当前拒绝执行此写入路径，兼容协议不构成相同事务语义的证明；云厂商版本仍须分别验收。

围栏含义是：**新代次的目标围栏事务建立后，更旧的代次不能再越过它提交。** 已经持有围栏行锁的旧事务与接管事务按目标锁顺序串行执行。这不等于元数据库租约过期瞬间就能远程撤销目标事务，也不是源、目标、元数据之间的分布式原子提交。CDC 仍用已有 COMMIT_UNCERTAIN 记录处理目标提交结果未知，禁止自动盲重放。

全量行恢复依赖主键 Upsert 幂等，目标触发器/外部副作用不自动获得 exactly-once 保证。元数据从较早备份恢复后，epoch 可能落后于目标围栏并被安全拒绝；必须核查位点和目标代次后制定恢复方案，不能删除围栏表来绕过。

## Saga 和 IAM

割接记录 STOP_READER、DRAIN_SPOOL、SYNC_GENERATED_STATE、FINALIZE_GENERATED_MODES 的开始、完成/失败以及时间。状态和操作证据共用 MigrationTask 的 CAS 更新；反向准备在可能创建复制槽前保存操作意图，捕获后保存完整起点和资源名。成功后的复制槽清理保存 PENDING/RUNNING/DONE/RETRY，重启巡检可重试幂等清理。

停止 Reader、排空可以安全重试；中断的数据库结构/生成值步骤和结果无法证明的反向起点创建会进入 FAILED/NEEDS_REVIEW。当前是持久化、失败关闭的前向恢复 Saga，**没有自动撤销跨数据库 DDL 的补偿引擎**。人工核查后再决定恢复，不将未知副作用默认为成功。

IAM 的普通资料更新、改密和角色更新共用 Revision；过期快照返回 409。PostgreSQL 在事务内先取得 IAM advisory lock，再读取用户及剩余管理员，避免两个请求同时停用最后两个管理员。登录时间采用窄字段更新。Memory 文件快照改用专门持久结构保存密码哈希；历史文件如果此前已经丢失哈希，升级无法还原，须通过管理员恢复流程重设。

## 升级与验证

先备份元数据、Spool 和配置，暂停领取及业务变更，停止所有旧 Server、Worker 和 CDC 子进程。从 rc49 手动升级时依次执行 `083_v015_rc50_atomic_control.sql`、`084_v015_rc50_user_sessions.sql` 和 `085_v015_rc50_personal_tokens.sql`，或运行受控自动升级。已标记 rc50 的元数据库也需要补齐这三项。确认会话表、个人令牌表和版本标记后，统一启动同版本的 Server 和 Worker。恢复备份时须核对目标围栏代次及 COMMIT_UNCERTAIN。

本轮在 PostgreSQL 17.11 上使用历史 RC40 Schema，连续两次升级后核对了任务状态、已迁移行数、数据源凭据字段和新登记表。PostgreSQL 17.11 与 MySQL 8.4.7 共通过八个 TLS 用例，覆盖受信任 CA、错误 CA、主机名不符与拒绝明文。Chrome 在登录后主动断开 Worker 页事件连接，验证第二次握手和列表刷新。Kubernetes 生命周期脚本以模拟 kubectl 验证部署契约。

此前固定副本 `ce130641549f3b06d620d4e11abe2b086e594ca9ba41b13fe3dad28ac0b943ca` 在 Windows 上通过普通 Go 测试和 `-race`。该结果仅对应当时的源码。包含 OIDC、令牌和连接池的本轮构建，在 Windows 上运行 `-race` 时 ThreadSanitizer 遇到虚拟内存分配错误，随后在断网 Ubuntu 24.04.5 虚拟机用 Go 1.26.11 原生运行全后端竞态检测，51 个有测试的包通过，35 个其余包完成编译。该轮未启用 integration 标签。详见 [Linux Race 记录](../dist/qualification/rc50-linux-race-20261123/summary.json)。

数据库专项覆盖 CRC32、按 GTID 和文件位点启动、ZSTD 混合表事务、复合主键分页、含引号及反斜杠的文本跨库读写、停写检查和围栏接管。PostgreSQL 元数据测试实际执行了升级和并发写入。DB2 通过了分片 SQL 生成回归，尚待连接 DB2 实例核对。自定义过滤表达式解析器通过 171,018 次模糊测试执行。

| 深入审核中发现的问题 | 修复后的行为 |
| --- | --- |
| 全量行过滤与后续按表消费的 CDC 范围不同 | 仅全量任务可使用行过滤。旧过滤任务进入 CDC、割接或回切前会收到错误。 |
| DB2 整数分片漏末行、复合键续读超出边界，超大 BIGINT 跨度计算溢出 | 整数分片使用闭区间，复合键使用下闭上开区间。已知行数时每表初始整数分片上限为 16,384，行数未知时为 128，超出后自动放宽估算跨度。 |
| 已领取分片可能被拆分操作覆盖，子分片写入失败可能留下空档 | 拆分须匹配原始分片和领取状态。内存仓库在同一把锁内更新，PostgreSQL 在同一事务中写入全部子分片。真实 PostgreSQL 双连接验证了并发领取和失败回滚。校验、割接还会核对旧数字分片是否覆盖原表范围，以及普通 keyset 的首尾边界能否连续相接。 |
| MySQL 轮转后校验模式可能错误，压缩事务解码可占用过多内存 | 逐文件识别并核对 CRC，按外层位点确认事务。解码超限时保留已确认位点。 |
| 校验可能漏掉短页后的数据、大整数 JSON 差异或写入失败的校验证据 | 扫描持续到空页，保留数值精度，检查游标前进与分片覆盖。证据写入或状态提交失败会中止本轮收尾。 |
| PostgreSQL 元数据写入将合法空串写成 NULL | 创建任务、表、分片与进度上报保留需要的空字符串。包内 083、084 升级脚本在真实 PostgreSQL 各执行两次通过，嵌入式升级单独验证通过。 |
| 完整离线包的清单指向已合并删除的文件，解包流水线可能漏报 tar 错误 | 清单指向实际归档。解包完整读取 tar 后才替换旧文件。使用小型镜像样本测试了精简包和完整包的 Linux 构建脚本，完整包合并了 13 个镜像标签。 |

原生隔离验收入口：

```bash
docker compose -f deployments/qualification/compose.yaml up -d --wait
bash deployments/qualification/run-native.sh
# 重复原生迁移长跑，默认 24 小时；输出每轮日志、源码快照、结果。
DTS_SOAK_SECONDS=86400 bash deployments/qualification/run-soak.sh
docker compose -f deployments/qualification/compose.yaml down --volumes
```

这些命令会在隔离数据库中建表、清理测试数据并读写复制位点，验收机需提供 Go 和 `zstd`。本次使用本机回环地址上的临时数据库运行同一套 Go 用例。Linux Compose 入口和麒麟现场部署仍需在目标环境执行。

重复迁移长跑不会自动判定生产合格，结果显式保留 `production_qualified=false`。还必须在实际集群完成：双 Server/多 Worker 进程强杀和恢复、节点暂停超过 TTL 后复活、单向断网/延迟、元数据库及目标库切主、Spool 存储故障、滚动停机升级与备份恢复，以及单任务持续 CDC 24–72 小时的吞吐、积压、内存/协程和位点不丢失证据。现有 synthetic chaos/soak 的结果不能代替这些演练。

实现参考：[PostgreSQL INSERT/ON CONFLICT 行锁与条件更新](https://www.postgresql.org/docs/17/sql-insert.html)、[PostgreSQL 行锁生命周期](https://www.postgresql.org/docs/17/explicit-locking.html)、[MySQL XA RECOVER](https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html)。
