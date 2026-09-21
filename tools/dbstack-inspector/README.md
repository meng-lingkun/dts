# DBStack + PolarDB-X Go 巡检工具

根据 2026-09-14 对 `10.10.156.111` 的现场只读调查编写。程序使用 Go，复用执行节点的 `kubectl` 和 kubeconfig。Linux 二进制无需安装 Go、Python 或 MySQL 客户端。

## 在已登录的服务器运行

已经在 `10.10.156.111:/root/dbstack-inspection-20260914/` 放置可执行文件、现场配置和实测报告：

```bash
cd /root/dbstack-inspection-20260914
./dbstack-inspector -config config.json -out reports -deadline 3m
rc=$?
printf 'inspection exit code: %s\n' "$rc"
```

程序只查询 Kubernetes、请求健康接口、读取 MySQL 握手；配置数据库账号后执行固定的 `SELECT` / `SHOW` 查询，配置 AK 后调用固定的 `DescribeInstancePatrolInfo`。不执行修复、重启、切换、建表或任意 SQL。发现过程中读取过 Pod 内的已安装 SDK；**巡检程序本身不执行 kubectl exec、不读取 Secret、不远程 SSH**。

输出 `inspection-时间戳.md` 中文报告和同名 `.json`，包括状态、证据摘要及节点、Service、数据库 Pod 清单。报告文件以 0600 创建，目录以 0700 创建；已有目录权限由运维人员管理。每次运行生成独立文件，不覆盖历史报告。

| 退出码 | 含义 |
|---|---|
| 0 | 检查均为 OK |
| 1 | 有 WARN，无 CRITICAL / UNKNOWN |
| 2 | 有 CRITICAL；可能同时存在 UNKNOWN |
| 3 | 有 UNKNOWN，或配置、执行、报告写入失败 |

判断完整性还需要读取 JSON 中 `summary.UNKNOWN`。有 CRITICAL 时优先返回 2，不会掩盖报告中的 UNKNOWN。报告中的条目不是独立故障数：同一问题可能同时影响 Pod、Deployment、Service 和连接探测。

## 已实现的检查

| 层次 | 检查内容 |
|---|---|
| Kubernetes | API Server `/readyz`；节点 Ready、资源压力、禁止调度 |
| 平台组件 | Pod 就绪、等待原因、近期重启；Deployment / StatefulSet / DaemonSet 副本；PVC；EndpointSlice 可用地址 |
| 主机资源 | 全部节点 kubelet summary：CPU、内存、rootfs、imagefs、返回的卷容量与 inode |
| DBStack API | 动态解析 Service 地址；按现场 Pod 探针发现明确的健康路径；Ingress Host 路由；JSON 业务成功字段 |
| Operator | XStore 阶段、就绪副本、Leader Pod、代际；PolarDBXCluster 阶段 |
| DBStack 业务实例 | 按 `apsara.metric.replicaset_name` 自动分组，检查副本基线与跨节点分布；检查预期实例 Service 是否消失 |
| 数据库连通性 | Pod 地址、Service VIP、externalIP、EndpointSlice 实际后端的 TCP/MySQL greeting；不将来源地址拒绝误判为实例故障 |
| 备份与事件 | 备份 CronJob 最近成功时间、暂停状态；最近一小时 Warning 事件 |
| 可选业务 API | 按现场 SDK 实现 AK 签名、实例巡检清单分页；严格限定 `DescribeInstancePatrolInfo` |
| 可选 SQL | `SELECT 1`、全局连接数/运行线程/累计慢查询、连接占用率；CN `SHOW STORAGE` 健康与可用的延迟字段 |

累计重启数仅作为证据，只有最近时间窗口内发生重启才告警。成功结束的 Job Pod 不按未就绪报警。新建 Pending Pod 在前 5 分钟按 WARN 提示启动状态，镜像拉取失败等明确错误不适用宽限。HTTP 不自动跟随重定向；现场 Portal 的预期 308 有显式目标校验。证书信任失败、鉴权不足、字段不支持、采集缺失均标为 UNKNOWN。

**覆盖边界：**kubelet summary 不保证返回所有 hostPath 数据盘；Pod Ready 不等于 SQL / Paxos 健康；CronJob 成功不等于业务备份可恢复；DN Paxos、CDC 延迟、业务备份集完整性及恢复演练没有未经验证的成功判定。SQL 和 AK 功能已做模拟测试，因未提供对应凭据，未进行现场认证调用。

## 配置与基线

`config.10.10.156.111.json` 是现场配置。默认每个请求 10 秒、并发 6、整轮最多 10 分钟，可通过 `-deadline` 调整。CPU/内存告警阈值默认 85%，磁盘/inode 80% 告警、90% 严重。CPU 为瞬时采样、分母为节点 allocatable。

`expected_groups` 来源于**首次发现快照**，用于发现后续副本减少，不代表已经确认的产品规划副本数。尤其 `pxc-xdb-s-xxr5a44mk8kh4q5073` 当前仅发现 2 个 Pod，工具额外提示核对副本规划和 Paxos 成员；不要把 2 当作已确认的合理配置。扩缩容后应核对平台配置再更新基线。`expected_services` 为三个已发现业务实例入口。

Service URL 使用 `.svc.cluster.local` 时，工具从 Kubernetes 清单解析 ClusterIP，避免依赖宿主机的集群 DNS。普通域名仍需要正常 DNS 或 hosts；工具不修改服务器 DNS、hosts 或证书配置。

可使用 `-kubeconfig /path/config`、`-context name`、`-kubectl /path/kubectl`。建议在有集群网络访问能力的管理节点运行；从外部机器直接运行可能访问不到 Pod/ClusterIP。配置未知字段会被拒绝，避免拼写错误静默生效。

## 启用业务 OpenAPI

本环境 OpenAPI 域名为 `openapi.dbstack.test.com`，Region 从现场 MetaDB 拓扑推定为 `cn-hangzhou`，使用前请与平台账号所属 Region 核对。当前宿主机不能解析该域名，需要配置正常的域名解析和受信任证书链。

```bash
read -rp 'DBStack AccessKey ID: ' DBSTACK_ACCESS_KEY_ID
read -rsp 'DBStack AccessKey Secret: ' DBSTACK_ACCESS_KEY_SECRET
printf '\n'
export DBSTACK_ACCESS_KEY_ID DBSTACK_ACCESS_KEY_SECRET
./dbstack-inspector -config config.json -out reports
unset DBSTACK_ACCESS_KEY_ID DBSTACK_ACCESS_KEY_SECRET
```

不要把 SSH 的 root 密码当作 AccessKey，也不要写进 JSON。`healthy_status_descriptions` 默认空数组，因为现场还没有取得业务 API 的真实状态枚举。取得授权响应并核对状态后，可配置实际的健康状态文本。未识别的状态保持 UNKNOWN；明确含异常、失败等状态描述会报警。API 使用分页上限及重复实例检测；响应未完整读取时不会报告清单完整。

`api-catalog.json` 从现场 SDK 提取了 **169 个 Action**，其中 80 个名称以 Describe/List/Get 开头。它是接口目录，不是安全白名单。本版脚本只调用已经确认用途的 `DescribeInstancePatrolInfo`。详见 `ARCHITECTURE_AND_API.md`。

## 启用数据库 SQL 检查

在配置的 `sql` 数组中添加实例。以下地址为现场旧实例的业务入口：

```json
{
  "name": "pxc-xxrn1szq4dqgbb",
  "address": "10.10.156.106:35009",
  "username_env": "POLARDBX_INSPECT_USER",
  "password_env": "POLARDBX_INSPECT_PASSWORD",
  "role": "cn",
  "tls": "true",
  "connection_warn_percent": 80,
  "lag_warn_seconds": 30
}
```

```bash
read -rp '数据库巡检用户名: ' POLARDBX_INSPECT_USER
read -rsp '数据库巡检密码: ' POLARDBX_INSPECT_PASSWORD
printf '\n'
export POLARDBX_INSPECT_USER POLARDBX_INSPECT_PASSWORD
./dbstack-inspector -config config.json -out reports
unset POLARDBX_INSPECT_USER POLARDBX_INSPECT_PASSWORD
```

`tls: "true"` 要求数据库支持 TLS 且证书受执行节点信任。如果实例确实未开启 TLS，可按现场策略显式配置 `false`，报告会注明。不要直接复用 SSH 凭据。`SHOW STORAGE` 字段按返回列名判断；字段缺失或权限不足标为 UNKNOWN。DN 使用 `role: "dn"` 时只执行通用只读查询，不用传统 MySQL 主从状态冒充 Paxos 健康。

## 构建与验证

源码是独立 Go module，不改变 DTS backend 模块及现有部署。构建需 Go 1.23+ 和下载依赖的网络；运行只需要二进制及 kubectl。

```bash
go mod download
go test ./...
go vet ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o dbstack-inspector .
```

测试覆盖错误/截断 MySQL 握手、来源访问限制、HTTP 假成功与重定向、TLS 信任错误、Service 动态解析、Pod 未就绪、历史重启、缺失副本、容量阈值、签名编码、分页及凭据不进入报告。现场验证使用麒麟 V10 x86_64、Kubernetes 1.30.3。

所需 Kubernetes 权限：get/list nodes、pods、services、PVC、events、工作负载、CronJob、EndpointSlice 和相关 PolarDB-X CR；GET `/readyz`；开启资源检查时 GET `nodes/proxy`。`nodes/proxy` 权限能力较广，应使用已有受控巡检身份；若不提供，可设置 `node_proxy:false`。无需 Secret 或 pods/exec 权限。程序当前按集群范围列举后过滤 namespace，因此 namespace 范围的 Role 不足以运行全部采集。

未安装常驻进程或定时任务。需要调度时可由已有运维调度平台执行该命令并按退出码、JSON 状态告警。
