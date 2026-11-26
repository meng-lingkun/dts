# DTS PostgreSQL 与 S3 CDC Spool 离线灾备工具

支持 Linux amd64。解压后运行 `sha256sum -c SHA256SUMS`。本工具作为 RC50 离线安装包的附加组件使用，运行所需的 `pg_dump`、`pg_restore` 和系统库已在两个核心安装包的 `dts/postgres:17` 镜像内。维护主机若已有与元数据库匹配的 PostgreSQL 客户端，也可直接执行二进制。

## 停写备份

1. 暂停新任务并排空正在提交的作业，停止所有 Server、Worker 和控制器。保留专用元数据库。暂停 S3 生命周期、复制及一切外部写入，保证整个操作期间仅恢复进程可以写目标。强一致的对象读取和列举是恢复点成立的条件。
2. 在可信客户端准备受保护的 `/secure/dr-source.env`，设置 `DTS_WRITERS_STOPPED=YES`、原始 `DTS_MASTER_KEY`，以及 `DTS_METADATA_HOST`、`DTS_METADATA_PORT`、`DTS_METADATA_USER`、`DTS_METADATA_PASSWORD`、`DTS_METADATA_DATABASE`。账号须能读取全部元数据和锁定表。需要验证数据库证书时配置 `PGSSLMODE=verify-full` 并挂载 `PGSSLROOTCERT` 对应的 CA。
3. 在同一文件内配置现有 Spool 的 `DTS_CDC_SPOOL_S3_ENDPOINT`、`DTS_CDC_SPOOL_S3_BUCKET`、`DTS_CDC_SPOOL_S3_PREFIX`、`DTS_CDC_SPOOL_S3_ACCESS_KEY`、`DTS_CDC_SPOOL_S3_SECRET_KEY`。与应用保持一致的可选项包括 `REGION`、`PATH_STYLE`、`SESSION_TOKEN`、`CA_CERT`、`TLS_SERVER_NAME`、`TLS_CLIENT_CERT` 和 `TLS_CLIENT_KEY`，均使用同一前缀。多行 PEM 可使用容器 Secret 注入环境。备份凭据需允许列举对象和未完成分片、读取对象及解密源对象的 KMS 权限。
4. 用私有目录存放恢复集，凭据另行保管。下面是 Compose 安装的运行命令，当前目录为解压后的工具目录。`/srv/dts-recovery` 须事先存在并允许当前 UID 写入，`set-001` 必须为新目录。

```sh
docker run --pull=never --rm --network dts-offline_default \
  --user "$(id -u):$(id -g)" --env-file /secure/dr-source.env \
  -v "$PWD:/tools:ro" -v /srv/dts-recovery:/recovery \
  --entrypoint /tools/dts-s3-recovery dts/postgres:17 backup /recovery/set-001

docker run --pull=never --rm --network none \
  --user "$(id -u):$(id -g)" --env-file /secure/dr-source.env \
  -v "$PWD:/tools:ro" -v /srv/dts-recovery:/recovery:ro \
  --entrypoint /tools/dts-s3-recovery dts/postgres:17 verify /recovery/set-001
```

备份用 PostgreSQL 同一个可重复读快照采集引用和生成完整元数据库归档，对应用表持有共享锁。它分页列举专用前缀，按 ETag 条件读取并对内容计算 SHA-256，然后再次列举并比较。一个有效恢复集包含 `metadata.dump`、按摘要存放的 `objects/`、`manifest.json` 及最后生成的 HMAC-SHA256 完成标记 `READY`。归档保留执行时可见的全部对象，包括待回放、已应用保留对象及孤立对象。APPLIED 元数据内的历史 pending 路径可能已清理。v2 待回放引用核验原始摘要，v1 核验存在性并记录本次摘要。工具使用应用的 AES-GCM 实现验证待回放对象、数据源凭据、内嵌 Spool 和死信密文。全新空库没有可用于识别原主密钥的密文，须使用部署时保存的主密钥。

## 恢复到隔离环境

预建独立的空数据库和新桶或无历史版本的空专用前缀，目标账号须能创建架构对象。所有应用实例保持停止，直至恢复和人工复核完成。目标桶可以不同，前缀须保留原值，因为 PostgreSQL 保存了包含前缀的对象键。目标 S3 须支持 ListObjectVersions、ListMultipartUploads、条件 PutObject、条件 CompleteMultipartUpload、条件 DeleteObject，以及分片上传、列举和读回验证。加密桶还需相应 KMS 权限。通过目标桶策略配置新对象的默认加密、访问控制和保留策略。

使用 `/secure/dr-target.env` 指向目标数据库和桶，设置 `DTS_RESTORE_CONFIRM=YES`、`DTS_WRITERS_STOPPED=YES` 及原主密钥。恢复集须对维护容器只读，并在整个恢复期间保持不可变。

```sh
docker run --pull=never --rm --network dts-offline_default \
  --user "$(id -u):$(id -g)" --env-file /secure/dr-target.env \
  -v "$PWD:/tools:ro" -v /srv/dts-recovery:/recovery:ro \
  --entrypoint /tools/dts-s3-recovery dts/postgres:17 restore /recovery/set-001
```

开始写入前，工具校验恢复集、原密钥、其他数据库连接和两端空目标。向 S3 申请 `.dts-recovery-incomplete` 标记后，条件上传对象并逐一读回，比对成功后通过单事务恢复元数据，再读回对象、数据库版本和待回放引用，最后释放标记。失败时隔离新库和新前缀，暂停对外使用并保留现场；另选全新空库和桶重试。工具不会自动启动应用、修改源数据或删除半成品。恢复成功后核查目标写入围栏、未知提交、源日志保留和检查点，然后启动服务。

归档覆盖业务元数据与 S3 CDC Spool 的当前内容。历史版本、删除标记、对象策略/标签、WORM 验证报告及源端和目标端业务数据库各自使用其保留或备份机制。元数据库中的报告归档记录仍指向原报告 URI。数据库角色、部署 Secret 和 TLS/KMS 密钥独立备份。离线恢复目录和转运介质需加密保护。磁盘应能容纳整个前缀和元数据库归档，内存应能解密最大的单个待回放批次，并容纳对象与引用清单。

## 重建与回归

在代码仓库从根目录执行 `python tools/s3-recovery/build.py`。构建器使用 `.go-version` 和模块校验和并生成 Linux 静态二进制、依赖许可证与安装包校验文件。开发回归在独立 SeaweedFS 服务与 PostgreSQL 中创建新测试库和桶。集成测试的端口、隔离环境开关和测试身份在 `integration_test.go` 中，运行命令为 `go test -tags=integration -count=1 -v .`。
