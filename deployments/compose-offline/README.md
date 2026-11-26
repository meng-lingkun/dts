# DTS 单机 Compose 离线包

安装包的 `tools/` 目录附带适用于 S3 Spool 的 PostgreSQL 联合恢复工具。解压工具后可按其中的中文说明，使用随包 PostgreSQL 镜像执行备份和恢复。

在 Linux x86_64 上部署 PostgreSQL 17、DTS Server、Worker 和 Web。包内提供 Docker Engine 29.7.2 和 Compose 2.40.2。安装使用 Linux 基础命令和 util-linux 的 flock，首次安装运行时需要 root、systemd、iptables。使用已有 Docker Engine 24+ 和 Compose v2 时，当前用户需要有权访问本地 Docker 套接字。

核对随包的外部 SHA-256，解压并在包目录运行 `sudo sh install.sh`。安装器核对文件、导入镜像、以项目名 `dts-offline` 启动服务，等待就绪并验证 API 和 Web。地址为 `http://127.0.0.1:8088` 和 `http://127.0.0.1:8080/readyz`。初始管理员为 `admin`，密码在模式 600 的 `.env` 中的 `DTS_BOOTSTRAP_ADMIN_PASSWORD`。妥善备份该文件、PostgreSQL 和 Spool 卷。

麒麟 V10 SP3 2403 x86_64 基础系统缺少 iptables 或 flock 时，可显式执行 `sudo sh install.sh --install-kylin-deps`。该选项仅启用包内的 RPM 仓库，由系统 yum/dnf 验证发行版 RPM 签名，限制为新增缺少的软件包及依赖。其他发行版请使用对应可信系统仓库准备 iptables 与 util-linux。主机还需满足所选 Docker Engine 的内核、cgroup、存储及网络条件。

`sh verify.sh` 可重复执行。`sh uninstall.sh` 停止本项目并保留卷和密钥。`sh uninstall.sh --purge-data` 会删除本项目的数据卷、Spool 和本目录的 `.env`，再次安装将生成随机新凭据。运行时可供其他项目使用。旧 QMigration/RC49 部署的数据升级需要单独按迁移指南执行。

备份时先停止任务、Server、Worker 及所有 CDC 写入，保持 PostgreSQL 运行。将 `dts-offline_spool` 挂载到具备 GNU tar、coreutils 和 PostgreSQL 客户端的维护容器，接入本项目网络。在容器内设置 `DTS_METADATA_{HOST,PORT,USER,DATABASE,PASSWORD}`、`DTS_MASTER_KEY`、`DTS_CDC_SPOOL_DIR`、`DTS_WRITERS_STOPPED=YES`，运行 `sh scripts/recovery-set.sh backup /backup/new-set`。将 `.env` 单独纳入受控凭据备份。可使用 `verify` 验证归档。恢复操作要求独立空库、空 Spool、原密钥和 `DTS_RESTORE_CONFIRM=YES`。在进程停止期间运行 `sh scripts/recovery-set.sh restore /backup/new-set`，核查目标围栏代次、未知提交和源日志留存后再启动服务。

构建机可使用 Python 3.11+ 从同版本完整 Kubernetes 包的解压目录生成 Compose 包。

```sh
python deployments/compose-offline/build.py --package dist/dts-kubernetes-offline-0.15.0-rc50-linux-amd64
```

添加 `--runtime-cache DIR` 可使用 DIR 下已有的 `docker-29.7.2.tgz` 和 `docker-compose-linux-x86_64`，构建程序会核对两个文件的固定 SHA-256。
