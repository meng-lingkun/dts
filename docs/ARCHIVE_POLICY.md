# QMigration 开发版本归档策略

从 `V0.15.0-unified-dev9` 开始，每次开发执行结束都必须形成可恢复归档，而不是只保留工作目录。

每个版本固定产生五类产物：

1. `qmigration-<version>.zip`：完整源码快照，不包含 `.git/`、`bin/`、`data/`、`node_modules/`、`dist/`。
2. `qmigration-<previous>-to-<version>.patch`：上一开发版本到当前版本的增量 Binary Patch。
3. `qmigration-<version>.patch`：正式 `V0.13` 基线到当前版本的累计 Binary Patch，用于灾难恢复。
4. `qmigration-<version>.sha256`：归档文件 SHA-256。
5. `qmigration-<version>.manifest.json`：版本、上一版本、文件大小、SHA-256 和恢复验证结果。

归档不是“生成文件即成功”。两个 Patch 都必须从干净基线执行：

```text
git apply --check
    ↓
git apply
    ↓
恢复源码与当前源码逐文件比较
    ↓
go test ./...
    ↓
go vet ./...
```

任意一步失败，都不得把该版本标记为已归档。

仓库提供统一脚本：

```bash
deployments/scripts/archive-version.sh \
  --source /path/to/current \
  --previous /path/to/previous \
  --baseline /path/to/qmigration-v0.13 \
  --out /path/to/archive-output
```

Patch 生成时使用临时 Git Index 和 `git diff --cached --binary --full-index`，确保新增文件、删除文件以及二进制变更不会像普通工作目录 diff 那样被遗漏。
