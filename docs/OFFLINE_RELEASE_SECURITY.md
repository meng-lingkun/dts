# 离线包 SBOM、扫描与签名

Kubernetes 和 Compose 离线发布包共用 `tools/release-security/release.py`，构建完成后针对最终 tar.gz 生成独立证据目录。扫描在发布机运行，交付现场用 Python 3.11+、OpenSSL 3 和独立可信渠道发放的 Ed25519 PEM 公钥验签。

```sh
python tools/release-security/release.py scan --package "$PACKAGE" --out evidence
python tools/release-security/release.py sign --package "$PACKAGE" --out evidence --key /secure/release-ed25519.pem
python tools/release-security/release.py verify --package "$PACKAGE" --out evidence --key /trusted/release-public.pem
```

扫描读取归档中的全部常规文件，并为文件清单生成 CycloneDX SBOM。多镜像 Docker 归档按镜像拆分后交给 Trivy，分别生成依赖 SBOM 和漏洞明细。扫描发现 HIGH/CRITICAL 时返回非零并保留证据。扫描器、漏洞数据库和实际包缺一项都不能完成签名流程。包内独立运行时和 RPM 的文件级 SBOM 提供名称与哈希，逐包操作系统依赖漏洞检测的覆盖范围是容器镜像。

`scan-summary.json` 记录扫描时间、归档 SHA-256、Trivy 与漏洞数据库版本、各镜像的 HIGH/CRITICAL 数量、软件包版本、修复版本和完整报告的相对路径。计数单位为每个镜像、软件目标和软件包中的发现，跨镜像去重后的漏洞 ID 数单列。“无修复版本”指本次数据库记录没有给出 FixedVersion。

`release.ed25519` 签署清单的原始字节，涵盖整包 SHA-256、所有镜像标签及不可变配置 ID、SBOM 和扫描结果哈希。现场应先验整包，再加载镜像。加载后可按已签名标签检查本地镜像 ID。

```sh
tag=dts/server:0.15.0-rc50
python tools/release-security/release.py verify-image --out evidence --key /trusted/release-public.pem \
  --tag "$tag" --image-id "$(docker image inspect --format '{{.Id}}' "$tag")"
```

GitHub Actions 的 **Offline release signatures and image scanning** 工作流从同一仓库指定的构建运行读取归档，并校验发布方给出的 SHA-256。发布环境 `release-signing` 使用独立的 `DTS_RELEASE_ED25519_PRIVATE_PEM` Secret 和 `DTS_RELEASE_ED25519_PUBLIC_PEM` Variable。签名私钥和安装包应由不同渠道管理。

仅生成文件 SBOM 时使用 `inventory --package "$PACKAGE" --out package.cdx.json`。同时生成逐镜像依赖 SBOM 时使用 `sbom --package "$PACKAGE" --out sbom-dir`。离线扫描可通过环境变量 `TRIVY_CACHE_DIR` 指向预先更新的漏洞库，并给 `scan` 加 `--offline-db`。Scanner 数据源、缓存和离线更新说明见 [Trivy 离线运行说明](https://trivy.dev/docs/v0.55/advanced/air-gap/)。
