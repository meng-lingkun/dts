param(
    [string]$OutputDirectory = "",
    [ValidateSet("amd64")][string]$Architecture = "amd64",
    [switch]$FinalizeOnly,
    [string]$ClusterPayloadDirectory = "",
    [string]$CachedPackageDirectory = "",
    [string]$RefreshedImageDirectory = ""
)

$ErrorActionPreference = "Stop"
if ($CachedPackageDirectory -and ($FinalizeOnly -or $ClusterPayloadDirectory)) {
    throw "CachedPackageDirectory is a full rebuild mode and supplies its own cluster payload."
}
if ($RefreshedImageDirectory -and -not $CachedPackageDirectory) {
    throw "RefreshedImageDirectory requires CachedPackageDirectory."
}
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$repositoryRoot = [System.IO.Path]::GetFullPath((Join-Path $scriptDirectory "..\.."))
$version = (Get-Content -LiteralPath (Join-Path $repositoryRoot "VERSION") -Raw).Trim()
$goVersion = (Get-Content -LiteralPath (Join-Path $repositoryRoot ".go-version") -Raw).Trim()

function Invoke-BuildGo {
    $previousToolchain = $env:GOTOOLCHAIN
    try {
        $env:GOTOOLCHAIN = "go$goVersion"
        & go @args
        if ($LASTEXITCODE -ne 0) { throw "Go $goVersion command failed: $($args[0])" }
    } finally {
        $env:GOTOOLCHAIN = $previousToolchain
    }
}
if (-not $OutputDirectory) { $OutputDirectory = Join-Path $repositoryRoot "dist" }
$outputRoot = [System.IO.Path]::GetFullPath($OutputDirectory)
$packageName = "dts-kubernetes-offline-$version-linux-$Architecture"
$stage = [System.IO.Path]::GetFullPath((Join-Path $outputRoot $packageName))
$binaryDirectory = Join-Path $outputRoot ".offline-linux-$Architecture-bin"
$archive = Join-Path $outputRoot "$packageName.tar.gz"
$runtimeSource = Join-Path $scriptDirectory "runtime"
$runtimeDirectory = Join-Path $stage "runtime"

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
if (-not $stage.StartsWith($outputRoot + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase) -or
    -not ([System.IO.Path]::GetFullPath($binaryDirectory)).StartsWith($outputRoot + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase) -or
    -not ([System.IO.Path]::GetFullPath($archive)).StartsWith($outputRoot + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to clean stage outside output directory: $stage"
}
if ($CachedPackageDirectory) {
    $CachedPackageDirectory = (Resolve-Path -LiteralPath $CachedPackageDirectory).Path
    if (($CachedPackageDirectory + '\').StartsWith($stage + '\', [StringComparison]::OrdinalIgnoreCase) -or
        ($CachedPackageDirectory + '\').StartsWith($binaryDirectory + '\', [StringComparison]::OrdinalIgnoreCase)) {
        throw "Runtime cache must be outside the build stage and binary directory."
    }
}
if (-not $FinalizeOnly) {
    if (Test-Path -LiteralPath $stage) { Remove-Item -LiteralPath $stage -Recurse -Force }
    if (Test-Path -LiteralPath $binaryDirectory) { Remove-Item -LiteralPath $binaryDirectory -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $stage, (Join-Path $stage "images"), (Join-Path $stage "bin"), (Join-Path $stage "migrations"), (Join-Path $stage "docs"), (Join-Path $stage "kubernetes"), $binaryDirectory | Out-Null
} else {
    if (-not (Test-Path -LiteralPath (Join-Path $stage "images"))) { throw "FinalizeOnly requires existing image archives in $stage" }
    foreach ($runtimeSecret in 'INITIAL_ADMIN_CREDENTIALS.txt', '.env') {
        if (Test-Path -LiteralPath (Join-Path $stage $runtimeSecret)) {
            throw "Refusing to package an installed directory containing $runtimeSecret. Finalize a clean build stage."
        }
    }
    New-Item -ItemType Directory -Force -Path (Join-Path $stage "bin"), (Join-Path $stage "migrations"), (Join-Path $stage "docs"), (Join-Path $stage "kubernetes") | Out-Null
}

if (-not $FinalizeOnly) {
Write-Host "[1/8] Building reproducible Web assets"
Push-Location (Join-Path $repositoryRoot "web")
try {
    npm ci --prefer-offline --ignore-scripts --no-audit --no-fund
    if ($LASTEXITCODE -ne 0) { throw "npm ci failed" }
    npm run build
    if ($LASTEXITCODE -ne 0) { throw "npm run build failed" }
} finally {
    Pop-Location
}

Write-Host "[2/8] Cross-compiling Linux DTS binaries"
$commands = @(
    "server", "worker", "dtsctl", "certgen", "cdc-bridge", "binlog-inspect",
    "mysql-cdc", "tidb-cdc", "postgres-cdc", "opengauss-cdc", "gaussdb-cdc",
    "sqlserver-cdc", "oracle-cdc", "db2-cdc", "dameng-cdc", "gbase-cdc", "gbase8s-cdc"
)
$oldGoos = $env:GOOS
$oldGoarch = $env:GOARCH
$oldCgo = $env:CGO_ENABLED
$env:GOOS = "linux"
$env:GOARCH = $Architecture
$env:CGO_ENABLED = "0"
Push-Location (Join-Path $repositoryRoot "backend")
try {
    foreach ($command in $commands) {
        $outputName = "dts-$command"
        if ($command -eq "server") { $outputName = "dts-server" }
        if ($command -eq "worker") { $outputName = "dts-worker" }
        if ($command -eq "dtsctl") { $outputName = "dtsctl" }
        Invoke-BuildGo build -trimpath -ldflags="-s -w" -o (Join-Path $binaryDirectory $outputName) "./cmd/$command"
    }
} finally {
    Pop-Location
    $env:GOOS = $oldGoos
    $env:GOARCH = $oldGoarch
    $env:CGO_ENABLED = $oldCgo
}

Write-Host "[3/8] Creating Docker-compatible Linux image archives without a daemon"
if ($CachedPackageDirectory) {
    $imageArguments = @()
    if ($RefreshedImageDirectory) { $imageArguments = @("--runtime-images", $RefreshedImageDirectory) }
    python (Join-Path $scriptDirectory "refresh-cached-runtime.py") --cache $CachedPackageDirectory --stage $stage --bin $binaryDirectory --web (Join-Path $repositoryRoot "web/dist") --nginx (Join-Path $repositoryRoot "deployments/nginx.conf") --version $version @imageArguments
    if ($LASTEXITCODE -ne 0) { throw "Offline application rebuild failed" }
} else {
Push-Location (Join-Path $repositoryRoot "tools\offline-image-builder")
try {
    Invoke-BuildGo run . --binary-dir $binaryDirectory --web-dir (Join-Path $repositoryRoot "web\dist") --nginx-config (Join-Path $repositoryRoot "deployments\nginx.conf") --output-dir (Join-Path $stage "images") --version $version
} finally {
    Pop-Location
}
}

}

Write-Host "[4/8] Staging pinned Kubernetes client"
New-Item -ItemType Directory -Force -Path $runtimeDirectory | Out-Null
Copy-Item -LiteralPath (Join-Path $runtimeSource "versions.env") -Destination $runtimeDirectory
$runtimeValues = @{}
foreach ($line in Get-Content -LiteralPath (Join-Path $runtimeSource "versions.env")) {
    if ($line -match '^([A-Z0-9_]+)=(.+)$') { $runtimeValues[$Matches[1]] = $Matches[2] }
}
$kubectlBinary = Join-Path $runtimeDirectory "kubectl"

function Get-VerifiedRuntimeFile {
    param([string]$Uri, [string]$Destination, [string]$ExpectedSHA256)
    if (Test-Path -LiteralPath $Destination) {
        $currentHash = (Get-FileHash -LiteralPath $Destination -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($currentHash -eq $ExpectedSHA256) { return }
    }
    $temporary = "$Destination.download"
    for ($attempt = 1; $attempt -le 6; $attempt++) {
        try {
            Invoke-WebRequest -Uri $Uri -OutFile $temporary -TimeoutSec 600
            $downloadHash = (Get-FileHash -LiteralPath $temporary -Algorithm SHA256).Hash.ToLowerInvariant()
            if ($downloadHash -ne $ExpectedSHA256) { throw "SHA-256 mismatch for $Uri" }
            Move-Item -LiteralPath $temporary -Destination $Destination -Force
            return
        } catch {
            if ($attempt -eq 6) { throw }
            Start-Sleep -Seconds ($attempt * 2)
        }
    }
}

Get-VerifiedRuntimeFile -Uri "https://dl.k8s.io/release/v$($runtimeValues.KUBECTL_VERSION)/bin/linux/amd64/kubectl" -Destination $kubectlBinary -ExpectedSHA256 $runtimeValues.KUBECTL_SHA256
foreach ($tool in "kubeadm", "kubelet") {
    Get-VerifiedRuntimeFile -Uri "https://dl.k8s.io/release/v$($runtimeValues.KUBECTL_VERSION)/bin/linux/amd64/$tool" -Destination (Join-Path $runtimeDirectory $tool) -ExpectedSHA256 $runtimeValues["$($tool.ToUpperInvariant())_SHA256"]
}
Write-Host "[5/8] Staging installer and operational material"
python (Join-Path $repositoryRoot "tools/s3-recovery/build.py") --out (Join-Path $stage "tools")
if ($LASTEXITCODE -ne 0) { throw "S3 recovery tool build failed" }
foreach ($file in "install.sh", "install-kubernetes.sh", "install-kubernetes-tools.sh", "load-images-kubernetes.sh", "verify.sh", "uninstall.sh", "uninstall-all.sh", "README.md") {
    Copy-Item -LiteralPath (Join-Path $scriptDirectory $file) -Destination $stage
}
Copy-Item -Path (Join-Path $repositoryRoot "deployments\kubernetes\*.yaml") -Destination (Join-Path $stage "kubernetes")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "VERSION") -Destination $stage
if (-not $FinalizeOnly) {
    foreach ($binary in "dtsctl", "dts-certgen") {
        Copy-Item -LiteralPath (Join-Path $binaryDirectory $binary) -Destination (Join-Path $stage "bin")
    }
} elseif (-not (Test-Path -LiteralPath (Join-Path $stage "bin/dtsctl")) -or -not (Test-Path -LiteralPath (Join-Path $stage "bin/dts-certgen"))) {
    throw "FinalizeOnly requires staged dtsctl and dts-certgen binaries from the matching release"
}
Copy-Item -Path (Join-Path $repositoryRoot "backend\migrations\*.sql") -Destination (Join-Path $stage "migrations")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "backend/internal/repository/postgres/schema.sql") -Destination (Join-Path $stage "migrations/schema.sql")
New-Item -ItemType Directory -Force -Path (Join-Path $stage "scripts") | Out-Null
Copy-Item -LiteralPath (Join-Path $repositoryRoot "deployments/scripts/migrate-metadata.sh") -Destination (Join-Path $stage "scripts")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "deployments/scripts/recovery-set.sh") -Destination (Join-Path $stage "scripts")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "deployments/kubernetes/README.md") -Destination (Join-Path $stage "kubernetes/README.md")
foreach ($doc in "USER_GUIDE.md", "MAINTENANCE_GUIDE.md", "PROJECT_ARCHITECTURE.md", "ARCHITECTURE_ASSESSMENT.md", "DTS_RENAME_UPGRADE.md", "ENTERPRISE_HARDENING_RC50.md", "OFFLINE_RELEASE_SECURITY.md", "SUPPORT_MATRIX_V0.15_RC50.md", "openapi.json") {
    Copy-Item -LiteralPath (Join-Path $repositoryRoot "docs\$doc") -Destination (Join-Path $stage "docs")
}

New-Item -ItemType Directory -Force -Path (Join-Path $stage "tools/release-security") | Out-Null
Copy-Item -LiteralPath (Join-Path $repositoryRoot "tools/release-security/release.py") -Destination (Join-Path $stage "tools/release-security")
Write-Host "[6/8] Writing package manifest"
if ($ClusterPayloadDirectory) {
    $clusterSource = [System.IO.Path]::GetFullPath($ClusterPayloadDirectory)
    foreach ($required in "runtime/containerd-static-2.3.5-linux-amd64.tar.gz", "runtime/runc.amd64", "runtime/crictl-v1.32.0-linux-amd64.tar.gz", "runtime/cni-plugins-linux-amd64-v1.9.1.tgz", "network/kube-flannel.yaml", "rpms/repodata/repomd.xml", "kylin-rpms.json", "upstream-tools.json") {
    if (-not (Test-Path -LiteralPath (Join-Path $clusterSource $required))) { throw "Missing cluster payload: $required" }
    }
    if (Get-ChildItem -LiteralPath $clusterSource -Recurse -File | Where-Object { $_.Name -match '\.(download|partial)$' }) { throw "Incomplete cluster payload files" }
    $repomdText = [System.IO.File]::ReadAllText((Join-Path $clusterSource "rpms/repodata/repomd.xml"))
    if ($repomdText -notmatch '<repomd\s+xmlns=["'']http://linux\.duke\.edu/metadata/repo["'']' -or $repomdText -match '<ns\d+:repomd') {
        throw "Kylin RPM repomd.xml must use the canonical default namespace; rebuild the cluster payload"
    }
    $clusterStage = Join-Path $stage "cluster"
    New-Item -ItemType Directory -Path $clusterStage -Force | Out-Null
    foreach ($directory in "runtime", "network", "rpms", "images") {
        Copy-Item -LiteralPath (Join-Path $clusterSource $directory) -Destination $clusterStage -Recurse -Force
    }
    foreach ($file in "kylin-rpms.json", "upstream-tools.json", "image-provenance.json") { Copy-Item -LiteralPath (Join-Path $clusterSource $file) -Destination $clusterStage }
    Copy-Item -Path (Join-Path $scriptDirectory "cluster/*") -Destination $clusterStage -Force
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "cluster-images.txt") -Destination $clusterStage
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "install-cluster.sh"), (Join-Path $scriptDirectory "configure-kylin-419-cgroup.sh"), (Join-Path $scriptDirectory "restore-kylin-419-cgroup.sh") -Destination $stage
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "install-registry.sh"), (Join-Path $scriptDirectory "configure-registry.sh") -Destination $stage
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "deploy.sh"), (Join-Path $scriptDirectory "unpack-images.sh") -Destination $stage
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "automation") -Destination $stage -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "CLUSTER_INSTALL.md") -Destination $stage
    $imageRefs = Get-Content -LiteralPath (Join-Path $scriptDirectory "cluster-images.txt")
    foreach ($ref in $imageRefs) {
        $file = $ref.Replace('/', '_').Replace(':', '_') + '.tar'
        if (-not (Test-Path -LiteralPath (Join-Path $clusterStage "images/$file"))) { throw "Missing cluster image $ref" }
    }
    $combinedTar = Join-Path $stage "images/all-images.tar"
    $combinedGzip = "$combinedTar.gz"
    $mergeInputs = @(Get-ChildItem -LiteralPath (Join-Path $stage "images"), (Join-Path $clusterStage "images") -Filter '*.tar' -File | Where-Object Name -ne 'all-images.tar' | ForEach-Object FullName)
    $previousTar = Join-Path $stage '.previous-images.tar'
    if (-not (Get-ChildItem -LiteralPath (Join-Path $stage "images") -Filter 'dts-server-*.tar') -and (Test-Path -LiteralPath $combinedGzip)) {
        $inputStream = [IO.File]::OpenRead($combinedGzip)
        $inflate = [IO.Compression.GZipStream]::new($inputStream, [IO.Compression.CompressionMode]::Decompress)
        $outputStream = [IO.File]::Create($previousTar)
        try { $inflate.CopyTo($outputStream) } finally { $outputStream.Dispose(); $inflate.Dispose(); $inputStream.Dispose() }
        $mergeInputs += $previousTar
    }
    $mergeList = Join-Path $stage '.merge-inputs.txt'
    [IO.File]::WriteAllLines($mergeList, $mergeInputs, [Text.UTF8Encoding]::new($false))
    Push-Location (Join-Path $repositoryRoot 'tools/offline-image-builder')
    try {
        Invoke-BuildGo run . --merge-list $mergeList --merged-archive $combinedTar
    } finally { Pop-Location }
    if (Test-Path -LiteralPath "$combinedTar.sha256") { Remove-Item -LiteralPath "$combinedTar.sha256" -Force }
    $inputStream = [IO.File]::OpenRead($combinedTar)
    $outputStream = [IO.File]::Create($combinedGzip)
    $deflate = [IO.Compression.GZipStream]::new($outputStream, [IO.Compression.CompressionLevel]::Optimal)
    try { $inputStream.CopyTo($deflate) } finally { $deflate.Dispose(); $outputStream.Dispose(); $inputStream.Dispose() }
    foreach ($file in @($mergeInputs) + @($mergeList, $combinedTar)) {
        if (-not ([IO.Path]::GetFullPath($file)).StartsWith($stage + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) { throw 'Refusing cleanup outside stage' }
        Remove-Item -LiteralPath $file -Force
    }
} elseif (($FinalizeOnly -or $CachedPackageDirectory) -and (Test-Path -LiteralPath (Join-Path $stage "cluster/runtime/containerd-static-2.3.5-linux-amd64.tar.gz"))) {
    # Operational-only releases reuse the already verified unified image archive and
    # cluster payload, but must refresh every script, inventory and configuration file.
    $clusterStage = Join-Path $stage "cluster"
    Copy-Item -Path (Join-Path $scriptDirectory "cluster/*") -Destination $clusterStage -Force
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "cluster-images.txt") -Destination $clusterStage -Force
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "install-cluster.sh"), (Join-Path $scriptDirectory "configure-kylin-419-cgroup.sh"), (Join-Path $scriptDirectory "restore-kylin-419-cgroup.sh") -Destination $stage -Force
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "install-registry.sh"), (Join-Path $scriptDirectory "configure-registry.sh") -Destination $stage -Force
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "deploy.sh"), (Join-Path $scriptDirectory "unpack-images.sh") -Destination $stage -Force
    if (Test-Path -LiteralPath (Join-Path $stage "automation")) { Remove-Item -LiteralPath (Join-Path $stage "automation") -Recurse -Force }
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "automation") -Destination $stage -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $scriptDirectory "CLUSTER_INSTALL.md") -Destination $stage -Force
}
$hasClusterPayload = Test-Path -LiteralPath (Join-Path $stage "cluster/runtime/containerd-static-2.3.5-linux-amd64.tar.gz")
$imageFiles = Get-ChildItem -LiteralPath (Join-Path $stage "images") -File | Where-Object { $_.Extension -in '.tar', '.gz' } | Sort-Object Name
$manifest = [ordered]@{
    name = "DTS offline installation package"
    version = $version
    platform = "linux/$Architecture"
    created_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    base_images = @("alpine:3.22", "nginx/nginx-unprivileged:stable-alpine-slim@sha256:64a2a533a92f64b868d5f6d262dbc8c9bdc0302154f631389170ac409761047a", "postgres:17")
    deployment = "kubernetes"
    node_runtime = "containerd (provided by existing Kubernetes cluster)"
    cgroup_profiles = @("Kylin 4.19/cgroup v1 compatibility", "kernel 5.10+/cgroup v2")
    cluster_payload = [bool]$hasClusterPayload
    target_os = "Kylin V10 SP3 2403 x86_64"
    kubernetes_client = [ordered]@{
        kubectl_version = $runtimeValues.KUBECTL_VERSION
        kubectl_archive = "runtime/kubectl"
        kubectl_sha256 = $runtimeValues.KUBECTL_SHA256
        kubeadm_archive = "runtime/kubeadm"
        kubeadm_sha256 = $runtimeValues.KUBEADM_SHA256
        kubelet_archive = "runtime/kubelet"
        kubelet_sha256 = $runtimeValues.KUBELET_SHA256
    }
    images = @($imageFiles | ForEach-Object {
        [ordered]@{ archive = "images/$($_.Name)"; bytes = $_.Length; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant() }
    })
}
if ($hasClusterPayload) { $manifest.node_runtime = "containerd 2.3.5 bundled; offline kubeadm bootstrap available" }
if (Test-Path -LiteralPath (Join-Path $runtimeDirectory 'image-cache-provenance.json')) {
    $manifest.Remove('base_images')
    $manifest.image_provenance = 'runtime/image-cache-provenance.json'
}
$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $stage "manifest.json") -Encoding utf8

# Fail closed when an old Compose payload is accidentally reused.
foreach ($obsolete in "docker-compose.offline.yml", "install-container-runtime.sh", "runtime/docker.service", "runtime/docker-compose-linux-x86_64") {
    if (Test-Path -LiteralPath (Join-Path $stage $obsolete)) { throw "Obsolete runtime payload in stage: $obsolete. Use a clean Kubernetes stage." }
}
if (Get-ChildItem -LiteralPath $runtimeDirectory -Filter "docker-*.tgz") { throw "Docker Engine must not be shipped in the Kubernetes package" }
# Normalize Linux text assets even when checked out with core.autocrlf=true.
$utf8 = [System.Text.UTF8Encoding]::new($false)
Get-ChildItem -LiteralPath $stage -File -Recurse | Where-Object { $_.Extension -in ".sh", ".env", ".yaml", ".yml", ".md", ".sql", ".json", ".service", ".toml", ".txt" -or $_.Name -eq "VERSION" } | ForEach-Object {
    $content = [System.IO.File]::ReadAllText($_.FullName).Replace("`r`n", "`n")
    [System.IO.File]::WriteAllText($_.FullName, $content, $utf8)
}
Write-Host "[7/8] Writing SHA-256 inventory"
$checksumLines = Get-ChildItem -LiteralPath $stage -File -Recurse | Where-Object Name -ne "SHA256SUMS" | Sort-Object FullName | ForEach-Object {
    $relative = [System.IO.Path]::GetRelativePath($stage, $_.FullName).Replace("\", "/")
    "$((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant())  $relative"
}
$ascii = [System.Text.ASCIIEncoding]::new()
$checksumPath = Join-Path $stage "SHA256SUMS"
[System.IO.File]::WriteAllText($checksumPath, (($checksumLines -join "`n") + "`n"), $ascii)
if ([System.IO.File]::ReadAllBytes($checksumPath) -contains 13) {
    throw "SHA256SUMS contains a CR byte; Linux checksum verification would fail"
}

Write-Host "[8/8] Creating final archive"
$temporaryArchive = "$archive.$([Guid]::NewGuid().ToString('N')).tmp"
try {
    tar -czf $temporaryArchive -C $outputRoot $packageName
    if ($LASTEXITCODE -ne 0) { throw "tar archive creation failed" }
    $archiveHash = (Get-FileHash -LiteralPath $temporaryArchive -Algorithm SHA256).Hash.ToLowerInvariant()
    [System.IO.File]::WriteAllText("$temporaryArchive.sha256", "$archiveHash  $packageName.tar.gz`n", $ascii)
    [System.IO.File]::Move($temporaryArchive, $archive, $true)
    [System.IO.File]::Move("$temporaryArchive.sha256", "$archive.sha256", $true)
} finally {
    [System.IO.File]::Delete($temporaryArchive)
    [System.IO.File]::Delete("$temporaryArchive.sha256")
}

Write-Host "Package: $archive"
Write-Host "SHA-256: $archiveHash"
