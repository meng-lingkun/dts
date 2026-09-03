param(
    [string]$OutputDirectory = "",
    [ValidateSet("amd64")][string]$Architecture = "amd64",
    [switch]$FinalizeOnly
)

$ErrorActionPreference = "Stop"
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$repositoryRoot = [System.IO.Path]::GetFullPath((Join-Path $scriptDirectory "..\.."))
$version = (Get-Content -LiteralPath (Join-Path $repositoryRoot "VERSION") -Raw).Trim()
if (-not $OutputDirectory) { $OutputDirectory = Join-Path $repositoryRoot "dist" }
$outputRoot = [System.IO.Path]::GetFullPath($OutputDirectory)
$packageName = "qmigration-offline-$version-linux-$Architecture"
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
if (-not $FinalizeOnly) {
    if (Test-Path -LiteralPath $stage) { Remove-Item -LiteralPath $stage -Recurse -Force }
    if (Test-Path -LiteralPath $binaryDirectory) { Remove-Item -LiteralPath $binaryDirectory -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $stage, (Join-Path $stage "images"), (Join-Path $stage "bin"), (Join-Path $stage "migrations"), (Join-Path $stage "docs"), (Join-Path $stage "kubernetes"), $binaryDirectory | Out-Null
} else {
    if (-not (Test-Path -LiteralPath (Join-Path $stage "images"))) { throw "FinalizeOnly requires existing image archives in $stage" }
    New-Item -ItemType Directory -Force -Path (Join-Path $stage "bin"), (Join-Path $stage "migrations"), (Join-Path $stage "docs"), (Join-Path $stage "kubernetes") | Out-Null
}

if (-not $FinalizeOnly) {
Write-Host "[1/8] Building reproducible Web assets"
Push-Location (Join-Path $repositoryRoot "web")
try {
    npm ci --ignore-scripts --no-audit --no-fund
    if ($LASTEXITCODE -ne 0) { throw "npm ci failed" }
    npm run build
    if ($LASTEXITCODE -ne 0) { throw "npm run build failed" }
} finally {
    Pop-Location
}

Write-Host "[2/8] Cross-compiling Linux QMigration binaries"
$commands = @(
    "server", "worker", "qmigrationctl", "cdc-bridge", "binlog-inspect",
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
        $outputName = "qmigration-$command"
        if ($command -eq "server") { $outputName = "qmigration-server" }
        if ($command -eq "worker") { $outputName = "qmigration-worker" }
        if ($command -eq "qmigrationctl") { $outputName = "qmigrationctl" }
        go build -trimpath -ldflags="-s -w" -o (Join-Path $binaryDirectory $outputName) "./cmd/$command"
        if ($LASTEXITCODE -ne 0) { throw "Go build failed for $command" }
    }
} finally {
    Pop-Location
    $env:GOOS = $oldGoos
    $env:GOARCH = $oldGoarch
    $env:CGO_ENABLED = $oldCgo
}

Write-Host "[3/8] Creating Docker-compatible Linux image archives without a daemon"
Push-Location (Join-Path $repositoryRoot "tools\offline-image-builder")
try {
    go run . --binary-dir $binaryDirectory --web-dir (Join-Path $repositoryRoot "web\dist") --nginx-config (Join-Path $repositoryRoot "deployments\nginx.conf") --output-dir (Join-Path $stage "images") --version $version
    if ($LASTEXITCODE -ne 0) { throw "offline image builder failed" }
} finally {
    Pop-Location
}

}

Write-Host "[4/8] Staging pinned Docker Engine and Compose runtimes"
New-Item -ItemType Directory -Force -Path $runtimeDirectory | Out-Null
Copy-Item -LiteralPath (Join-Path $runtimeSource "versions.env"), (Join-Path $runtimeSource "docker.service") -Destination $runtimeDirectory
$runtimeValues = @{}
foreach ($line in Get-Content -LiteralPath (Join-Path $runtimeSource "versions.env")) {
    if ($line -match '^([A-Z0-9_]+)=(.+)$') { $runtimeValues[$Matches[1]] = $Matches[2] }
}
$dockerArchiveName = "docker-$($runtimeValues.DOCKER_ENGINE_VERSION).tgz"
$dockerArchive = Join-Path $runtimeDirectory $dockerArchiveName
$composeBinary = Join-Path $runtimeDirectory "docker-compose-linux-x86_64"
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

Get-VerifiedRuntimeFile -Uri "https://download.docker.com/linux/static/stable/x86_64/$dockerArchiveName" -Destination $dockerArchive -ExpectedSHA256 $runtimeValues.DOCKER_ENGINE_SHA256
Get-VerifiedRuntimeFile -Uri "https://github.com/docker/compose/releases/download/v$($runtimeValues.DOCKER_COMPOSE_VERSION)/docker-compose-linux-x86_64" -Destination $composeBinary -ExpectedSHA256 $runtimeValues.DOCKER_COMPOSE_SHA256
Get-VerifiedRuntimeFile -Uri "https://dl.k8s.io/release/v$($runtimeValues.KUBECTL_VERSION)/bin/linux/amd64/kubectl" -Destination $kubectlBinary -ExpectedSHA256 $runtimeValues.KUBECTL_SHA256
$dockerArchiveEntries = @(tar -tzf $dockerArchive)
foreach ($binary in "ctr", "docker", "docker-init", "containerd", "containerd-shim-runc-v2", "dockerd", "docker-proxy", "runc") {
    if ($dockerArchiveEntries -notcontains "docker/$binary") { throw "Docker runtime archive is missing $binary" }
}

Write-Host "[5/8] Staging installer and operational material"
foreach ($file in "docker-compose.offline.yml", "install.sh", "install-kubernetes.sh", "load-images-kubernetes.sh", "install-container-runtime.sh", "verify.sh", "uninstall.sh", "README.md") {
    Copy-Item -LiteralPath (Join-Path $scriptDirectory $file) -Destination $stage
}
Copy-Item -Path (Join-Path $repositoryRoot "deployments\kubernetes\*.yaml") -Destination (Join-Path $stage "kubernetes")
Copy-Item -LiteralPath (Join-Path $repositoryRoot "VERSION") -Destination $stage
Copy-Item -LiteralPath (Join-Path $binaryDirectory "qmigrationctl") -Destination (Join-Path $stage "bin")
Copy-Item -Path (Join-Path $repositoryRoot "backend\migrations\*.sql") -Destination (Join-Path $stage "migrations")
foreach ($doc in "USER_GUIDE.md", "MAINTENANCE_GUIDE.md", "PROJECT_ARCHITECTURE.md", "ARCHITECTURE_ASSESSMENT.md") {
    Copy-Item -LiteralPath (Join-Path $repositoryRoot "docs\$doc") -Destination (Join-Path $stage "docs")
}

Write-Host "[6/8] Writing package manifest"
$imageFiles = Get-ChildItem -LiteralPath (Join-Path $stage "images") -File | Sort-Object Name
$manifest = [ordered]@{
    name = "QMigration offline installation package"
    version = $version
    platform = "linux/$Architecture"
    created_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    base_images = @("alpine:3.22", "nginxinc/nginx-unprivileged:1.29-alpine", "postgres:17")
    container_runtime = [ordered]@{
        docker_engine_version = $runtimeValues.DOCKER_ENGINE_VERSION
        docker_engine_archive = "runtime/$dockerArchiveName"
        docker_engine_sha256 = $runtimeValues.DOCKER_ENGINE_SHA256
        docker_compose_version = $runtimeValues.DOCKER_COMPOSE_VERSION
        docker_compose_archive = "runtime/docker-compose-linux-x86_64"
        docker_compose_sha256 = $runtimeValues.DOCKER_COMPOSE_SHA256
        kubectl_version = $runtimeValues.KUBECTL_VERSION
        kubectl_archive = "runtime/kubectl"
        kubectl_sha256 = $runtimeValues.KUBECTL_SHA256
    }
    images = @($imageFiles | ForEach-Object {
        [ordered]@{ archive = "images/$($_.Name)"; bytes = $_.Length; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant() }
    })
}
$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $stage "manifest.json") -Encoding utf8

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
if (Test-Path -LiteralPath $archive) { Remove-Item -LiteralPath $archive -Force }
tar -czf $archive -C $outputRoot $packageName
if ($LASTEXITCODE -ne 0) { throw "tar archive creation failed" }
$archiveHash = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
[System.IO.File]::WriteAllText("$archive.sha256", "$archiveHash  $packageName.tar.gz`n", $ascii)

Write-Host "Package: $archive"
Write-Host "SHA-256: $archiveHash"
