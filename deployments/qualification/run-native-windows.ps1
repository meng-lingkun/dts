# Run native database acceptance against two disposable, loopback-only instances.
# Use -DatasourceTLS to run only the TLS suite. Put openssl.exe on PATH and
# supply OpenSSL-enabled PostgreSQL and MySQL binaries. This creates temporary
# CAs inside the test directory and never imports them into the OS trust store.
param(
    [Parameter(Mandatory)][string]$PostgresBin,
    [Parameter(Mandatory)][string]$MySQLBin,
    [ValidateRange(1,65535)][int]$PostgresPort = 25432,
    [ValidateRange(1,65535)][int]$MySQLPort = 23306,
    [switch]$ControlProcesses,
    [switch]$DatasourceTLS,
    [switch]$WorkerTakeover,
    [ValidateRange(0,259200)][int]$ContinuousCDCSeconds=0
)
$ErrorActionPreference = 'Stop'
if ($DatasourceTLS -and ($ControlProcesses -or $ContinuousCDCSeconds -or $WorkerTakeover)) { throw 'Run the TLS-only fixture separately from process supervision and continuous CDC' }
if ($ContinuousCDCSeconds -gt 0 -and $ContinuousCDCSeconds -lt 5) { throw 'Continuous CDC requires at least 5 seconds' }
if (!$DatasourceTLS -and !(Get-Command zstd.exe -ErrorAction SilentlyContinue)) { throw 'Put zstd.exe on PATH for MySQL compressed CDC acceptance' }
if ($DatasourceTLS -and !(Get-Command openssl.exe -ErrorAction SilentlyContinue)) { throw 'Put openssl.exe on PATH for the temporary test certificates' }
if ($PostgresPort -eq $MySQLPort) { throw 'Choose distinct database ports' }
$occupied = [Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
if ($occupied | Where-Object Port -in $PostgresPort,$MySQLPort) { throw 'A requested test port is in use' }
$lab = New-Item -ItemType Directory -Path (Join-Path ([IO.Path]::GetTempPath()) ("dts-native-" + [guid]::NewGuid().ToString('N')))
Write-Output "Qualification evidence: $lab"
$pgData = Join-Path $lab 'pg'
$myData = Join-Path $lab 'mysql'
$secret = [guid]::NewGuid().ToString('N')
$pwFile = Join-Path $lab 'initdb.pw'
$pgStarted = $false
$mysql = $null
$testExit = 1
$testEnvNames = @(
    'DTS_E2E_ISOLATED','DTS_E2E_CONTROL_PROCESSES','DTS_E2E_POSTGRES_HOST','DTS_E2E_MYSQL_HOST',
    'DTS_E2E_POSTGRES_PASSWORD','DTS_E2E_MYSQL_PASSWORD','DTS_E2E_MYSQL_PORT','DTS_E2E_POSTGRES_PORT',
    'DTS_E2E_POSTGRES_DATABASE','DTS_TEST_POSTGRES_HOST','DTS_TEST_POSTGRES_PORT','DTS_TEST_POSTGRES_USER',
    'DTS_TEST_POSTGRES_PASSWORD','DTS_TEST_POSTGRES_DATABASE','MYSQL_PWD','DTS_E2E_CONTINUOUS_SECONDS','DTS_E2E_CONTINUOUS_EVIDENCE',
    'DTS_E2E_TLS_CA_FILE','DTS_E2E_TLS_WRONG_CA_FILE','DTS_E2E_WORKER_TAKEOVER'
)
$previousEnv = @{}
foreach ($key in $testEnvNames) {
    $previousEnv[$key] = [Environment]::GetEnvironmentVariable($key,'Process')
}
try {
    Remove-Item -LiteralPath 'Env:MYSQL_PWD' -ErrorAction SilentlyContinue
    [IO.File]::WriteAllText($pwFile, $secret)
    & "$PostgresBin/initdb.exe" -D $pgData -U postgres -A scram-sha-256 "--pwfile=$pwFile" --encoding=UTF8 --locale=C
    if ($LASTEXITCODE) { throw 'initdb failed' }
    Remove-Item -LiteralPath $pwFile
    $pgOptions="-p $PostgresPort -h 127.0.0.1 -c wal_level=logical -c max_prepared_transactions=10"
    $myTLSOptions=@()
    if ($DatasourceTLS) {
        & (Join-Path $PSScriptRoot 'new-tls-fixture.ps1') -Directory $lab.FullName
        Copy-Item -LiteralPath (Join-Path $lab 'server.crt'),(Join-Path $lab 'server.key') -Destination $pgData
        $pgOptions+=' -c ssl=on'
        $hba=Join-Path $pgData 'pg_hba.conf'
        [IO.File]::WriteAllText($hba, "hostnossl all all 127.0.0.1/32 reject`n"+[IO.File]::ReadAllText($hba))
        $env:DTS_E2E_TLS_CA_FILE=Join-Path $lab 'ca.crt'
        $env:DTS_E2E_TLS_WRONG_CA_FILE=Join-Path $lab 'wrong-ca.crt'
        $myTLSOptions=@('--require-secure-transport=ON',('--ssl-ca="{0}/ca.crt"' -f $lab),('--ssl-key="{0}/server.key"' -f $lab),('--ssl-cert="{0}/server.crt"' -f $lab))
    } else {
        Remove-Item -LiteralPath Env:DTS_E2E_TLS_CA_FILE -ErrorAction SilentlyContinue
    }
    & "$PostgresBin/pg_ctl.exe" -D $pgData -l (Join-Path $lab 'pg.log') -o $pgOptions -w start
    if ($LASTEXITCODE) { throw 'PostgreSQL failed to start' }
    $pgStarted = $true
    & "$MySQLBin/mysqld.exe" --no-defaults --initialize-insecure "--datadir=$myData" "--log-error=$lab/mysql-init.log"
    if ($LASTEXITCODE) { throw 'mysqld initialization failed' }
    $myArgs=@('--no-defaults', ('--datadir="{0}"' -f $myData), "--port=$MySQLPort", '--bind-address=127.0.0.1', '--mysqlx=OFF', '--server-id=521', '--gtid-mode=ON', '--enforce-gtid-consistency=ON', '--binlog-row-image=FULL', ('--log-error="{0}/mysql.log"' -f $lab)) + $myTLSOptions
    $mysql = Start-Process -WindowStyle Hidden -PassThru -FilePath "$MySQLBin/mysqld.exe" -ArgumentList $myArgs
    $clientArgs = @('--no-defaults','--host=127.0.0.1',"--port=$MySQLPort",'--user=root')
    for ($i=0; $i -lt 60 -and !$mysql.HasExited; $i++) {
        & "$MySQLBin/mysqladmin.exe" @clientArgs ping --silent 2>$null
        if (!$LASTEXITCODE) { break }
        Start-Sleep -Milliseconds 500
    }
    if ($mysql.HasExited) { throw "MySQL exited. See $lab/mysql.log" }
    "CREATE DATABASE dts_e2e; ALTER USER 'root'@'localhost' IDENTIFIED BY '$secret';" | & "$MySQLBin/mysql.exe" @clientArgs
    if ($LASTEXITCODE) { throw 'MySQL test account setup failed' }
    $env:DTS_E2E_ISOLATED='1'
    Remove-Item -LiteralPath Env:DTS_E2E_CONTINUOUS_SECONDS -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath Env:DTS_E2E_WORKER_TAKEOVER -ErrorAction SilentlyContinue
    $env:DTS_E2E_CONTROL_PROCESSES=if($ControlProcesses){'1'}else{'0'}
    $env:DTS_E2E_POSTGRES_HOST='127.0.0.1'
    $env:DTS_E2E_MYSQL_HOST='127.0.0.1'
    $env:DTS_E2E_POSTGRES_PASSWORD=$secret
    $env:DTS_E2E_MYSQL_PASSWORD=$secret
    $env:DTS_E2E_MYSQL_PORT="$MySQLPort"
    $env:DTS_E2E_POSTGRES_PORT="$PostgresPort"
    $env:DTS_E2E_POSTGRES_DATABASE='postgres'
    $env:DTS_TEST_POSTGRES_HOST='127.0.0.1'
    $env:DTS_TEST_POSTGRES_PORT="$PostgresPort"
    $env:DTS_TEST_POSTGRES_USER='postgres'
    $env:DTS_TEST_POSTGRES_PASSWORD=$secret
    $env:DTS_TEST_POSTGRES_DATABASE='postgres'
    Push-Location (Join-Path $PSScriptRoot '../../backend')
    try {
        $testArgs=if ($DatasourceTLS) { @('./integration','-run','^TestNativeDatasourceTLS$') } else { @('./integration','./internal/repository/postgres') }
        go test -v -tags integration @testArgs -count=1 -timeout 5m 2>&1 | Tee-Object -FilePath (Join-Path $lab 'go-test.log')
        $testExit = $LASTEXITCODE
        if (!$testExit -and $WorkerTakeover) {
            $env:DTS_E2E_WORKER_TAKEOVER='1'
            go test -v -tags integration ./integration -run '^TestWorkerTakeoverDuringFullLoad$' -count=1 -timeout 9m 2>&1 | Tee-Object -FilePath (Join-Path $lab 'worker-takeover.log')
            $testExit=$LASTEXITCODE
        }
        if (!$testExit -and $ContinuousCDCSeconds) {
            $env:DTS_E2E_CONTINUOUS_SECONDS="$ContinuousCDCSeconds"
            $env:DTS_E2E_CONTINUOUS_EVIDENCE=Join-Path $lab 'continuous-cdc.jsonl'
            go test -v -tags integration ./integration -run '^TestContinuousPostgresCDC$' -count=1 -timeout "$($ContinuousCDCSeconds+300)s" 2>&1 | Tee-Object -FilePath (Join-Path $lab 'continuous-test.log')
            $testExit=$LASTEXITCODE
        }
    } finally { Pop-Location }
} finally {
    try {
        if (Test-Path -LiteralPath $pwFile) { Remove-Item -LiteralPath $pwFile }
        if ($null -ne $mysql -and !$mysql.HasExited) {
            $env:MYSQL_PWD=$secret
            & "$MySQLBin/mysqladmin.exe" @clientArgs shutdown
            if (!$mysql.WaitForExit(10000)) { $mysql.Kill(); $mysql.WaitForExit() }
        }
        if ($pgStarted) { & "$PostgresBin/pg_ctl.exe" -D $pgData -m fast -w stop }
    } finally {
        if ($DatasourceTLS) {
            foreach ($keyFile in @('server.key','pg/server.key','ca.key','wrong-ca.key')) {
                Remove-Item -LiteralPath (Join-Path $lab $keyFile) -ErrorAction SilentlyContinue
            }
        }
        foreach ($key in $testEnvNames) {
            if ($null -eq $previousEnv[$key]) {
                Remove-Item -LiteralPath ("Env:"+$key) -ErrorAction SilentlyContinue
            } else {
                [Environment]::SetEnvironmentVariable($key,$previousEnv[$key],'Process')
            }
        }
    }
}
exit $testExit
