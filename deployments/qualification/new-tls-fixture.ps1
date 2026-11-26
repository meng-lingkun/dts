# Create two ephemeral CAs and a server certificate for the isolated TLS runner.
param([Parameter(Mandatory)][string]$Directory)
$ErrorActionPreference='Stop'
function Invoke-OpenSSL {
    & openssl.exe @args
    if ($LASTEXITCODE) { throw "openssl $($args[0]) failed" }
}
Push-Location $Directory
try {
    @'
[req]
distinguished_name=dn
[dn]
[root]
basicConstraints=critical,CA:TRUE
keyUsage=critical,keyCertSign,cRLSign
[server]
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:db.dts-qa.invalid
'@ | Set-Content -Encoding ascii tls.cnf
    foreach ($ca in 'ca','wrong-ca') {
        Invoke-OpenSSL genrsa -out "$ca.key" 2048
        Invoke-OpenSSL req -config tls.cnf -x509 -new -sha256 -days 2 -key "$ca.key" -subj "/CN=DTS temporary $ca" -extensions root -out "$ca.crt"
    }
    Invoke-OpenSSL genrsa -out server.key 2048
    Invoke-OpenSSL req -config tls.cnf -new -key server.key -subj '/CN=db.dts-qa.invalid' -out server.csr
    Invoke-OpenSSL x509 -req -in server.csr -CA ca.crt -CAkey ca.key -set_serial 101 -sha256 -days 2 -extfile tls.cnf -extensions server -out server.crt
} finally {
    Remove-Item -LiteralPath (Join-Path $Directory 'ca.key'),(Join-Path $Directory 'wrong-ca.key') -ErrorAction SilentlyContinue
    Pop-Location
}
