#!/usr/bin/env sh
set -eu
OUT=${1:-./dts-backup-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"
: "${DTS_METADATA_HOST:=127.0.0.1}"
: "${DTS_METADATA_PORT:=5432}"
: "${DTS_METADATA_USER:=dts}"
: "${DTS_METADATA_DATABASE:=dts}"
export PGPASSWORD=${DTS_METADATA_PASSWORD:?DTS_METADATA_PASSWORD is required}
pg_dump -h "$DTS_METADATA_HOST" -p "$DTS_METADATA_PORT" -U "$DTS_METADATA_USER" -d "$DTS_METADATA_DATABASE" -Fc -f "$OUT/metadata.dump"
cat > "$OUT/README.txt" <<TXT
DTS metadata backup created $(date -u +%FT%TZ).
Back up DTS_MASTER_KEY and DTS_AUTH_SECRET separately in your secret manager.
They are intentionally NOT written into this directory.
TXT
sha256sum "$OUT/metadata.dump" > "$OUT/SHA256SUMS"
echo "$OUT"
