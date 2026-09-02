#!/usr/bin/env sh
set -eu
OUT=${1:-./qmigration-backup-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"
: "${QMIGRATION_METADATA_HOST:=127.0.0.1}"
: "${QMIGRATION_METADATA_PORT:=5432}"
: "${QMIGRATION_METADATA_USER:=qmigration}"
: "${QMIGRATION_METADATA_DATABASE:=qmigration}"
export PGPASSWORD=${QMIGRATION_METADATA_PASSWORD:?QMIGRATION_METADATA_PASSWORD is required}
pg_dump -h "$QMIGRATION_METADATA_HOST" -p "$QMIGRATION_METADATA_PORT" -U "$QMIGRATION_METADATA_USER" -d "$QMIGRATION_METADATA_DATABASE" -Fc -f "$OUT/metadata.dump"
cat > "$OUT/README.txt" <<TXT
QMigration metadata backup created $(date -u +%FT%TZ).
Back up QMIGRATION_MASTER_KEY and QMIGRATION_AUTH_SECRET separately in your secret manager.
They are intentionally NOT written into this directory.
TXT
sha256sum "$OUT/metadata.dump" > "$OUT/SHA256SUMS"
echo "$OUT"
