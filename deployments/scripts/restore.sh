#!/usr/bin/env sh
set -eu
DIR=${1:?usage: restore.sh BACKUP_DIR}
[ "${DTS_RESTORE_CONFIRM:-}" = "YES" ] || { echo "set DTS_RESTORE_CONFIRM=YES to restore" >&2; exit 2; }
: "${DTS_METADATA_HOST:=127.0.0.1}"
: "${DTS_METADATA_PORT:=5432}"
: "${DTS_METADATA_USER:=dts}"
: "${DTS_METADATA_DATABASE:=dts}"
export PGPASSWORD=${DTS_METADATA_PASSWORD:?DTS_METADATA_PASSWORD is required}
(cd "$DIR" && sha256sum -c SHA256SUMS)
pg_restore -h "$DTS_METADATA_HOST" -p "$DTS_METADATA_PORT" -U "$DTS_METADATA_USER" -d "$DTS_METADATA_DATABASE" --clean --if-exists --no-owner "$DIR/metadata.dump"
echo "restore complete; restart DTS with the SAME DTS_MASTER_KEY used when credentials were encrypted"
