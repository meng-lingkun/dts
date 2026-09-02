#!/usr/bin/env sh
set -eu
DIR=${1:?usage: restore.sh BACKUP_DIR}
[ "${QMIGRATION_RESTORE_CONFIRM:-}" = "YES" ] || { echo "set QMIGRATION_RESTORE_CONFIRM=YES to restore" >&2; exit 2; }
: "${QMIGRATION_METADATA_HOST:=127.0.0.1}"
: "${QMIGRATION_METADATA_PORT:=5432}"
: "${QMIGRATION_METADATA_USER:=qmigration}"
: "${QMIGRATION_METADATA_DATABASE:=qmigration}"
export PGPASSWORD=${QMIGRATION_METADATA_PASSWORD:?QMIGRATION_METADATA_PASSWORD is required}
(cd "$DIR" && sha256sum -c SHA256SUMS)
pg_restore -h "$QMIGRATION_METADATA_HOST" -p "$QMIGRATION_METADATA_PORT" -U "$QMIGRATION_METADATA_USER" -d "$QMIGRATION_METADATA_DATABASE" --clean --if-exists --no-owner "$DIR/metadata.dump"
echo "restore complete; restart QMigration with the SAME QMIGRATION_MASTER_KEY used when credentials were encrypted"
