#!/bin/sh
# Creates two uniquely named databases on the explicitly disposable PostgreSQL service.
set -eu
[ "${DTS_E2E_ISOLATED:-}" = 1 ] || { echo "Set DTS_E2E_ISOLATED=1 for the disposable PostgreSQL service" >&2; exit 2; }
: "${PGHOST:?}" "${PGPORT:?}" "${PGUSER:?}" "${PGPASSWORD:?}"
repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d)
suffix=$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')
db1=dts_recovery_src_$suffix
db2=dts_recovery_dst_$suffix
created1=false
created2=false
cleanup() {
  if "$created2"; then dropdb --if-exists "$db2"; fi
  if "$created1"; then dropdb --if-exists "$db1"; fi
  rm -rf -- "$work"
}
trap cleanup EXIT
createdb "$db1"
created1=true
createdb "$db2"
created2=true
psql -Xq -v ON_ERROR_STOP=1 -d "$db1" -f "$repo/backend/internal/repository/postgres/schema.sql" >/dev/null
psql -Xq -v ON_ERROR_STOP=1 -d "$db1" -c "CREATE TABLE recovery_fixture(id int PRIMARY KEY, value text); INSERT INTO recovery_fixture VALUES(1, 'fixture')"
mkdir "$work/source" "$work/target"
printf 'matching spool bytes\n' > "$work/source/txn"
export DTS_METADATA_HOST="$PGHOST" DTS_METADATA_PORT="$PGPORT" DTS_METADATA_USER="$PGUSER" DTS_METADATA_PASSWORD="$PGPASSWORD"
export DTS_WRITERS_STOPPED=YES DTS_MASTER_KEY=isolated-recovery-fixture-key
export DTS_METADATA_DATABASE="$db1" DTS_CDC_SPOOL_DIR="$work/source"
sh "$repo/deployments/scripts/recovery-set.sh" backup "$work/set"
export DTS_METADATA_DATABASE="$db2" DTS_CDC_SPOOL_DIR="$work/target" DTS_RESTORE_CONFIRM=YES
sh "$repo/deployments/scripts/recovery-set.sh" restore "$work/set"
cmp "$work/source/txn" "$work/target/txn"
[ "$(psql -XAt -d "$db2" -c 'SELECT value FROM recovery_fixture WHERE id=1')" = fixture ]
echo "PostgreSQL + file spool restore passed"
