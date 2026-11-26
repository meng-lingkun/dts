#!/bin/sh
# Coordinated offline recovery for a dedicated metadata DB and mounted File/RWX spool.
set -eu
umask 077
action=${1:-}
dir=${2:?usage: recovery-set.sh backup|verify|restore RECOVERY_DIRECTORY}
[ "$#" = 2 ] || exit 2
case "$action" in backup|restore|verify) ;; *) exit 2;; esac
case "$dir" in -*) echo "Use an absolute or ./relative recovery path" >&2; exit 2;; esac

verify() {
  [ -f "$dir/READY" ] && [ -f "$dir/SHA256SUMS" ] || { echo "Incomplete recovery set" >&2; exit 1; }
  (cd "$dir" && sha256sum -c SHA256SUMS && pg_restore --list metadata.dump >/dev/null && tar -tzf spool.tar.gz >/dev/null)
}
if [ "$action" = verify ]; then verify; exit; fi
[ "${DTS_WRITERS_STOPPED:-}" = YES ] || {
  echo "Drain tasks, stop all DTS writers and reconcilers for the entire operation, then set DTS_WRITERS_STOPPED=YES" >&2; exit 1;
}
case "${DTS_CDC_SPOOL_STORAGE:-file}" in file|shared-fs) ;; *) echo "Mounted file spool required" >&2; exit 1;; esac
spool=${DTS_CDC_SPOOL_DIR:?set the path of the mounted DTS spool}
[ -d "$spool" ] || { echo "Spool directory does not exist" >&2; exit 1; }
spool=$(CDPATH= cd -- "$spool" && pwd -P)
[ "$spool" != / ] || { echo "Spool must have its own directory" >&2; exit 1; }
export PGHOST=${DTS_METADATA_HOST:-127.0.0.1} PGPORT=${DTS_METADATA_PORT:-5432}
export PGUSER=${DTS_METADATA_USER:-dts} PGDATABASE=${DTS_METADATA_DATABASE:?select dedicated metadata DB}
export PGPASSWORD=${DTS_METADATA_PASSWORD:?required}
: "${DTS_MASTER_KEY:?supply the original encryption key from your secret store}"
fingerprint=$(printf '%s' "$DTS_MASTER_KEY" | sha256sum | cut -d' ' -f1)
for cmd in psql pg_dump pg_restore tar sha256sum; do command -v "$cmd" >/dev/null; done
sql() { psql -XqwAt -v ON_ERROR_STOP=1 -c "$1"; }
spool_inventory() {
  (cd "$spool" && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 -r sha256sum --)
}
idle() {
  clients=$(sql "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND pid<>pg_backend_pid()")
  [ "$clients" = 0 ] || { echo "Dedicated metadata DB has other connected clients; keep writers stopped" >&2; exit 1; }
}
idle
if [ "$action" = backup ]; then
  parent=$(CDPATH= cd -- "$(dirname "$dir")" && pwd -P)
  case "$parent/" in "$spool/"*) echo "Choose a backup destination outside the spool" >&2; exit 1;; esac
  if [ -n "$(find "$spool" ! -type d ! -type f -print -quit)" ]; then
    echo "Spool contains symlinks or special files" >&2; exit 1
  fi
  mkdir -- "$dir"
  version=$(sql "SELECT schema_version FROM metadata_schema_state WHERE id=1")
  [ -n "$version" ] || { echo "Missing DTS metadata version" >&2; exit 1; }
  printf '%s\n' "$version" > "$dir/schema-version"
  printf '%s\n' "$fingerprint" > "$dir/master-key.sha256"
  spool_inventory > "$dir/spool.sha256"
  pg_dump -w --format=custom --no-owner --no-acl --file="$dir/metadata.dump"
  tar -C "$spool" -czf "$dir/spool.tar.gz" .
  tar -C "$spool" -dzf "$dir/spool.tar.gz"
  idle
  pg_restore --list "$dir/metadata.dump" >/dev/null
  spool_inventory | cmp "$dir/spool.sha256" - || { echo "Spool changed during backup" >&2; exit 1; }
  (cd "$dir" && sha256sum metadata.dump spool.tar.gz spool.sha256 schema-version master-key.sha256 > SHA256SUMS)
  printf '%s\n' "file-spool-v1" > "$dir/READY"
  echo "Recovery set created at $dir. Keep matching credentials and deployment configuration separately."
  exit
fi

[ "${DTS_RESTORE_CONFIRM:-}" = YES ] || { echo "Set DTS_RESTORE_CONFIRM=YES on an empty recovery target" >&2; exit 2; }
verify
[ "$(cat "$dir/master-key.sha256")" = "$fingerprint" ] || { echo "Master key differs from backup" >&2; exit 1; }
[ -z "$(find "$spool" -mindepth 1 -print -quit)" ] || { echo "Restore requires an empty dedicated spool" >&2; exit 1; }
objects=$(sql "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp_%'")
[ "$objects" = 0 ] || { echo "Restore requires an empty dedicated database" >&2; exit 1; }
tar -C "$spool" -xzf "$dir/spool.tar.gz" --no-same-owner
spool_inventory | cmp "$dir/spool.sha256" - || { echo "Spool verification failed; keep writers stopped" >&2; exit 1; }
pg_restore -w --exit-on-error --single-transaction --no-owner --no-acl -d "$PGDATABASE" "$dir/metadata.dump" || {
  echo "Database transaction failed. Keep writers stopped and retry with a fresh empty recovery spool." >&2; exit 1;
}
restored=$(sql "SELECT schema_version FROM metadata_schema_state WHERE id=1")
[ "$restored" = "$(cat "$dir/schema-version")" ] || { echo "Restored metadata version mismatch; keep writers stopped" >&2; exit 1; }
echo "Restored schema $restored and matching spool. Review target fencing generations, COMMIT_UNCERTAIN and source log retention before resuming."
