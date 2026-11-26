#!/bin/sh
# Exercise failure handling with disposable directories and simulated PostgreSQL CLI responses.
set -eu
script=$(CDPATH= cd -- "$(dirname "$0")/../scripts" && pwd)/recovery-set.sh
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
mkdir "$work/bin" "$work/source" "$work/target"
cat > "$work/bin/psql" <<'EOF'
#!/bin/sh
case "$*" in
  *pg_stat_activity*) echo "${CLIENTS:-0}";;
  *pg_class*) echo "${OBJECTS:-0}";;
  *schema_version*) echo "0.15.0-rc50";;
  *) exit 2;;
esac
EOF
cat > "$work/bin/pg_dump" <<'EOF'
#!/bin/sh
for arg do case "$arg" in --file=*) printf 'archive\n' > "${arg#--file=}";; esac; done
EOF
cat > "$work/bin/pg_restore" <<'EOF'
#!/bin/sh
case "$*" in *--list*) exit 0;; *--single-transaction*) exit "${RESTORE_FAIL:-0}";; *) exit 2;; esac
EOF
chmod 755 "$work/bin/"*
export PATH="$work/bin:$PATH" DTS_METADATA_DATABASE=disposable DTS_METADATA_PASSWORD=fixture
export DTS_CDC_SPOOL_DIR="$work/source" DTS_MASTER_KEY=fixture-key DTS_RESTORE_CONFIRM=YES
printf 'encrypted spool bytes\n' > "$work/source/spool entry"
expect_failure() { if "$@"; then echo "Unexpected success" >&2; exit 1; fi; }
expect_failure sh "$script" backup "$work/rejected"
export DTS_WRITERS_STOPPED=YES
expect_failure env CLIENTS=1 sh "$script" backup "$work/rejected"
expect_failure sh "$script" backup "$work/source/nested"
sh "$script" backup "$work/backup"
sh "$script" verify "$work/backup"
export DTS_CDC_SPOOL_DIR="$work/target"
expect_failure env OBJECTS=1 sh "$script" restore "$work/backup"
expect_failure env DTS_MASTER_KEY=wrong sh "$script" restore "$work/backup"
sh "$script" restore "$work/backup"
cmp "$work/source/spool entry" "$work/target/spool entry"
expect_failure sh "$script" restore "$work/backup"
printf 'damaged' >> "$work/backup/spool.tar.gz"
expect_failure sh "$script" verify "$work/backup"
echo "Recovery set lifecycle passed"
