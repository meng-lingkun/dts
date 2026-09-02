#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DB2_HOME=${DB2_HOME:-${IBM_DB_HOME:-}}
[[ -n "$DB2_HOME" ]] || { echo "DB2_HOME (IBM Data Server Client/Runtime root) is required" >&2; exit 2; }
CC=${CC:-cc}
OUT=${1:-$ROOT/bin/qmigration-db2-readlog-provider}
mkdir -p "$(dirname "$OUT")"
inc="$DB2_HOME/include"
lib="$DB2_HOME/lib64"
[[ -d "$lib" ]] || lib="$DB2_HOME/lib"
"$CC" -std=c11 -O2 -Wall -Wextra -Werror -I"$inc" \
  "$ROOT/backend/native/db2readlog/qmigration_db2readlog.c" \
  -L"$lib" -Wl,-rpath,"$lib" -ldb2 -o "$OUT"
echo "built $OUT"
