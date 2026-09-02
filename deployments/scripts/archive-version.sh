#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  archive-version.sh \
    --source /path/to/current-source \
    --previous /path/to/previous-source \
    --baseline /path/to/formal-v0.13-source \
    --out /path/to/output [--preverified-go|--no-go-verify]

Produces and verifies:
  qmigration-<version>.zip
  qmigration-<previous-version>-to-<version>.patch
  qmigration-<version>.patch              # formal baseline -> current
  qmigration-<version>.sha256
  qmigration-<version>.manifest.json

The source archive excludes build/runtime artifacts (.git, bin, data,
node_modules, dist). Patches are generated from a temporary Git index so new,
deleted and binary files are always included. Both patches are applied to clean
copies and compared byte-for-byte with the normalized source tree. Go verification is
run on restored trees by default, or recorded as preverified/not-run explicitly.
USAGE
}

SOURCE=""
PREVIOUS=""
BASELINE=""
OUT=""
VERIFY_GO=1
PREVERIFIED_GO=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) SOURCE=$2; shift 2 ;;
    --previous) PREVIOUS=$2; shift 2 ;;
    --baseline) BASELINE=$2; shift 2 ;;
    --out) OUT=$2; shift 2 ;;
    --no-go-verify) VERIFY_GO=0; PREVERIFIED_GO=0; shift ;;
    --preverified-go) VERIFY_GO=0; PREVERIFIED_GO=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

for v in SOURCE PREVIOUS BASELINE OUT; do
  [[ -n "${!v}" ]] || { echo "missing --${v,,}" >&2; usage >&2; exit 2; }
done
SOURCE=$(cd "$SOURCE" && pwd)
PREVIOUS=$(cd "$PREVIOUS" && pwd)
BASELINE=$(cd "$BASELINE" && pwd)
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)

VERSION=$(tr -d '[:space:]' < "$SOURCE/VERSION")
PREVIOUS_VERSION=$(tr -d '[:space:]' < "$PREVIOUS/VERSION")
[[ -n "$VERSION" && -n "$PREVIOUS_VERSION" ]] || { echo "VERSION file is empty" >&2; exit 1; }

TAG="v${VERSION}"
PREVIOUS_TAG="v${PREVIOUS_VERSION}"
ARCHIVE="qmigration-${TAG}.zip"
INCREMENTAL="qmigration-${PREVIOUS_TAG}-to-${TAG}.patch"
CUMULATIVE="qmigration-${TAG}.patch"
SHA="qmigration-${TAG}.sha256"
MANIFEST="qmigration-${TAG}.manifest.json"

TMP=$(mktemp -d "${TMPDIR:-/tmp}/qmigration-archive.XXXXXX")
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

RSYNC_EXCLUDES=(
  --exclude=.git/
  --exclude=bin/
  --exclude=data/
  --exclude=node_modules/
  --exclude=dist/
  --exclude=.cache/
  --exclude='*.log'
)

normalize_tree() {
  local src=$1 dst=$2
  mkdir -p "$dst"
  rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$src/" "$dst/"
}

make_patch() {
  local from=$1 patch=$2
  local repo="$TMP/patch-repo-$(basename "$patch" .patch)"
  mkdir -p "$repo"
  normalize_tree "$from" "$repo"
  (
    cd "$repo"
    git init -q
    git config user.name QMigration-Archive
    git config user.email archive@qmigration.local
    git add -A
    git commit -qm baseline
    rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$SOURCE/" "$repo/"
    git add -A
    git diff --cached --check
    git diff --cached --binary --full-index > "$patch"
  )
}

verify_patch() {
  local from=$1 patch=$2 label=$3
  local verify="$TMP/verify-$label"
  local expected="$TMP/expected-$label"
  mkdir -p "$verify" "$expected"
  normalize_tree "$from" "$verify"
  normalize_tree "$SOURCE" "$expected"
  (
    cd "$verify"
    git init -q
    git apply --check "$patch"
    git apply "$patch"
  )
  if ! diff -qr --exclude=.git "$expected" "$verify" > "$TMP/diff-$label.txt"; then
    echo "archive verification failed: restored tree differs for $label" >&2
    cat "$TMP/diff-$label.txt" >&2
    exit 1
  fi
  if [[ "$VERIFY_GO" == 1 && -f "$verify/backend/go.mod" ]]; then
    (cd "$verify/backend" && go test ./... && go vet ./...)
  fi
}

# Normalize into a versioned top-level directory for a predictable ZIP layout.
STAGE="$TMP/qmigration-${TAG}"
normalize_tree "$SOURCE" "$STAGE"
(
  cd "$TMP"
  zip -qr "$OUT/$ARCHIVE" "qmigration-${TAG}"
)

make_patch "$PREVIOUS" "$OUT/$INCREMENTAL"
make_patch "$BASELINE" "$OUT/$CUMULATIVE"
verify_patch "$PREVIOUS" "$OUT/$INCREMENTAL" incremental
verify_patch "$BASELINE" "$OUT/$CUMULATIVE" cumulative

(
  cd "$OUT"
  sha256sum "$ARCHIVE" "$INCREMENTAL" "$CUMULATIVE" > "$SHA"
)

python3 - "$OUT" "$VERSION" "$PREVIOUS_VERSION" "$ARCHIVE" "$INCREMENTAL" "$CUMULATIVE" "$SHA" "$MANIFEST" "$VERIFY_GO" "$PREVERIFIED_GO" <<'PY'
import hashlib, json, os, sys, datetime
out, version, previous, archive, incremental, cumulative, sha, manifest, verify_go, preverified_go = sys.argv[1:]

def digest(name):
    p=os.path.join(out,name)
    h=hashlib.sha256()
    with open(p,'rb') as f:
        for b in iter(lambda:f.read(1024*1024), b''):
            h.update(b)
    return {"file":name,"bytes":os.path.getsize(p),"sha256":h.hexdigest()}
if verify_go == "1":
    go_ok, go_mode = True, "archive-clean-restores"
elif preverified_go == "1":
    go_ok, go_mode = True, "preverified-source-byte-equal-restores"
else:
    go_ok, go_mode = False, "not-run"
obj={
    "product":"QMigration",
    "version":version,
    "previous_version":previous,
    "created_at_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "archive_policy":"source archive + incremental patch + formal-v0.13 cumulative patch + clean restore verification",
    "verification":{"incremental_restore":True,"cumulative_restore":True,"source_tree_equal":True,"go_test":go_ok,"go_vet":go_ok,"go_verification_mode":go_mode},
    "artifacts":[digest(archive),digest(incremental),digest(cumulative)],
}
with open(os.path.join(out,manifest),'w',encoding='utf-8') as f:
    json.dump(obj,f,ensure_ascii=False,indent=2)
    f.write('\n')
PY
(
  cd "$OUT"
  sha256sum "$MANIFEST" >> "$SHA"
)

echo "ARCHIVE_VERSION=$VERSION"
echo "ARCHIVE_ZIP=$OUT/$ARCHIVE"
echo "ARCHIVE_INCREMENTAL_PATCH=$OUT/$INCREMENTAL"
echo "ARCHIVE_CUMULATIVE_PATCH=$OUT/$CUMULATIVE"
echo "ARCHIVE_SHA256=$OUT/$SHA"
echo "ARCHIVE_MANIFEST=$OUT/$MANIFEST"
