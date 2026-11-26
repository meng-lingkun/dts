#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
cd images
test -s all-images.tar.gz || { echo 'ERROR: images/all-images.tar.gz is missing or empty' >&2; exit 1; }
archive_tmp=$(mktemp ./all-images.tar.XXXXXX)
trap 'rm -f -- "$archive_tmp" "$archive_tmp.list"' EXIT
trap 'exit 1' INT TERM
gzip -dc all-images.tar.gz > "$archive_tmp"
tar -tf "$archive_tmp" > "$archive_tmp.list"
grep -Fxq manifest.json "$archive_tmp.list" || {
  echo 'ERROR: unified image archive has no Docker manifest.json' >&2
  exit 1
}
mv -f "$archive_tmp" all-images.tar
echo 'Unified image archive ready: images/all-images.tar'
