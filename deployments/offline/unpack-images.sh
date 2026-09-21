#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
cd images
test -s all-images.tar.gz || { echo 'ERROR: images/all-images.tar.gz is missing or empty' >&2; exit 1; }
trap 'rm -f -- all-images.tar.tmp' EXIT INT TERM
gzip -t all-images.tar.gz
gzip -dc all-images.tar.gz > all-images.tar.tmp
tar -tf all-images.tar.tmp | awk '$0 == "manifest.json" { found=1 } END { exit !found }' || {
  echo 'ERROR: unified image archive has no Docker manifest.json' >&2
  exit 1
}
mv -f all-images.tar.tmp all-images.tar
echo 'Unified image archive ready: images/all-images.tar'
