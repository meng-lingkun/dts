#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"

if [ "$(uname -s)" != "Linux" ]; then
  echo "ERROR: this image loader targets Linux" >&2
  exit 1
fi
if [ "$(id -u)" -ne 0 ]; then
  if command -v sudo >/dev/null 2>&1; then
    exec sudo sh "$0" "$@"
  fi
  echo "ERROR: run this script as root on a Kubernetes node" >&2
  exit 1
fi

sha256sum -c SHA256SUMS >/dev/null

import_archive() {
  archive=$1
  if command -v k3s >/dev/null 2>&1; then
    k3s ctr images import "$archive"
  elif command -v microk8s >/dev/null 2>&1; then
    microk8s ctr image import "$archive"
  elif command -v ctr >/dev/null 2>&1; then
    ctr --namespace k8s.io images import "$archive"
  elif command -v nerdctl >/dev/null 2>&1; then
    nerdctl --namespace k8s.io load --input "$archive"
  elif command -v docker >/dev/null 2>&1; then
    docker load --input "$archive"
  else
    echo "ERROR: no supported Kubernetes image importer found (k3s, microk8s, ctr, nerdctl or Docker)" >&2
    exit 1
  fi
}

for archive in images/*.tar; do
  echo "Importing $archive"
  import_archive "$archive"
done

echo "Images imported on node $(hostname). Repeat this command on every schedulable node."
