#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
sha256sum -c SHA256SUMS
docker compose --env-file .env -f docker-compose.offline.yml ps
docker compose --env-file .env -f docker-compose.offline.yml exec -T server wget -q -O - http://127.0.0.1:8080/readyz
