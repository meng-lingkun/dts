#!/bin/sh
set -eu
cd "$(dirname "$0")"
DTS_VERSION=$(cat VERSION); export DTS_VERSION
export DOCKER_HOST=unix:///var/run/docker.sock
unset DOCKER_CONTEXT
compose() { docker compose -p dts-offline --env-file .env -f compose.yaml "$@"; }
for service in postgres server worker web; do
  id=$(compose ps --status running -q "$service")
  [ -n "$id" ] || { echo "Service $service is not running" >&2; exit 1; }
done
for service in server web; do
  ready=$(compose exec -T "$service" wget -qO - http://127.0.0.1:8080/readyz) || {
    echo "$service readiness request failed" >&2; exit 1;
  }
  for field in '"status":"ready"' "\"schema_version\":\"$DTS_VERSION\"" "\"version\":\"$DTS_VERSION\""; do
    printf '%s\n' "$ready" | grep -F "$field" >/dev/null || { echo "Unexpected $service readiness: $ready" >&2; exit 1; }
  done
done
worker_sql="SELECT EXISTS(SELECT 1 FROM workers WHERE last_heartbeat >= now() - interval '45 seconds')"
attempt=0
until [ "$(compose exec -T postgres psql -XAtw -U dts -d dts -c "$worker_sql")" = t ]; do
  attempt=$((attempt+1)); [ "$attempt" -lt 15 ] || { echo "No recent worker heartbeat" >&2; exit 1; }
  sleep 2
done
compose exec -T web wget -qO /dev/null http://127.0.0.1:8080/
response=$(compose exec -T web wget -S -O /dev/null http://127.0.0.1:8080/api/v1/workers 2>&1) && {
  echo "Workers API unexpectedly accessible without credentials" >&2; exit 1;
}
printf '%s\n' "$response" | grep -E 'HTTP/[0-9.]+ 401( |$)' >/dev/null || { echo "Web API authentication check failed" >&2; exit 1; }
echo "DTS ready. Web http://127.0.0.1:8088, API http://127.0.0.1:8080"
