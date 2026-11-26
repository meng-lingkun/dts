#!/bin/sh
# Exercise real Compose, PostgreSQL, Server, Worker and Web on a fresh Linux CI runner.
set -eu
[ "${DTS_E2E_ISOLATED:-}" = 1 ] || { echo "Set DTS_E2E_ISOLATED=1 on a disposable Docker host" >&2; exit 2; }
repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
export DOCKER_HOST=unix:///var/run/docker.sock
unset DOCKER_CONTEXT
version=$(cat "$repo/VERSION")
export DTS_VERSION="$version"
archive=${DTS_E2E_OFFLINE_ARCHIVE:-}
if command -v docker >/dev/null 2>&1; then
  docker info >/dev/null
  test -z "$(docker ps -aq --filter label=com.docker.compose.project=dts-offline)"
  test -z "$(docker volume ls -q --filter name=dts-offline_)"
  for image in "dts/server:$version" "dts/web:$version" "dts/postgres:17"; do
    if docker image inspect "$image" >/dev/null 2>&1; then echo "Fresh Docker host required" >&2; exit 1; fi
  done
elif [ -z "$archive" ]; then
  echo "Set DTS_E2E_OFFLINE_ARCHIVE on a disposable host without Docker" >&2
  exit 2
fi
work=$(mktemp -d)
installed=false
replacement=
cleanup() {
  if [ -n "$replacement" ]; then docker rm -f "$replacement" >/dev/null || true; fi
  if "$installed"; then (cd "$work" && sh uninstall.sh --purge-data) || true; fi
  rm -rf -- "$work"
}
trap cleanup EXIT
if [ -n "$archive" ]; then
  name=$(basename "$archive")
  [ "$name" = "dts-compose-offline-$version-linux-amd64.tar.gz" ]
  (cd "$(dirname "$archive")" && sha256sum -c "$name.sha256")
  tar -xzf "$archive" -C "$work" --strip-components=1
else
  docker build -f "$repo/deployments/Dockerfile.backend" -t "dts/server:$version" "$repo"
  docker build -f "$repo/deployments/Dockerfile.web" -t "dts/web:$version" "$repo"
  docker pull postgres:17
  docker tag postgres:17 dts/postgres:17
  mkdir "$work/images"
  docker save -o "$work/images/application.tar" "dts/server:$version" "dts/web:$version" dts/postgres:17
  gzip "$work/images/application.tar"
  cp "$repo"/deployments/compose-offline/*.sh "$repo/deployments/compose-offline/compose.yaml" "$repo/deployments/compose-offline/nginx.conf" "$repo/VERSION" "$work"
  (cd "$work" && sha256sum ./*.sh compose.yaml nginx.conf VERSION images/application.tar.gz > SHA256SUMS)
fi
cd "$work"
sh install.sh
installed=true
original=$(sha256sum .env)
compose() { docker compose -p dts-offline --env-file .env -f compose.yaml "$@"; }
probe=$(docker inspect --format '{{index .Config.Healthcheck.Test 1}}' "$(compose ps -q postgres)")
compose exec -T postgres sh -ec "$probe" >/dev/null
if compose exec -T -e POSTGRES_PASSWORD=incorrect-for-qualification postgres sh -ec "$probe" >/dev/null 2>&1; then
  echo "PostgreSQL readiness accepted the wrong password" >&2
  exit 1
fi
echo "PostgreSQL probe accepted the installed password and rejected an incorrect password"
sql() {
  compose exec -T postgres sh -ec 'PGREQUIREAUTH=scram-sha-256,md5 PGPASSWORD="$POSTGRES_PASSWORD" exec psql -X -w -h postgres -U dts -d dts -v ON_ERROR_STOP=1 -Atqc "$1"' sh "$1"
}
sentinel=$(cat /proc/sys/kernel/random/uuid)
sql "CREATE TABLE public.dts_smoke_retention(id uuid PRIMARY KEY); INSERT INTO public.dts_smoke_retention VALUES ('$sentinel')"
curl --fail --silent http://127.0.0.1:8080/readyz >/dev/null
curl --fail --silent http://127.0.0.1:8088/ >/dev/null
# Keep Web running while the API moves to a different address. A second
# backend joins with the service aliases and no published host port.
server=$(compose ps -q server)
web=$(compose ps -q web)
web_process() { docker inspect --format '{{.State.Pid}}:{{.State.StartedAt}}:{{.RestartCount}}' "$web"; }
original_web_process=$(web_process)
proxied_ready() {
  curl -fsS --max-time 3 http://127.0.0.1:8088/readyz 2>/dev/null | grep -F "\"schema_version\":\"$version\"" >/dev/null
}
proxied_ready
replacement=$(compose run -d --no-deps --use-aliases server)
ip_of() { docker inspect --format '{{(index .NetworkSettings.Networks "dts-offline_default").IPAddress}}' "$1"; }
original_ip=$(ip_of "$server")
replacement_ip=$(ip_of "$replacement")
[ -n "$original_ip" ] && [ -n "$replacement_ip" ] && [ "$original_ip" != "$replacement_ip" ] || exit 1
docker stop "$server" >/dev/null
[ "$(docker inspect --format '{{.State.Running}}' "$server")" = false ]
attempt=0
until proxied_ready; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 40 ] || { echo "Web proxy failed to resolve the replacement Server" >&2; exit 1; }
  sleep 1
done
[ "$(compose ps -q web)" = "$web" ]
[ "$(web_process)" = "$original_web_process" ]
echo "Web process $original_web_process served /readyz after the Server moved from $original_ip to $replacement_ip"
docker start "$server" >/dev/null
docker rm -f "$replacement" >/dev/null
replacement=
sh uninstall.sh
sh install.sh
[ "$(sha256sum .env)" = "$original" ]
[ "$(sql 'SELECT id FROM public.dts_smoke_retention')" = "$sentinel" ]
echo "Retained installation preserved its original credentials and database row"
sh uninstall.sh --purge-data
[ ! -e .env ]
sh install.sh
[ "$(sha256sum .env)" != "$original" ]
[ "$(sql "SELECT to_regclass('public.dts_smoke_retention') IS NULL")" = t ]
echo "Purged installation generated new credentials and created a clean database"
echo "Real single-node Compose deployment, retained-data restart and clean reinstall passed"
