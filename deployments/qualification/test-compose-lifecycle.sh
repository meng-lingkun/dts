#!/bin/sh
# Exercise the package entry points with a stateful Docker CLI fixture.
set -eu
repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
mkdir "$work/bin" "$work/package" "$work/state" "$work/package/images"
cp "$repo"/deployments/compose-offline/*.sh "$repo/deployments/compose-offline/compose.yaml" "$repo/VERSION" "$work/package"
export MOCK_STATE="$work/state" PATH="$work/bin:$PATH"
cat > "$work/bin/uname" <<'EOF'
#!/bin/sh
echo 'Linux x86_64'
EOF
cat > "$work/bin/flock" <<'EOF'
#!/bin/sh
exit 0
EOF
cat > "$work/bin/docker" <<'EOF'
#!/bin/sh
case "$*" in
  info|load*) exit 0;;
  'version --format '*) echo 29.7.2;;
  'compose version'*) echo 2.40.2;;
  'volume ls -q') if [ -e "$MOCK_STATE/volumes" ]; then printf 'dts-offline_pgdata\ndts-offline_spool\n'; fi;;
  'volume inspect --format '*) echo "${MOCK_OWNER:-dts-offline}";;
  'volume inspect '*) test -f "$MOCK_STATE/volumes";;
  'compose -p dts-offline '*)
    case "$*" in
      *' config --quiet') test -s .env;;
      *' up -d '*) touch "$MOCK_STATE/volumes";;
      *' ps --status running -q '*) echo fake-container;;
      *' exec -T server wget '*) printf '{"status":"ready","version":"%s","schema_version":"%s"}\n' "$DTS_VERSION" "${MOCK_SCHEMA:-$DTS_VERSION}";;
      *' exec -T web wget -qO - http://127.0.0.1:8080/readyz')
        if [ -n "${MOCK_WEB_FALLBACK:-}" ]; then echo '<html>DTS</html>'; else
          printf '{"status":"ready","version":"%s","schema_version":"%s"}\n' "$DTS_VERSION" "$DTS_VERSION"
        fi;;
      *' exec -T postgres psql '*) echo t;;
      *' exec -T web wget -S '*) if [ -z "${MOCK_PUBLIC_WORKERS:-}" ]; then echo 'HTTP/1.1 401 Unauthorized' >&2; exit 1; fi;;
      *' exec -T web wget '*) exit 0;;
      *' down --volumes') rm -f -- "$MOCK_STATE/volumes";;
      *' down') exit 0;;
      *) echo "unrecognized Compose invocation" >&2; exit 2;;
    esac;;
  *) echo "unrecognized Docker invocation" >&2; exit 2;;
esac
EOF
chmod +x "$work/bin/"*
cd "$work/package"
sha256sum ./*.sh compose.yaml VERSION > SHA256SUMS
sh install.sh
saved=$(sha256sum .env)
sh install.sh
[ "$(sha256sum .env)" = "$saved" ]
if MOCK_SCHEMA=old sh verify.sh; then exit 1; fi
if MOCK_WEB_FALLBACK=1 sh verify.sh; then exit 1; fi
if MOCK_PUBLIC_WORKERS=1 sh verify.sh; then exit 1; fi
sh uninstall.sh
[ -e "$MOCK_STATE/volumes" ] && [ "$(sha256sum .env)" = "$saved" ]
: > .env
if sh install.sh; then echo "recreated credentials over retained volumes" >&2; exit 1; fi
if MOCK_OWNER=other sh uninstall.sh --purge-data; then exit 1; fi
[ -e "$MOCK_STATE/volumes" ]
sh uninstall.sh --purge-data
[ ! -e "$MOCK_STATE/volumes" ] && [ ! -e .env ]
sh install.sh
[ "$(sha256sum .env)" != "$saved" ]
echo "Compose lifecycle checks passed"
