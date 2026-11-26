#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export DTS_E2E_ISOLATED=1
export DTS_TEST_POSTGRES_HOST=127.0.0.1
export DTS_TEST_POSTGRES_PORT=15432
export DTS_TEST_POSTGRES_USER=postgres
export DTS_TEST_POSTGRES_PASSWORD=dts
export DTS_TEST_POSTGRES_DATABASE=dts
cd "$ROOT/backend"
go test -tags integration ./integration ./internal/repository/postgres -count=1 -timeout 5m -v
