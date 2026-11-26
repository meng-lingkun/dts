#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SECONDS_REQUIRED="${DTS_SOAK_SECONDS:-86400}"
if ! [[ "$SECONDS_REQUIRED" =~ ^[0-9]+$ ]] || (( SECONDS_REQUIRED < 3600 )); then
  echo 'DTS_SOAK_SECONDS must be at least 3600; default is 86400 (24 hours)' >&2
  exit 2
fi
OUT="${DTS_SOAK_EVIDENCE_DIR:-$ROOT/artifacts/native-soak-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
start=$(date +%s)
deadline=$((start+SECONDS_REQUIRED))
cycle=0
printf 'started_utc=%s\nrequired_seconds=%s\n' "$(date -u +%FT%TZ)" "$SECONDS_REQUIRED" > "$OUT/run.txt"
git -C "$ROOT" rev-parse HEAD >> "$OUT/run.txt"
git -C "$ROOT" diff HEAD --binary > "$OUT/worktree.patch"
git -C "$ROOT" status --porcelain > "$OUT/worktree-status.txt"
(cd "$ROOT" && git ls-files -z --cached --others --exclude-standard | tar --null -T - -czf "$OUT/source.tar.gz")
while (( $(date +%s) < deadline )); do
  cycle=$((cycle+1))
  # Real database migrations and two-connection fences, not a virtual load model.
  if ! bash "$ROOT/deployments/qualification/run-native.sh" > "$OUT/cycle-$cycle.log" 2>&1; then
    printf '{"suite_passed":false,"production_qualified":false,"cycle":%s,"elapsed_seconds":%s}\n' "$cycle" "$(( $(date +%s)-start ))" > "$OUT/result.json"
    cat "$OUT/cycle-$cycle.log"
    exit 1
  fi
  printf 'cycle=%s finished_utc=%s\n' "$cycle" "$(date -u +%FT%TZ)" >> "$OUT/run.txt"
  sleep 5
done
printf '{"suite_passed":true,"production_qualified":false,"kind":"repeated-native-migration-soak","cycles":%s,"elapsed_seconds":%s,"required_seconds":%s}\n' "$cycle" "$(( $(date +%s)-start ))" "$SECONDS_REQUIRED" > "$OUT/result.json"
cat "$OUT/result.json"
