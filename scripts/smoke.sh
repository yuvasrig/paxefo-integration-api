#!/usr/bin/env bash
# Posts the 13 provided Tive samples (in order) to a running API and checks each response against
# specs/001-integration-api/expected/. Usage: scripts/smoke.sh <base-url> <api-key>
#
# Expected results describe the first run on a tenant that has not seen the samples. On a re-run the same
# payloads are duplicates, so a sample that was accepted/flagged is also allowed to answer "duplicate".
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: $0 <base-url> <api-key>" >&2
  exit 2
fi
base="${1%/}"
key="$2"
dir="$(cd "$(dirname "$0")/.." && pwd)/testdata/samples"
command -v curl >/dev/null || { echo "curl is required" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 2; }

# index:http:status on the first run
expected=(
  "0:200:flagged" "1:200:conflict" "2:200:accepted" "3:200:flagged" "4:200:conflict"
  "5:200:flagged" "6:200:flagged" "7:200:flagged" "8:200:conflict" "9:200:accepted"
  "10:200:accepted" "11:200:accepted" "12:422:rejected"
)

# Wake a sleeping free-tier instance.
printf 'waiting for %s/healthz ' "$base"
for _ in $(seq 1 60); do
  if curl -fsS -o /dev/null --max-time 10 "$base/healthz" 2>/dev/null; then echo "ok"; break; fi
  printf '.'; sleep 2
done

pass=0; fail=0
tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT
for entry in "${expected[@]}"; do
  IFS=: read -r i want_code want_status <<<"$entry"
  file=$(printf '%s/%02d.json' "$dir" "$i")
  code=$(curl -sS -o "$tmp" -w '%{http_code}' --max-time 30 -X POST "$base/api/webhook/tive" \
    -H "X-API-Key: $key" -H "Content-Type: application/json" -H "X-Request-ID: smoke-$i" --data-binary "@$file")
  status=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("status",""))' "$tmp" 2>/dev/null || echo "?")

  ok=0
  if [ "$code" = "$want_code" ] && [ "$status" = "$want_status" ]; then ok=1; fi
  if [ "$code" = "200" ] && [ "$status" = "duplicate" ] && { [ "$want_status" = "accepted" ] || [ "$want_status" = "flagged" ]; }; then ok=1; fi

  if [ "$ok" = 1 ]; then
    pass=$((pass+1)); printf 'PASS  sample %2d  HTTP %s  %s\n' "$i" "$code" "$status"
  else
    fail=$((fail+1)); printf 'FAIL  sample %2d  HTTP %s %s (want %s %s)\n' "$i" "$code" "$status" "$want_code" "$want_status"
    head -c 400 "$tmp"; echo
  fi
done

echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
