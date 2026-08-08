#!/usr/bin/env bash
# Measure what ONE idle Playpen sandbox actually costs.
# Spawns a realistic sandbox (nodejs + postgres), waits for ACTIVE, reads
# currentHardwareResource off every container, prices it, then deletes.
#
#   ZEROPS_TOKEN="$(cat ~/.zerops-token)" ./cost.sh
set -uo pipefail

API="https://api.app-prg1.zerops.io/api/rest/public"
REGION="${REGION:-eu-central}"
BOLD=$'\033[1m'; GRN=$'\033[32m'; DIM=$'\033[2m'; OFF=$'\033[0m'
now(){ python3 -c 'import time;print(time.time())'; }
since(){ python3 -c "import sys;print(f'{float(sys.argv[2])-float(sys.argv[1]):.1f}')" "$1" "$(now)"; }
api(){ local m="$1" p="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -sS -X "$m" "$API$p" -H "Authorization: Bearer $ZEROPS_TOKEN" -H 'Content-Type: application/json' -d "$body"
  else
    curl -sS -X "$m" "$API$p" -H "Authorization: Bearer $ZEROPS_TOKEN"
  fi; }

PROJECT_ID=""
cleanup(){ [ -n "$PROJECT_ID" ] && { printf "\n${BOLD}CLEANUP${OFF}\n  deleting %s\n" "$PROJECT_ID"; api DELETE "/project/$PROJECT_ID" >/dev/null && printf "  ${GRN}deleted${OFF}\n"; }; }
trap cleanup EXIT

[ -n "${ZEROPS_TOKEN:-}" ] || { echo "ZEROPS_TOKEN not set"; exit 1; }
CLIENT_ID=$(api GET "/user/info" | python3 -c "
import json,sys
def walk(o):
    if isinstance(o,dict):
        for k,v in o.items():
            if k=='clientId' and isinstance(v,str): yield v
            yield from walk(v)
    elif isinstance(o,list):
        for v in o: yield from walk(v)
print(next(walk(json.load(sys.stdin)),''))")

read -r -d '' YAML <<YML
project:
  name: cost-probe
  location: $REGION
services:
  - hostname: db
    type: postgresql:single@17
    priority: 10
  - hostname: app
    type: ubuntu/nodejs@22
    startWithoutCode: true
    minContainers: 1
    priority: 1
YML
PAYLOAD=$(python3 -c 'import json,sys;print(json.dumps({"yaml":sys.stdin.read()}))' <<<"$YAML")

printf "${BOLD}Spawning a representative sandbox (nodejs + postgres)${OFF}\n"
T0=$(now)
PROJECT_ID=$(api POST "/client/$CLIENT_ID/project/import" "$PAYLOAD" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin).get("projectId",""))')
[ -n "$PROJECT_ID" ] || { echo "import failed"; exit 1; }

for _ in $(seq 1 120); do
  OUT=$(api GET "/project/$PROJECT_ID/service-stack" | python3 _status.py)
  printf "\r  ${DIM}%6ss  %s${OFF}\033[K" "$(since "$T0")" "$(sed -n '2p' <<<"$OUT")"
  [ "$(head -n1 <<<"$OUT")" = "READY" ] && break
  sleep 3
done
echo; echo
printf "${BOLD}Measured resource allocation at idle${OFF}\n"
python3 _cost.py "$PROJECT_ID" "$ZEROPS_TOKEN"
