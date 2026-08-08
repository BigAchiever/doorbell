#!/usr/bin/env bash
# GATE 1 re-run — provisioning latency only.
# Gate 2 already passed on the live account; no need to repeat it.
#
#   ZEROPS_TOKEN="$(cat ~/.zerops-token)" ./gate1.sh
set -uo pipefail

API="https://api.app-prg1.zerops.io/api/rest/public"
REGION="${REGION:-eu-central}"
BOLD=$'\033[1m'; RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; DIM=$'\033[2m'; OFF=$'\033[0m'
now(){ python3 -c 'import time;print(time.time())'; }
since(){ python3 -c "import sys;print(f'{float(sys.argv[2])-float(sys.argv[1]):.1f}')" "$1" "$(now)"; }
api(){ local m="$1" p="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -sS -X "$m" "$API$p" -H "Authorization: Bearer $ZEROPS_TOKEN" -H 'Content-Type: application/json' -d "$body"
  else
    curl -sS -X "$m" "$API$p" -H "Authorization: Bearer $ZEROPS_TOKEN"
  fi; }

PROJECT_ID=""
cleanup(){
  if [ -n "$PROJECT_ID" ]; then
    printf "\n${BOLD}CLEANUP${OFF}\n  deleting only the project this script created: %s\n" "$PROJECT_ID"
    api DELETE "/project/$PROJECT_ID" >/dev/null && printf "  ${GRN}deleted${OFF}\n"
  fi
}
trap cleanup EXIT

[ -n "${ZEROPS_TOKEN:-}" ] || { echo "ZEROPS_TOKEN not set"; exit 1; }
CLIENT_ID=$(api GET "/user/info" | python3 -c '
import json,sys
def walk(o):
    if isinstance(o,dict):
        for k,v in o.items():
            if k=="clientId" and isinstance(v,str): yield v
            yield from walk(v)
    elif isinstance(o,list):
        for v in o: yield from walk(v)
print(next(walk(json.load(sys.stdin)),""))')
[ -n "$CLIENT_ID" ] || { echo "no clientId"; exit 1; }
printf "clientId=%s  region=%s\n" "$CLIENT_ID" "$REGION"

read -r -d '' YAML <<YML
project:
  name: gate1-spike
  location: $REGION
services:
  - hostname: db
    type: postgresql:single@17
    priority: 10
  - hostname: cache
    type: valkey:single@7.2
    priority: 10
  - hostname: api
    type: ubuntu/nodejs@22
    startWithoutCode: true
    enableSubdomainAccess: true
    minContainers: 1
    priority: 1
YML
PAYLOAD=$(python3 -c 'import json,sys;print(json.dumps({"yaml":sys.stdin.read()}))' <<<"$YAML")

printf "\n${BOLD}GATE 1 — provisioning latency${OFF}\n"
T0=$(now)
RESP=$(api POST "/client/$CLIENT_ID/project/import" "$PAYLOAD")
PROJECT_ID=$(python3 -c 'import json,sys;print(json.load(sys.stdin).get("projectId",""))' <<<"$RESP" 2>/dev/null)
if [ -z "$PROJECT_ID" ]; then echo "${RED}import failed:${OFF}"; echo "$RESP" | head -c 600; exit 1; fi
printf "  import accepted in %ss  projectId=%s\n" "$(since "$T0")" "$PROJECT_ID"

T_READY=""
for _ in $(seq 1 120); do
  OUT=$(api GET "/project/$PROJECT_ID/service-stack" | python3 _status.py)
  STATE=$(head -n1 <<<"$OUT"); DETAIL=$(sed -n '2p' <<<"$OUT")
  EL=$(since "$T0")
  printf "\r  ${DIM}%6ss  %s${OFF}\033[K" "$EL" "$DETAIL"
  [ "$STATE" = "READY" ] && { T_READY="$EL"; break; }
  if [ "$STATE" = "FAILED" ]; then
    printf "\n  ${RED}a service entered a FAILED state: %s${OFF}\n" "$DETAIL"; break
  fi
  sleep 3
done
echo
if [ -n "$T_READY" ]; then
  printf "  ${GRN}ALL SERVICES USABLE IN ${BOLD}%ss${OFF}\n" "$T_READY"
  python3 - "$T_READY" <<'PY'
import sys
t = float(sys.argv[1])
if t <= 60:
    print("  \033[32m-> under 60s: live-spawn demo works. Warm pool optional.\033[0m")
elif t <= 180:
    print("  \033[33m-> 1-3 min: too slow to watch live. WARM POOL IS MANDATORY.\033[0m")
else:
    print("  \033[31m-> over 3 min: spawn-on-demand is dead; only a warm pool saves it.\033[0m")
PY
else
  printf "  ${RED}never reached READY within 6 minutes${OFF}\n"
fi
