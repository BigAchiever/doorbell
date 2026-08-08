#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Playpen feasibility spike — Zerops Challenge
#
# Answers the only two questions that decide whether Playpen is buildable:
#   GATE 1  How many seconds from POST project/import to a USABLE project?
#   GATE 2  Does a per-project scoped token actually 403 on everything else?
#
# Creates ONE throwaway project, measures, then deletes it. Nothing else is
# touched. The only project this script can delete is the one it just created —
# the id is printed before deletion.
#
# Usage:
#   export ZEROPS_TOKEN='<personal access token from
#                         https://app.zerops.io/settings/token-management>'
#   ./zerops-spike.sh
#
# NOTE: must be a PERSONAL token. The API forbids integration tokens from
# minting scoped tokens ("Disallowed access for following token types:
# integration"), which is exactly what GATE 2 tests.
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

API="https://api.app-prg1.zerops.io/api/rest/public"
REGION="eu-central"          # eu-central | us-east-1 | us-west-1 — pick the closest
PROJECT_NAME="playpen-spike"

BOLD=$'\033[1m'; RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; DIM=$'\033[2m'; OFF=$'\033[0m'
ok(){   printf "  ${GRN}PASS${OFF}  %s\n" "$1"; }
bad(){  printf "  ${RED}FAIL${OFF}  %s\n" "$1"; }
warn(){ printf "  ${YEL}WARN${OFF}  %s\n" "$1"; }
step(){ printf "\n${BOLD}%s${OFF}\n" "$1"; }
now(){  python3 -c 'import time;print(time.time())'; }
since(){ python3 -c "import sys;print(f'{float(sys.argv[2])-float(sys.argv[1]):.1f}')" "$1" "$(now)"; }

# jq-free JSON field extraction (python3 is guaranteed on macOS)
jget(){ python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception: sys.exit(1)
for k in sys.argv[1].split("."):
    if isinstance(d,list):
        try: d=d[int(k)]
        except Exception: sys.exit(1)
    elif isinstance(d,dict) and k in d: d=d[k]
    else: sys.exit(1)
print(d if not isinstance(d,(dict,list)) else json.dumps(d))' "$1" 2>/dev/null; }

# recursive hunt for a key anywhere in the response (clientId lives in different
# places depending on account shape)
jfind(){ python3 -c '
import json,sys
key=sys.argv[1]
def walk(o):
    if isinstance(o,dict):
        for k,v in o.items():
            if k==key and isinstance(v,str): yield v
            yield from walk(v)
    elif isinstance(o,list):
        for v in o: yield from walk(v)
try: d=json.load(sys.stdin)
except Exception: sys.exit(1)
for v in walk(d): print(v); break' "$1" 2>/dev/null; }

api(){ # api METHOD PATH [BODY] [TOKEN]
  local m="$1" p="$2" body="${3:-}" tok="${4:-$ZEROPS_TOKEN}"
  if [ -n "$body" ]; then
    curl -sS -X "$m" "$API$p" -H "Authorization: Bearer $tok" \
         -H 'Content-Type: application/json' -d "$body" -w $'\n%{http_code}'
  else
    curl -sS -X "$m" "$API$p" -H "Authorization: Bearer $tok" -w $'\n%{http_code}'
  fi
}
code(){ tail -n1 <<<"$1"; }
bodyof(){ sed '$d' <<<"$1"; }

PROJECT_ID=""; SCOPED_TOKEN_ID=""
cleanup(){
  step "CLEANUP"
  if [ -n "$SCOPED_TOKEN_ID" ]; then
    r=$(api DELETE "/client/$CLIENT_ID/integration-token/$SCOPED_TOKEN_ID")
    printf "  scoped token %s deleted (HTTP %s)\n" "$SCOPED_TOKEN_ID" "$(code "$r")"
  fi
  if [ -n "$PROJECT_ID" ]; then
    printf "  deleting ONLY the project this script created: ${BOLD}%s${OFF}\n" "$PROJECT_ID"
    r=$(api DELETE "/project/$PROJECT_ID")
    c=$(code "$r")
    if [ "$c" = "200" ]; then ok "project deleted — no credit left burning"
    else bad "delete returned HTTP $c — DELETE IT BY HAND at https://app.zerops.io"; fi
  fi
}
trap cleanup EXIT

# ── preflight ────────────────────────────────────────────────────────────────
[ -n "${ZEROPS_TOKEN:-}" ] || { echo "${RED}ZEROPS_TOKEN not set.${OFF}"; exit 1; }

step "STEP 0 — token + account flags"
R=$(api GET "/user/info"); C=$(code "$R"); B=$(bodyof "$R")
[ "$C" = "200" ] || { bad "GET /user/info -> HTTP $C"; echo "$B" | head -c 400; exit 1; }
ok "token valid"
CLIENT_ID=$(jfind clientId <<<"$B")
[ -n "$CLIENT_ID" ] || CLIENT_ID=$(jfind id <<<"$B")
[ -n "$CLIENT_ID" ] || { bad "could not find clientId — dumping response:"; echo "$B" | head -c 800; exit 1; }
ok "clientId = $CLIENT_ID"
CCP=$(python3 -c '
import json,sys
def walk(o):
    if isinstance(o,dict):
        for k,v in o.items():
            if k=="canCreateProjects": yield v
            yield from walk(v)
    elif isinstance(o,list):
        for v in o: yield from walk(v)
print(next(walk(json.load(sys.stdin)),"unknown"))' <<<"$B")
if [ "$CCP" = "True" ]; then ok "canCreateProjects = true"
else warn "canCreateProjects = $CCP — ImportProject REQUIRES this flag. If step 1 403s, this is why."; fi

R=$(api GET "/billing/client/$CLIENT_ID/status"); C=$(code "$R"); B=$(bodyof "$R")
if [ "$C" = "200" ]; then
  python3 -c '
import json,sys
d=json.load(sys.stdin); c=d.get("credit",0); p=d.get("promoCredit",0)
print(f"  \033[32mPASS\033[0m  credit=${c:.2f}  promoCredit=${p:.2f}  total=${c+p:.2f}")
print("        \033[2m(recon found conflicting $65 vs $15 claims — this is the real number)\033[0m")
if c+p < 5: print("  \033[33mWARN\033[0m  under $5 — enough for this spike, tight for a 48h project")' <<<"$B"
else warn "could not read credit balance (HTTP $C) — check manually in the console"; fi

# ── GATE 1 ───────────────────────────────────────────────────────────────────
step "GATE 1 — provisioning latency (the number that decides the live-spawn demo)"
IMPORT_YAML=$(cat <<YAML
project:
  name: $PROJECT_NAME
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
YAML
)
PAYLOAD=$(python3 -c 'import json,sys;print(json.dumps({"yaml":sys.stdin.read()}))' <<<"$IMPORT_YAML")

T0=$(now)
R=$(api POST "/client/$CLIENT_ID/project/import" "$PAYLOAD")
C=$(code "$R"); B=$(bodyof "$R")
T_CALL=$(since "$T0")
if [ "$C" != "200" ]; then
  bad "POST project/import -> HTTP $C  (${T_CALL}s)"
  echo "$B" | head -c 900
  echo; echo "${YEL}GATE 1 FAILED — Playpen is not buildable on this account. Switch to PAR/SELECT.${OFF}"
  exit 1
fi
PROJECT_ID=$(jget projectId <<<"$B")
ok "import accepted in ${T_CALL}s — projectId=$PROJECT_ID (3 services requested)"

printf "  ${DIM}polling until every service reports ACTIVE...${OFF}\n"
READY=""; T_READY=""
for i in $(seq 1 90); do
  sleep 4
  R=$(api GET "/project/$PROJECT_ID/service-stack"); B=$(bodyof "$R")
  STAT=$(python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
items=d.get("items",d if isinstance(d,list) else [])
print(" ".join(f"{s.get(\"name\",\"?\")}={s.get(\"status\",\"?\")}" for s in items))' <<<"$B")
  ELAPSED=$(since "$T0")
  printf "\r  ${DIM}%5ss  %s${OFF}\033[K" "$ELAPSED" "$STAT"
  if [ -n "$STAT" ] && ! grep -qvE '=(ACTIVE|READY_TO_DEPLOY)( |$)' <<<"$(tr ' ' '\n' <<<"$STAT" | sed '/^$/d' | tr '\n' ' ')" ; then
    READY=1; T_READY=$ELAPSED; break
  fi
done
echo
if [ -n "$READY" ]; then
  ok "ALL SERVICES USABLE IN ${BOLD}${T_READY}s${OFF}"
  python3 - "$T_READY" <<'EOF'
import sys; t=float(sys.argv[1])
if t<=60:   print("  \033[32m→ under 60s: live-spawn demo works. Warm pool is a nicety, not a requirement.\033[0m")
elif t<=180:print("  \033[33m→ 1-3 min: live-spawn is too slow to watch. WARM POOL BECOMES MANDATORY.\033[0m")
else:       print("  \033[31m→ over 3 min: the spawn demo is dead. Rethink or switch ideas.\033[0m")
EOF
else
  bad "services still not ACTIVE after 6 minutes — treat GATE 1 as failed"
fi

# ── GATE 2 ───────────────────────────────────────────────────────────────────
step "GATE 2 — is the scoped token actually blast-radius-limited?"
printf "  ${DIM}minting: client roleCode=NO_ACCESS, project %s roleCode=BASIC_USER${OFF}\n" "$PROJECT_ID"
TOKBODY=$(python3 -c '
import json,sys
print(json.dumps({"name":"playpen-spike-scoped","roleCode":"NO_ACCESS",
  "canCreateProjects":False,
  "projects":[{"projectId":sys.argv[1],"roleCode":"BASIC_USER"}]}))' "$PROJECT_ID")
R=$(api POST "/client/$CLIENT_ID/integration-token" "$TOKBODY")
C=$(code "$R"); B=$(bodyof "$R")
if [ "$C" != "200" ]; then
  bad "mint scoped token -> HTTP $C"; echo "$B" | head -c 700
  echo; echo "${YEL}GATE 2 FAILED — the safety half of Playpen does not work. Reconsider.${OFF}"
  exit 1
fi
SCOPED=$(jget token <<<"$B"); SCOPED_TOKEN_ID=$(jget id <<<"$B")
ok "scoped token minted (id=$SCOPED_TOKEN_ID)"

R=$(api GET "/project/$PROJECT_ID" "" "$SCOPED"); C=$(code "$R")
[ "$C" = "200" ] && ok "scoped token CAN read its own sandbox (200) — agent can work" \
                 || bad "scoped token cannot read its own project (HTTP $C) — token is useless"

R=$(api GET "/client/$CLIENT_ID/project" "" "$SCOPED"); C=$(code "$R"); B=$(bodyof "$R")
N=$(python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception: print("?"); raise SystemExit
print(len(d.get("items",d if isinstance(d,list) else [])))' <<<"$B")
if [ "$C" = "403" ]; then ok "listing all projects -> 403. Fully blind outside its sandbox."
elif [ "$N" = "1" ]; then ok "listing all projects -> sees exactly 1 (its own). Scoping holds."
else bad "listing all projects -> HTTP $C, sees $N projects. ${BOLD}SCOPING LEAKS — this kills the pitch.${OFF}"; fi

R=$(api DELETE "/project/$PROJECT_ID" "" "$SCOPED"); C=$(code "$R")
if [ "$C" = "403" ]; then ok "scoped token CANNOT delete its own sandbox (403) — DeleteProject needs OWNER/ADMIN"
elif [ "$C" = "200" ]; then bad "scoped token DELETED the project. Agent can destroy its own sandbox."; PROJECT_ID=""
else warn "delete-as-scoped returned HTTP $C (expected 403)"; fi

step "VERDICT"
cat <<EOF
  GATE 1  provisioning latency ....... ${T_READY:-FAILED}s
  GATE 2  scoped-token isolation ..... see PASS/FAIL above

  Both green  -> build Playpen. You now KNOW it works.
  Gate 1 slow -> build Playpen WITH a warm pool from hour 0, not as a stretch goal.
  Either red  -> switch to PAR/SELECT and you lost 20 minutes, not 30 hours.
EOF
