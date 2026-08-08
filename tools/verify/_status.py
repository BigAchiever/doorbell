#!/usr/bin/env python3
"""Parse ListProjectServiceStacks output.

stdout line 1: READY | WAIT | FAILED
stdout line 2: human-readable "name=STATUS name=STATUS ..."

Two bugs lived here before, both mine, both costing a full 6-minute run:
  1. nested quotes inside an f-string inside a bash heredoc -> SyntaxError
  2. the response key is "list", not "items" (verified in the OpenAPI spec:
     ResponseServiceStackList requires ["list", "totalCount"])
Hence: read the envelope defensively, and never report "not ready" when what
actually happened is "could not parse".
"""
import json
import sys

# A runtime service with no code deployed settles on READY_TO_DEPLOY, not
# ACTIVE — treat both as provisioned. SERVICE_ACTIVE is the managed-service
# variant of the same idea.
READY_STATES = {"ACTIVE", "READY_TO_DEPLOY", "SERVICE_ACTIVE"}
DEAD_STATES = {
    "FAILED", "ACTION_FAILED", "REPAIR_FAILED", "CONTAINER_FAILED",
    "SERVICE_FAILED", "SERVICE_ACTION_FAILED", "SERVICE_REPAIR_FAILED",
    "SERVICE_CONTAINER_FAILED",
}


def extract(payload):
    """Return the service array regardless of envelope shape."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("list", "items", "serviceStacks"):
            if isinstance(payload.get(key), list):
                return payload[key]
    return None


raw = sys.stdin.read()
try:
    data = json.loads(raw)
except Exception as exc:  # noqa: BLE001
    print("WAIT")
    print(f"unparseable JSON ({exc}): {raw[:160]}")
    sys.exit(0)

services = extract(data)
if services is None:
    print("WAIT")
    print(f"unrecognised envelope, keys={list(data)[:8] if isinstance(data, dict) else type(data).__name__}")
    sys.exit(0)
if not services:
    print("WAIT")
    print("zero services returned")
    sys.exit(0)

pairs = []
state = "READY"
for svc in services:
    name = svc.get("name") or svc.get("hostname") or "?"
    status = svc.get("status") or "?"
    pairs.append(f"{name}={status}")
    if status in DEAD_STATES:
        state = "FAILED"
    elif status not in READY_STATES and state != "FAILED":
        state = "WAIT"

print(state)
print(" ".join(pairs))
