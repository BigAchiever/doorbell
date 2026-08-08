# Platform verification

Scripts used to check Zerops' behaviour against the docs before the gateway was
written. They are kept because several of the answers shaped the architecture,
and because "we measured it" is a stronger claim with the measurement attached.

| Script | Question it answered |
|---|---|
| `rawtcp/` | Is a raw public TCP port actually reachable from the internet? (The whole project depends on yes.) |
| `gate1.sh` | Does a project with three services provision and come up clean, and how long does it take? |
| `zerops-spike.sh` | Import YAML, service lifecycle, and public port routing over the REST API. |
| `cost.sh`, `_cost.py` | What does an idle project actually cost per hour? |
| `_status.py` | Poll a project's services until they are all ACTIVE. |

They need a Zerops API token in `ZEROPS_TOKEN` and are not part of the build.
Nothing in `cmd/` or `internal/` imports them.
