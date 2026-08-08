# Doorbell

**A tunnel that doesn't lose your webhooks — and a tunnel network you own, deployed from one YAML file in about 90 seconds.**

Every tunnel ever built drops requests on the floor when your laptop isn't there. You close the lid; the payment provider, the git host, the CI job reporting back all get a 502 — and you go re-send each one by hand from whatever console it came from, assuming that sender kept it at all.

```
  doorbell 3000            →  https://…/t/shop/
  ^C                       →  laptop closed
  3 webhooks arrive        →  202 Accepted, held    (not 502)
  doorbell 3000            →  all 3 delivered, in order
```

ngrok, frp, bore, chisel and Cloudflare Tunnel all drop them. Doorbell holds them.

```bash
doorbell 3000
# -> https://quiet-frog.your-domain.com  (or the zero-config URL, no DNS needed)
```

Your laptop stays where it is. The tunnel network runs in *your* Zerops account, on *your* domain, and nobody else's servers sit in the path of your traffic.

---

## The problem

Your app runs on `localhost:3000`. Nothing on the internet can reach it. Three times a week you need it to:

- **Receive a callback.** A payment provider, a GitHub push, a CI job reporting a build, an OAuth redirect, any vendor whose integration you are building — all of them have to POST somewhere, and none of them can POST to `localhost`.
- **Check it on a real phone.** Not Chrome's fake device mode — an actual device on actual mobile Safari.
- **Show a client, right now**, without a deploy.

Tunnels solve this, and the demand is not hypothetical:

| Evidence | What it proves |
|---|---|
| ngrok is a funded company with paid plans | People pay real money for exactly this |
| `frp` — ~90,000 GitHub stars | The free version is top-tier open source by demand |
| bore, chisel, localtunnel, Cloudflare Tunnel | Five established solutions means the need is real, not imagined |

So why another one? Because the free tier of the incumbent gives you a **URL that changes on every restart**, an **interstitial warning page** your client sees, **rate limits**, and **a third party terminating TLS on your traffic**. And the self-hosted alternatives (`frp`, `bore`, `chisel`) ship as a bare daemon — you still have to invent subdomain routing, certificate issuance, auth, and a dashboard yourself.

**The gap is not "a tunnel protocol exists." It is that nobody packaged self-hosted tunnels as a one-click, own-your-domain product.**

---

## Why this needs Zerops specifically

This is the part that is structural, not marketing. A tunnel needs four things:

| Requirement | Zerops | Vercel | Netlify | CF Workers | Lambda |
|---|:--:|:--:|:--:|:--:|:--:|
| Raw TCP port for the control connection | ✅ 10–65435 | ❌ | ❌ | ❌ | ❌ |
| Always-on process (socket must never drop) | ✅ no scale-to-zero | ❌ | ❌ | ❌ | ❌ |
| Wildcard TLS | ✅ ACME DNS-01 | ❌ | ❌ | partial | ❌ |
| Private network between gateway, DB and cache | ✅ VXLAN | ❌ | ❌ | ❌ | ❌ |

A tunnel is one long-lived TCP socket held open for hours. A serverless function runs once per request and then stops, so nothing is left holding the socket. This is not "harder on Vercel" — it is **not expressible** on any HTTP-only platform.

The sharpest way to check that claim: Vercel rejects **WebSocket** handshakes regardless of configuration, and Fluid Compute does not change the connection model. A raw TCP listener is considerably further out of reach than a WebSocket. Half of Doorbell — the mailbox, an HTTP endpoint writing to Postgres — would run there quite happily. The tunnel would not, and the tunnel is the product.

**Honest caveat:** this would run on Fly.io or a $5 VPS. What neither gives you is the whole network — gateway, Postgres, Valkey, dashboard, private networking, managed TLS — standing up from a single pasted file in ~90 seconds. That difference *is* the demo.

---

## Architecture

```
  your laptop                              your Zerops project
  ┌───────────────┐                        ┌──────────────────────────────┐
  │ localhost:3000│                        │  gw  (Go)                    │
  │               │ ── one TCP conn ─────► │  :7000  raw TCP control      │
  │ $ doorbell    │    held open,          │  :3000  HTTP ingress ────────┼──► public
  │     3000      │ ◄── multiplexed ────── │                              │
  └───────────────┘                        │      │            │          │
                                           │   db:5432     cache:6379     │
                                           │  registry    routing table   │
                                           └──────────────────────────────┘
                                                    private VXLAN
```

1. `doorbell 3000` dials the **raw TCP control port** and holds it open.
2. A public request arrives at the HTTP ingress. The gateway looks up which tunnel owns that hostname/path.
3. The request is multiplexed back down the already-open socket to your laptop. Your app answers; the response returns the same way.

**Valkey is not decorative.** A tunnel is pinned to whichever gateway container holds its socket. When the gateway scales to two containers, a request can land on the container that *doesn't* hold the tunnel. Valkey is the shared routing table plus pub/sub that makes horizontal scaling correct instead of silently broken.

---

## Two modes, and the honest tradeoff

Doorbell ships both. You pick when you deploy, and the dashboard tells you what you are giving up.

| | **Zero-config** — running live | **Your own domain** — implemented, not demonstrated |
|---|---|---|
| URL | `gw-abc.zerops.app/t/xyz/` | `xyz.your-domain.com` |
| Setup | none | DNS record + ACME DNS-01 |
| HTTPS | ✅ Zerops-managed cert | ◐ Let's Encrypt wildcard |
| Webhook testing | ✅ perfect | ◐ by design |
| Full web apps | ⚠️ absolute paths need rewriting | ◐ by design |

✅ verified on the live gateway · ◐ implemented and unit-tested, never demonstrated end to end ·
⚠️ works with a documented caveat

The right-hand column is honest about itself: host routing is implemented and covered by seven
unit cases, but it has never run against a real domain. See
[Known limitation](#known-limitation-wildcard-mode-is-routed-but-not-certificated). Zero-config is
the default for exactly that reason — nothing on the critical path depends on it.

Zero-config works the instant the import finishes — no domain, no DNS, no waiting. Path-based routing does mean a page requesting `/css/app.css` needs it rewritten to `/t/xyz/css/app.css`; Doorbell handles the common cases (`Location` headers, `<base href>`, cookie paths) but it is not perfect for every app.

For **receiving callbacks — the single most common reason anyone opens a tunnel — none of that matters.** A sender POSTs to one URL and reads the response. That is why zero-config is the default and the domain is an upgrade, not a prerequisite.

---

## The mailbox

A request for a **reserved** name with no live session is stored in Postgres and answered `202 Accepted`. On reconnect the queue drains into the tunnel in arrival order, and any stored request can be **replayed** on demand — a feature ngrok charges for.

Details that are decisions, not accidents:

- **Delivery is sequential.** Webhook streams are usually causal — created → updated → deleted — and replaying them concurrently can leave an app in a state the real sequence would never have produced.
- **A failed delivery stays queued.** The next reconnect retries. Nothing is silently discarded; that is the entire promise.
- **Only reserved names buffer.** Otherwise a stranger could allocate storage by guessing URLs.
- **Each mailbox is capped at 200**, oldest dropped first — the newest events are the ones you were waiting for.
- **Redacted headers are not replayed** as if they were real credentials; the app would reject them confusingly.

**The honest trade:** answering `202` means Doorbell took responsibility for the request, not that your app processed it. For a *development* tunnel that is exactly the behaviour you want. For a production gateway it would be wrong, and Doorbell is not one.

## What Postgres and Valkey are actually for

Neither is decorative, and both degrade rather than block.

**Valkey fixes a correctness bug, not a performance one.** A tunnel is pinned to whichever gateway container holds its TCP socket — that is physics, the socket lives in one process. But the HTTP ingress load-balances across every container, so a request for `quiet-frog` can land on a container that does not hold it. Each container advertises *"I own `<id>`, reach me at `<addr>`"* into Valkey under a heartbeat-refreshed TTL; a container that cannot serve a tunnel forwards to the one that can, carrying a one-hop header so a stale route can never bounce a request in a loop.

**Postgres makes reserved names durable.** "Your URL changes on every restart" is ngrok's most-cited free-tier complaint, and you cannot fix it with in-memory state. Ownership is a hashed token, enforced by an atomic `INSERT … ON CONFLICT … WHERE owner_hash = EXCLUDED.owner_hash`, so two containers racing for the same name cannot both win.

With neither configured the gateway logs `DEGRADED` and runs single-container with random names. A tunnel is more useful than a database.

## Install the CLI

```bash
go install github.com/danishalisiddiqui/doorbell/cmd/doorbell@latest
```

No Go? Prebuilt binaries for macOS, Linux and Windows:

```bash
make release      # writes dist/doorbell-<os>-<arch>
```

Then point it at a gateway:

```bash
export DOORBELL_GATEWAY=<your-gateway-host>
export DOORBELL_TOKEN=<your-admin-token>
doorbell 3000
```

The gateway refuses any client whose token does not match, and logs a loud warning if you leave the token empty.

## Deploy your own

1. Copy [`zerops-import.yml`](zerops-import.yml)
2. [app.zerops.io](https://app.zerops.io) → **Import a project using YAML template** → paste
3. Wait ~90 seconds
4. **One manual step the import file cannot do:** open the raw TCP control port.
   `gw` → Public access → port routing, public `7000` → internal `7000`, protocol `tcp`.
   Pick IPv6 (free) or a dedicated IPv4 ($3/30d) — a *shared* IPv4 cannot carry raw
   ports, the API rejects it with `publicIpTypeNotSupported`.

## Running it locally

```bash
valkey-server --port 6399 &
createdb doorbell

DATABASE_URL=postgresql://localhost/doorbell \
REDIS_URL=redis://127.0.0.1:6399 \
DOORBELL_PUBLIC_BASE=http://localhost:3000 \
  go run ./cmd/gateway
```

Both backing services are optional — without them the gateway logs `DEGRADED` and runs single-container with random names.

```bash
make test    # 21 tests across 4 packages
```

---

## Verified platform facts

Measured live against a real Zerops account on 7 Aug 2026, before a line of product code was written — not taken from documentation:

- `POST /client/{id}/project/import` → all services ACTIVE in **70.2s** (3 services) / **45.9s** (2 services)
- One idle 3-service project costs **$0.0103/hour** — $0.25/day
- Project-scoped tokens isolate correctly: own project `200`, list all projects `403`, delete own project `403`
- Ports **25 and 465 are permanently blocked** ("no exceptions") — 7000 is clear
- `ListProjectServiceStacks` returns `list`, not `items`
- The API is `v0-beta` and returns transient errors mid-poll — the client retries
- Valkey closes connections idle for 300s and TCP keepalive does **not** reset that timer, so the pub/sub subscriber sends an application-level `PING` every 60s
- The L7 balancer's `send_timeout` defaults to 2s, so the SSE stream emits a comment frame every second — measured 7 heartbeats in 8s through the real balancer

## Multi-container: proven on Zerops

Two gateway services, `gw` and `gw2`, share one Valkey and one Postgres over the project's private network. A tunnel was opened against `gw` only:

```
gw   (holds the socket)      /t/crosstalk/direct     → 200, response from the laptop
gw2  (never saw this tunnel) /t/crosstalk/via-peer   → 200, response from the laptop

tunnels in gw's registry : 1
tunnels in gw2's registry: 0
```

`gw2` has zero tunnels of its own, looked the owner up in Valkey, and forwarded across the VXLAN. That is the whole reason Valkey is in the stack, demonstrated on the real platform.

One caveat worth stating: this is two *services*, not two containers of one service. `PUT /service-stack/{id}/autoscaling` accepts `minContainerCount: 2`, returns HTTP 200 and runs its async process to `FINISHED` — but the setting never persists (`customAutoscaling.horizontalAutoscaling` stays `null`, effective config remains `min 1 / max 2`), and `zcli` has no scale command. Horizontal scaling on this account appears to be load-driven with the floor pinned at one. The code path is identical either way.

## Known limitation: wildcard mode is routed but not certificated

Host-based routing is implemented and unit-tested across seven cases, including the ones that must *not* match (apex domain, multi-label subdomains, unrelated hosts).

It cannot be demonstrated end to end without a real domain, and not for the reason you would expect: **Zerops' L7 balancer routes by Host before the request reaches the gateway.** A spoofed `Host:` header against a `*.zerops.app` subdomain is rejected by the balancer with its own `{"error":{"code":"notFound"}}` — the request never arrives. Exercising wildcard mode therefore requires a custom domain actually pointed at the service, plus an ACME DNS-01 wildcard certificate.

So: routing verified, certificate issuance untested. Zero-config mode is the default precisely so none of this is on the critical path.

## AI disclosure

Claude Code was used for research, API verification scripting, and code assistance. Architecture decisions, the platform-capability analysis behind them, and the trade-offs documented above are my own and I can walk through any of them.
