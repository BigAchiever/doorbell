# Doorbell

**ngrok is a SaaS you rent. Doorbell is a tunnel network you own — deployed from one YAML file in about 90 seconds.**

```bash
doorbell 3000
# -> https://quiet-frog.your-domain.com  (or the zero-config URL, no DNS needed)
```

Your laptop stays where it is. The tunnel network runs in *your* Zerops account, on *your* domain, and nobody else's servers sit in the path of your traffic.

---

## The problem

Your app runs on `localhost:3000`. Nothing on the internet can reach it. Three times a week you need it to:

- **Test a webhook.** Stripe or Razorpay has to POST a payment callback somewhere. It cannot post to `localhost`.
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

A tunnel is one long-lived TCP socket held open for hours. A serverless function sleeps between requests; when it sleeps, the socket dies and the tunnel dies with it. This is not "harder on Vercel" — it is **not expressible** on any HTTP-only platform.

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

| | **Zero-config** | **Your own domain** |
|---|---|---|
| URL | `gw-abc.zerops.app/t/xyz/` | `xyz.your-domain.com` |
| Setup | none | DNS record + ACME DNS-01 |
| HTTPS | ✅ Zerops-managed cert | ✅ Let's Encrypt wildcard |
| Webhook testing | ✅ perfect | ✅ perfect |
| Full web apps | ⚠️ absolute paths need rewriting | ✅ perfect |

Zero-config works the instant the import finishes — no domain, no DNS, no waiting. Path-based routing does mean a page requesting `/css/app.css` needs it rewritten to `/t/xyz/css/app.css`; Doorbell handles the common cases (`Location` headers, `<base href>`, cookie paths) but it is not perfect for every app.

For **webhook testing — the single most common reason anyone opens a tunnel — none of that matters.** Stripe POSTs to one URL and reads the response. That is why zero-config is the default and the domain is an upgrade, not a prerequisite.

---

## What Postgres and Valkey are actually for

Neither is decorative, and both degrade rather than block.

**Valkey fixes a correctness bug, not a performance one.** A tunnel is pinned to whichever gateway container holds its TCP socket — that is physics, the socket lives in one process. But the HTTP ingress load-balances across every container, so a request for `quiet-frog` can land on a container that does not hold it. Each container advertises *"I own `<id>`, reach me at `<addr>`"* into Valkey under a heartbeat-refreshed TTL; a container that cannot serve a tunnel forwards to the one that can, carrying a one-hop header so a stale route can never bounce a request in a loop.

**Postgres makes reserved names durable.** "Your URL changes on every restart" is ngrok's most-cited free-tier complaint, and you cannot fix it with in-memory state. Ownership is a hashed token, enforced by an atomic `INSERT … ON CONFLICT … WHERE owner_hash = EXCLUDED.owner_hash`, so two containers racing for the same name cannot both win.

With neither configured the gateway logs `DEGRADED` and runs single-container with random names. A tunnel is more useful than a database.

## Deploy your own

1. Copy [`zerops-import.yml`](zerops-import.yml)
2. [app.zerops.io](https://app.zerops.io) → **Import a project using YAML template** → paste
3. Wait ~90 seconds
4. Set `DOORBELL_ADMIN_TOKEN` on the `gw` service, then:

```bash
export DOORBELL_GATEWAY=<your-gateway-host>
export DOORBELL_TOKEN=<your-admin-token>
doorbell 3000
```

The gateway refuses any client whose token does not match, and logs a loud warning if you leave the token empty.

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

## Known limitation: multi-container is proven, but not on Zerops yet

Peer forwarding is verified end to end — two gateways sharing one Valkey, where the non-owning gateway logged `peer: forwarding peertest to 127.0.0.1:4001` and returned the real response body from the laptop.

It is **not** yet demonstrated across two Zerops containers. `PUT /service-stack/{id}/autoscaling` accepts `customAutoscaling.horizontalAutoscalingNullable.minContainerCount = 2`, returns HTTP 200, runs an async process to `FINISHED` — and the setting does not persist: `customAutoscaling.horizontalAutoscaling` stays `null` and the effective config remains `min 1 / max 2`. `zcli` exposes no scale command. Horizontal scaling on this account appears to be load-driven only, with the floor pinned at one container.

So: the code path is proven, the platform has not been made to exercise it. Stated plainly rather than implied to work.

## AI disclosure

Claude Code was used for research, API verification scripting, and code assistance. Architecture decisions, the platform-capability analysis behind them, and the trade-offs documented above are my own and I can walk through any of them.
