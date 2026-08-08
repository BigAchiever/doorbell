<h1 align="center">Doorbell</h1>

<p align="center">
  <strong>A self-hosted tunnel that doesn't drop requests when your laptop is closed.</strong><br>
  Held, answered <code>202</code>, and delivered in order when you reconnect.
</p>

<p align="center">
  <a href="#30-second-demo">Demo</a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#why-zerops">Why Zerops</a> ·
  <a href="#known-limitations">Limitations</a>
</p>

<p align="center">
  <img src="docs/hero.png" alt="The Doorbell overview page, showing the live gateway panel with its timeline and counters" width="900">
</p>

<p align="center"><sub>The overview page draws the same timeline the dashboard does, from the same data - so the gateway is visibly running before you read a word.</sub></p>

<!-- A recording of the sequence below is the one thing still missing here.
     Record it, save it as docs/demo.gif, and add:
     <p align="center"><img src="docs/demo.gif" alt="A webhook held while the tunnel is offline, then delivered on reconnect" width="900"></p> -->

```
 $ doorbell 3000
   ◗ shop → https://gw.example.app/t/shop/   forwarding to localhost:3000

 …laptop closes…

 POST /t/shop/hooks/github   →  202 Accepted   held
 POST /t/shop/hooks/github   →  202 Accepted   held
 POST /t/shop/hooks/github   →  202 Accepted   held

 $ doorbell 3000
   ▲ requests held while you were away are arriving now
   ✓ 18:41:02  POST /hooks/github   200   held 6m 12s
   ✓ 18:41:02  POST /hooks/github   200   held 5m 48s
   ✓ 18:41:02  POST /hooks/github   200   held 5m 31s
```

---

## The problem

Something on the internet needs to POST to code running on your machine — a
payment provider, a git push, a CI job reporting back, an OAuth callback. Your
machine has no public address, so you open a tunnel.

Then you shut the lid. Every one of those senders gets a `502`, and you go and
re-send each one by hand from whatever console it came from — assuming that
sender kept it at all.

**Every tunnel has this problem**, because none of them has anywhere to put a
request. Doorbell does: it runs in your own infrastructure, with your own
database in the path.

## Why this is different

| Every other tunnel | Doorbell |
|---|---|
| Laptop offline → the request is lost | Laptop offline → the request is stored |
| Sender gets `502` and starts retrying | Sender gets `202 Accepted` and stops |
| Reconnect starts from now | Reconnect drains the queue, oldest first |
| Nothing to replay — it never arrived | Any stored request, re-sent byte for byte |
| Someone else's servers see your payloads | Your project, your database, your network |

ngrok's request inspector and replay are genuinely good, and free. They just
cannot replay a request that never reached your machine.

<p align="center">
  <img src="docs/comparison.png" alt="A capability comparison between ngrok, Cloudflare Tunnel, localtunnel, frp/bore and Doorbell" width="900">
</p>

---

## 30-second demo

**1.** Point a tunnel at a local port:

```bash
doorbell 3000
```

**2.** Send something while it is connected — it arrives instantly:

```bash
curl -X POST https://<gateway>/t/shop/hook -d '{"n":1}'
```

**3.** Stop the CLI with `^C`, then send three more. Each is answered
`202 Accepted` rather than `502`, and stored:

```bash
curl -i -X POST https://<gateway>/t/shop/hook -d '{"n":2}'
```

**4.** Start the CLI again. All three arrive, in the order they were sent:

```
▲ requests held while you were away are arriving now
✓ 18:41:02  POST /hook   200   held 6m 12s
```

Open `https://<gateway>/dashboard?token=$DOORBELL_TOKEN` to watch the same thing
on a timeline, with the offline stretch shaded and the held requests inside it.

<p align="center">
  <img src="docs/dashboard.png" alt="The Doorbell dashboard: a timeline showing the ci tunnel offline with three requests held, a live shop tunnel, and a request feed mixing held and delivered rows" width="900">
</p>

<p align="center"><sub>
  <code>ci</code> is offline, so its three requests sit in the mailbox marked <b>HELD</b> with the
  time they have been waiting. <code>shop</code> is connected, so its traffic goes straight through
  and returns <code>200</code>. The shaded band on the timeline is the outage itself.
</sub></p>

---

## What it guarantees

| | |
|---|---|
| **Nothing is dropped** | Written to Postgres *before* the sender is answered |
| **Answered immediately** | `202`, so the sender's retry policy never fires |
| **Delivered in order** | The queue drains oldest first |
| **Delivered once** | An atomic lease stops two reconnects double-delivering |
| **Replayable** | Any stored request, re-sent byte for byte, on demand |
| **Stable URL** | Reserved names survive restarts and redeploys |
| **Secrets not stored** | Signing and auth headers are redacted before the database |
| **Visible** | Live request and response bodies, and exactly when a tunnel was offline |

Answering `202` means *"I have taken responsibility for this"*, not *"your app
processed it"*. For a development tunnel that is the behaviour you want. For a
production gateway it would be wrong, and this is not one.

---

## Quick start

You need [Go 1.22+](https://go.dev/dl/) and a gateway. To use one that already
exists, skip to step 2.

**1. Deploy a gateway** (~90 seconds)

Copy [`zerops-import.yml`](zerops-import.yml), then in
[app.zerops.io](https://app.zerops.io) choose **Import a project using a YAML
template** and paste it. When it finishes, open the raw TCP control port:

> `gw` service → **Public access** → **Port routing** → public `7000` →
> internal `7000` → protocol `tcp`

**2. Install the CLI**

```bash
go install github.com/BigAchiever/Doorbell/cmd/doorbell@latest
```

If you get `doorbell: command not found`, `$(go env GOPATH)/bin` is not on your
`PATH`:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

Prebuilt binaries for macOS, Linux and Windows come from `make release`.

**3. Connect**

```bash
export DOORBELL_GATEWAY=<your-gateway-host>
export DOORBELL_TOKEN=<the DOORBELL_ADMIN_TOKEN from your gw service>
doorbell 3000
```

It prints your public URL. Register that URL with whoever is calling you — it
survives restarts, so you only do it once.

---

## Architecture

```
                    your Zerops project
  ┌──────────────────────────────────────────────────┐
  │                                                  │
  │   :3000  HTTPS ingress ──┐                       │
  │                          ├── gateway ── Postgres │  bodies, reservations
  │   :7000  raw TCP  ───────┘      │      Valkey    │  routes, events
  │                                 │                │
  └─────────────────────────────────┼────────────────┘
                                    │ yamux streams
                          the CLI dialled this, outbound
                                    │
                              localhost:3000
```

The CLI opens **one** outbound TCP connection and holds it open. Nothing
listens on your machine and nothing is opened on your router — the gateway
never needs to reach you, it just answers on a pipe you already dialled.

Requests arriving at the gateway become
[yamux](https://github.com/hashicorp/yamux) streams on that existing socket, and
`httputil.ReverseProxy` writes into them. Using the standard library's proxy
rather than hand-rolled framing is what makes chunked bodies, keep-alive and
WebSocket upgrades work for free.

**Postgres** holds anything that must outlive a restart: name reservations and
stored request bodies. **Valkey** holds anything that must be shared *right
now*: which container owns which tunnel, and the event bus that lets a dashboard
on one container show traffic proxied by another.

<details>
<summary><strong>Where each piece lives</strong></summary>

| Path | What it does |
|---|---|
| `cmd/gateway/main.go` | Process lifecycle: wiring, startup, graceful shutdown |
| `cmd/gateway/control.go` | The raw TCP port: accepting and authenticating CLIs |
| `cmd/gateway/proxy.go` | The data path, including forwarding to sibling containers |
| `cmd/gateway/mailbox.go` | Storing requests when no tunnel is connected, and draining them |
| `cmd/gateway/api.go` | JSON and HTML endpoints behind the dashboard |
| `cmd/gateway/auth.go` | Operator gating for anything that exposes captured bodies |
| `cmd/gateway/server.go` | HTTP server, routes, middleware |
| `cmd/gateway/config.go` | Environment configuration |
| `cmd/doorbell` | The CLI |
| `internal/tunnel` | The wire protocol both ends speak |
| `internal/registry` | Which tunnels are live on this container |
| `internal/persist` | Postgres: reservations, stored requests, history |
| `internal/routing` | Valkey: the cross-container routing table and event bus |
| `internal/inspect` | The ring buffer and live fan-out behind the dashboard |
| `internal/ratelimit` | Per-client token bucket on the public ingress |
| `internal/dashboard` | The embedded web interface |
| `tools/verify` | Scripts used to check platform behaviour; not part of the build |

</details>

---

## Why Zerops

Doorbell needs six infrastructure capabilities **at the same time, on one
private network**:

> **A raw public TCP port** · **a process that never sleeps** · **Postgres** ·
> **Valkey** · **private networking** · **managed TLS**

Zerops provisions all six as one project from a single YAML template, in about
ninety seconds. That is the whole reason this project targets it.

| Requirement | Why Doorbell needs it |
|---|---|
| Raw public TCP port | The control channel the CLI dials. Not HTTP |
| A process that never sleeps | A socket held for hours; if it sleeps, every tunnel dies |
| Postgres, privately reachable | The mailbox — what makes `202` an honest answer |
| Valkey | Routing and events across containers |
| A private network | The database holds captured bodies; it must not face the internet |
| Managed TLS and ingress | Senders will not deliver to a bad certificate |

<p align="center">
  <img src="docs/zerops.png" alt="The six infrastructure requirements, what Zerops does about each, and what you would do instead" width="900">
</p>

<details>
<summary><strong>Why not somewhere else?</strong></summary>

Serverless is out entirely: a function stops when it answers, so nothing is left
holding the socket. Vercel rejects even WebSocket handshakes regardless of
configuration, and a raw TCP listener is further out of reach than that. Half of
Doorbell — the mailbox, an HTTP endpoint writing to Postgres — would run almost
anywhere. The tunnel would not, and the tunnel is the product.

**Honest caveat:** any one of these six is easy somewhere. A $5 VPS gives you
the raw port; a managed database gives you Postgres. What no other target gives
you is all six at once, from a single pasted file — which is the difference
between a demo and something you would leave running.

</details>

---

## Local development

Postgres and Valkey are both optional. Without them you get random tunnel names
and no mailbox; the tunnel itself still works.

```bash
createdb doorbell
```

```bash
DATABASE_URL="postgres://$(whoami)@localhost:5432/doorbell?sslmode=disable" \
REDIS_URL="redis://localhost:6379" \
go run ./cmd/gateway
```

Then, in another terminal:

```bash
go run ./cmd/doorbell -gateway localhost 3000
```

## Tests

```bash
go test ./... -race
```

50 tests across nine packages. The suites in `internal/persist` and
`internal/routing` are integration tests: they need a real Postgres and Valkey
and **skip** without them, so this command works on a machine that has neither.
CI runs both as service containers and fails if they skip, because a green run
that silently tested nothing is worse than a red one.

The test worth reading is `TestOnlyOneClaimerWinsARow` — eight goroutines race
to claim the same stored request and exactly one may win. Every extra winner
would be a webhook delivered twice.

`make` runs the tests and builds both binaries. `make release` cross-compiles
the CLI into `dist/`.

---

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `3000` | HTTP ingress |
| `CONTROL_PORT` | `7000` | Raw TCP control port |
| `DOORBELL_MODE` | `zeroconfig` | `zeroconfig` or `wildcard` |
| `DOORBELL_BASE_DOMAIN` | — | Required by `wildcard` mode |
| `DOORBELL_ADMIN_TOKEN` | — | Gates the CLI and the operator surface |
| `DOORBELL_PUBLIC_BASE` | — | External origin, for building tunnel URLs |
| `DATABASE_URL` | — | Postgres. Without it: random names, no mailbox |
| `REDIS_URL` | — | Valkey. Without it: single-container mode |

---

## Known limitations

**Custom-domain mode has never run end to end.** Host routing is implemented and
covered by seven unit cases, including the ones that must *not* match. It cannot
be demonstrated without a real domain, and not for the obvious reason: Zerops'
L7 balancer routes by `Host` *before* the request reaches the gateway, so a
spoofed header against a `*.zerops.app` subdomain is rejected by the balancer
with its own 404. Exercising it needs a domain actually pointed at the service
plus a wildcard certificate. **Routing verified; certificate issuance untested.**

**Zero-config mode is path-based.** A page requesting `/css/app.css` needs it
rewritten to `/t/xyz/css/app.css`. Doorbell handles the common cases
(`Location` headers, `<base href>`, cookie paths) but not every app. This is why
zero-config is the default for webhooks and the domain is an upgrade rather than
a prerequisite.

**The mailbox holds bodies up to 1 MiB, and at most 200 per tunnel.** Over
either limit the request is refused with a clear error rather than stored
truncated — a clipped payload with a matching `Content-Length` is invalid JSON
nobody sent.

---

## Troubleshooting

**`doorbell: command not found`**
`go install` wrote the binary to `$(go env GOPATH)/bin`, which is not on your
`PATH`. Add it, or run the binary by its full path.

**`could not reach the control port`**
The control port is raw TCP, not HTTP. Either a firewall sits between you and
the gateway, or public port routing was never enabled for port 7000. On Zerops
a raw port also needs an IP that supports it — a shared IPv4 does not, IPv6 or
a dedicated IPv4 does.

**Requests return 404 instead of being held**
Only *reserved* names are held. A random name from a fresh tunnel is not
reserved, so a request for it after disconnect is a genuine 404 — otherwise
anyone could allocate storage by guessing URLs. Use `-name <something>`.

**Dashboard returns 401**
It exposes captured request and response bodies, so it needs
`DOORBELL_ADMIN_TOKEN` — as `?token=…` once, or an `Authorization: Bearer`
header. Tunnels keep working without it.

**A page loads through the tunnel but its CSS 404s**
See [Known limitations](#known-limitations); zero-config mode is path-based.

---

## Licence

MIT. See [LICENSE](LICENSE).
