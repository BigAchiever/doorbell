# Doorbell

A self-hosted tunnel that doesn't drop requests when your laptop is closed.

Something on the internet needs to POST to code running on your machine — a
payment provider, a git push, a CI job reporting back. Your machine has no
public address, so you open a tunnel. Then you shut the lid and every one of
those senders gets a 502.

Doorbell answers `202 Accepted` instead, stores the request, and delivers it in
order when you reconnect.

```
doorbell 3000        ->  https://<gateway>/t/shop/
^C                   ->  laptop closed
3 callbacks arrive   ->  202 Accepted, stored
doorbell 3000        ->  all 3 delivered, in order
```

The gateway runs in your own Zerops project. Nobody else's servers sit in the
path of your traffic — which is also why it can hold your payloads at all.

---

## Quick start

You need [Go 1.22+](https://go.dev/dl/) and a gateway to connect to. To use an
existing one, skip to step 2.

**1. Deploy a gateway** (~90 seconds)

Copy [`zerops-import.yml`](zerops-import.yml), then in
[app.zerops.io](https://app.zerops.io) choose **Import a project using a YAML
template** and paste it. When it finishes, open the raw TCP control port:

> `gw` service → **Public access** → **Port routing** → public `7000` →
> internal `7000` → protocol `tcp`

That port is the whole reason this project targets Zerops; see
[Why Zerops](#why-zerops).

**2. Install the CLI**

```bash
go install github.com/danishalisiddiqui/doorbell/cmd/doorbell@latest
```

`go install` puts the binary in `$(go env GOPATH)/bin`. If `doorbell: command
not found`, that directory is not on your `PATH`:

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

## Try the thing it exists for

With a tunnel running, in a second terminal:

```bash
curl -X POST https://<gateway>/t/<name>/hook -d '{"n":1}'
```

Now stop the CLI with `^C` and send three more. Each returns `202 Accepted`
rather than `502`. Start the CLI again and watch them arrive in order:

```
▲ requests held while you were away are arriving now
✓ 18:41:02 POST   /hook   200  held 6m 12s
```

Open `https://<gateway>/dashboard?token=$DOORBELL_TOKEN` to see the same thing
as a timeline, with the offline stretch shaded.

---

## What it guarantees

| | |
|---|---|
| Nothing is dropped | Written to Postgres before the sender is answered |
| Answered immediately | `202`, so the sender's retry policy never fires |
| Delivered in order | The queue drains oldest first |
| Delivered once | An atomic lease stops two reconnects double-delivering |
| Replayable | Any stored request, re-sent byte for byte, on demand |
| Stable URL | Reserved names survive restarts and redeploys |
| Secrets not stored | Signing and auth headers are redacted before the database |

Answering `202` means *"I have taken responsibility for this"*, not *"your app
processed it"*. For a development tunnel that is the behaviour you want. For a
production gateway it would be wrong, and this is not one.

---

## Running it locally

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

Then in another terminal:

```bash
go run ./cmd/doorbell -gateway localhost 3000
```

## Tests

```bash
go test ./... -race
```

`make` runs the tests and builds both binaries. `make release` cross-compiles
the CLI into `dist/`.

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

The CLI opens **one** outbound TCP connection and holds it. Nothing listens on
your machine and nothing is opened on your router. Requests arriving at the
gateway become [yamux](https://github.com/hashicorp/yamux) streams on that
existing socket, and `httputil.ReverseProxy` writes into them — so chunked
bodies, keep-alive and WebSocket upgrades work without hand-rolled framing.

### Where things live

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

**Postgres** holds anything that must outlive a restart: name reservations and
stored request bodies. **Valkey** holds anything that must be shared *right
now*: which container owns which tunnel, and the event bus that lets any
dashboard show traffic from any container.

---

## Why Zerops

Six things have to be true at the same time, on one private network:

| Requirement | Why it is needed |
|---|---|
| A raw public TCP port | The control channel the CLI dials. Not HTTP |
| A process that never sleeps | A socket held for hours; if it sleeps, tunnels die |
| Postgres, privately reachable | The mailbox — what makes `202` honest |
| Valkey | Routing and events across containers |
| A private network | The database holds captured bodies; it must not face the internet |
| Managed TLS and ingress | Senders will not deliver to a bad certificate |

On Zerops that is one YAML file. Elsewhere it is a VPS plus your own firewall,
a database to run and back up, a VPC to keep correct, and certificates to renew.

Serverless is out entirely: a function stops when it answers, so nothing holds
the socket. Vercel rejects even WebSocket handshakes regardless of
configuration — a raw TCP listener is further out of reach still.

**Honest caveat:** any one of these is easy somewhere. A $5 VPS gives you the
raw port. What no other target gives you is all six at once from a single pasted
file.

---

## Two modes

| | Zero-config — **running live** | Custom domain — **implemented, not demonstrated** |
|---|---|---|
| URL | `<gateway>/t/xyz/` | `xyz.your-domain.com` |
| Setup | none | DNS record + ACME DNS-01 |
| HTTPS | Zerops-managed certificate | Let's Encrypt wildcard |
| Full web apps | absolute asset paths need rewriting | no rewriting needed |

Zero-config is the default and needs no domain, no DNS and no waiting. Path
routing means a page requesting `/css/app.css` needs it rewritten to
`/t/xyz/css/app.css`; Doorbell handles the common cases (`Location` headers,
`<base href>`, cookie paths) but not every app.

**Custom-domain mode has never run end to end.** Host routing is implemented and
covered by seven unit cases, including the ones that must *not* match. It cannot
be demonstrated without a real domain, and not for the obvious reason: Zerops'
L7 balancer routes by `Host` *before* the request reaches the gateway, so a
spoofed header against a `*.zerops.app` subdomain is rejected by the balancer
with its own 404. Exercising it needs a domain actually pointed at the service
plus a wildcard certificate. Routing verified; certificate issuance untested.

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

**`413` on a large body**
The mailbox stores bodies whole or not at all; a truncated payload with a
matching `Content-Length` is invalid JSON nobody sent. The limit is 1 MiB.

**Dashboard returns 401**
It exposes captured request and response bodies, so it needs
`DOORBELL_ADMIN_TOKEN` — as `?token=…` once, or an
`Authorization: Bearer` header. Tunnels keep working without it.

**A page loads through the tunnel but its CSS 404s**
Zero-config mode is path-based. Absolute asset paths need rewriting; see
[Two modes](#two-modes).

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

## Licence

MIT. See [LICENSE](LICENSE).
# Doorbell
