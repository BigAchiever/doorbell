// Command gateway is the Doorbell server: a raw TCP control port plus an HTTP
// ingress, in one process.
//
// It is the piece that cannot exist on Vercel, Netlify, Cloudflare Workers or
// Lambda — not because it is hard there, but because those platforms terminate
// HTTP and sleep between requests, and this process must hold a TCP socket open
// for hours. Zerops' lack of scale-to-zero is what makes it possible.
//
// Request path:
//
//	internet -> :3000 /t/<id>/... -> lookup tunnel -> open a yamux stream on the
//	tunnel's existing socket -> ReverseProxy writes the HTTP request into the
//	stream -> the CLI on the developer's laptop pipes it to localhost.
//
// Using httputil.ReverseProxy with a Transport that dials yamux, instead of
// hand-rolling HTTP framing, buys correct handling of chunked bodies, keep-alive
// and WebSocket upgrades for free.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/BigAchiever/doorbell/internal/inspect"
	"github.com/BigAchiever/doorbell/internal/persist"
	"github.com/BigAchiever/doorbell/internal/ratelimit"
	"github.com/BigAchiever/doorbell/internal/registry"
	"github.com/BigAchiever/doorbell/internal/routing"
)

type gateway struct {
	cfg   config
	store registry.Store
	rec   *inspect.Recorder

	// closing is set once shutdown begins so the control listener stops
	// accepting new tunnels while existing ones drain.
	closing atomic.Bool

	limiter *ratelimit.Limiter

	// Both are optional. A nil db means random names and no history; a nil
	// route means single-container operation. Neither stops a tunnel working,
	// which is the point — a tunnel is more useful than a database.
	db    *persist.DB
	route *routing.Client
}

func main() {
	cfg := loadConfig()
	// Seeded from the clock only here, at process start, so tunnel names differ
	// between runs while staying injectable for tests.
	g := &gateway{
		cfg:   cfg,
		store: registry.NewMemory(time.Now().UnixNano()),
		rec:   inspect.NewRecorder(500),
		// Generous on purpose: a page pulling fifty assets or a webhook burst
		// must never trip this. It exists to stop a script hammering someone's
		// laptop through the tunnel, not to shape normal traffic.
		limiter: ratelimit.New(50, 200),
	}

	// SIGTERM is not hypothetical here: it is what Zerops sends on every
	// deploy, restart and scale event. Without handling it the process dies
	// instantly and every live tunnel is severed mid-request — an especially
	// poor look for a product whose whole premise is a long-lived connection,
	// and it quietly breaks the zero-downtime deploy that zerops.yaml asks for.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g.connectPersistence(ctx)
	g.connectRouting(ctx)

	controlLn, err := net.Listen("tcp", ":"+g.cfg.controlPort)
	if err != nil {
		log.Fatalf("control: cannot bind :%s — %v", g.cfg.controlPort, err)
	}
	go g.serveControl(controlLn)

	g.limiter.StartSweeper(ctx.Done(), time.Minute, 10*time.Minute)

	srv := g.httpServer()
	go func() {
		log.Printf("ingress: http listening on :%s (mode=%s)", g.cfg.httpPort, g.cfg.mode)
		// ErrServerClosed is the expected result of a clean Shutdown, not a
		// failure to report.
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("ingress: %v", err)
		}
	}()

	<-ctx.Done()
	g.shutdown(srv, controlLn)
}

// shutdownGrace bounds how long a deploy waits for in-flight work. Zerops sends
// SIGKILL after its own grace period, so overrunning buys nothing.
const shutdownGrace = 15 * time.Second

// shutdown drains in a deliberate order: stop taking new work, tell peers we
// are going away, let in-flight HTTP finish, then cut the tunnels.
func (g *gateway) shutdown(srv *http.Server, controlLn net.Listener) {
	log.Printf("shutdown: draining (grace %s)", shutdownGrace)
	g.closing.Store(true)

	// Stop accepting new tunnels immediately.
	if err := controlLn.Close(); err != nil {
		log.Printf("shutdown: close control listener: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	// Withdraw routes FIRST. Sibling containers must stop forwarding to us
	// before we stop being able to serve, otherwise they proxy into a corpse.
	if g.route != nil {
		for _, t := range g.store.List() {
			if err := g.route.Withdraw(ctx, t.ID); err != nil {
				log.Printf("shutdown: withdraw %s: %v", t.ID, err)
			}
		}
	}

	// Let in-flight requests finish before the sockets they depend on close.
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown: http: %v", err)
	}

	// Now close the tunnels, so each CLI gets a clean session close and can
	// report "the gateway closed the session" rather than hanging.
	live := g.store.List()
	for _, t := range live {
		if session, ok := t.Session().(*yamux.Session); ok {
			if err := session.Close(); err != nil {
				log.Printf("shutdown: close session %s: %v", t.ID, err)
			}
		}
	}
	if len(live) > 0 {
		log.Printf("shutdown: closed %d tunnel(s)", len(live))
	}

	if g.route != nil {
		if err := g.route.Close(); err != nil {
			log.Printf("shutdown: close valkey: %v", err)
		}
	}
	if g.db != nil {
		g.db.Close()
	}
	log.Printf("shutdown: done")
}

// connectPersistence attaches Postgres if configured. Failure is logged and
// tolerated: reserved names and history are features, not preconditions.
func (g *gateway) connectPersistence(ctx context.Context) {
	if g.cfg.databaseURL == "" {
		log.Printf("persist: DATABASE_URL unset — random names, no history")
		return
	}
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	db, err := persist.Open(dialCtx, g.cfg.databaseURL)
	if err != nil {
		log.Printf("persist: DEGRADED, continuing without Postgres: %v", err)
		return
	}
	g.db = db
	log.Printf("persist: Postgres connected — reserved names and history enabled")
}

// connectRouting attaches Valkey if configured, then starts the heartbeat that
// keeps this container's route claims alive and the subscriber that mirrors
// sibling containers' inspector events into the local dashboard.
func (g *gateway) connectRouting(ctx context.Context) {
	if g.cfg.redisURL == "" {
		log.Printf("routing: REDIS_URL unset — single-container mode")
		return
	}
	self, err := routing.SelfAddress(g.cfg.httpPort)
	if err != nil {
		log.Printf("routing: DEGRADED, cannot determine own address: %v", err)
		return
	}
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := routing.Connect(dialCtx, g.cfg.redisURL, self)
	if err != nil {
		log.Printf("routing: DEGRADED, continuing without Valkey: %v", err)
		return
	}
	g.route = client
	log.Printf("routing: Valkey connected — this container advertises itself as %s", self)

	go client.Heartbeat(ctx, func() []string {
		list := g.store.List()
		ids := make([]string, 0, len(list))
		for _, t := range list {
			ids = append(ids, t.ID)
		}
		return ids
	})

	// Mirror sibling containers' records so any dashboard shows all traffic,
	// no matter which container proxied the request.
	go func() {
		for payload := range client.SubscribeEvents(ctx) {
			var rec inspect.Record
			if err := json.Unmarshal(payload, &rec); err != nil {
				continue
			}
			if rec.Origin == client.Self() {
				continue // our own event, already in the local ring buffer
			}
			g.rec.AddRemote(&rec)
		}
	}()
}

// ─── control plane: the raw TCP port ────────────────────────────────────────

// already logs a loud warning about that.

// them — a payment provider, a git host, a CI job, any vendor's webhook console.
// handshake, while writes and connection lifecycle go straight to the socket.

// a gateway that is merely popular.
// handleTunnelRequest serves zero-config mode: /t/<id>/rest/of/path
// in-memory ring buffer forgot on the last deploy.
