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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/danishalisiddiqui/doorbell/internal/dashboard"
	"github.com/danishalisiddiqui/doorbell/internal/inspect"
	"github.com/danishalisiddiqui/doorbell/internal/persist"
	"github.com/danishalisiddiqui/doorbell/internal/ratelimit"
	"github.com/danishalisiddiqui/doorbell/internal/registry"
	"github.com/danishalisiddiqui/doorbell/internal/routing"
	"github.com/danishalisiddiqui/doorbell/internal/tunnel"
)

const (
	modeZeroConfig = "zeroconfig"
	modeWildcard   = "wildcard"
)

type config struct {
	httpPort    string
	controlPort string
	mode        string
	baseDomain  string
	adminToken  string
	publicBase  string // externally visible origin, for building tunnel URLs
	databaseURL string
	redisURL    string
}

func loadConfig() config {
	c := config{
		httpPort:    envOr("PORT", "3000"),
		controlPort: envOr("CONTROL_PORT", "7000"),
		mode:        envOr("DOORBELL_MODE", modeZeroConfig),
		baseDomain:  os.Getenv("DOORBELL_BASE_DOMAIN"),
		adminToken:  os.Getenv("DOORBELL_ADMIN_TOKEN"),
		publicBase:  strings.TrimSuffix(os.Getenv("DOORBELL_PUBLIC_BASE"), "/"),
		databaseURL: os.Getenv("DATABASE_URL"),
		redisURL:    os.Getenv("REDIS_URL"),
	}
	// Wildcard mode without a domain is a misconfiguration that would produce
	// broken URLs on every tunnel. Fall back loudly rather than serving them.
	if c.mode == modeWildcard && c.baseDomain == "" {
		log.Printf("WARN mode=wildcard but DOORBELL_BASE_DOMAIN is empty; falling back to zeroconfig")
		c.mode = modeZeroConfig
	}
	if c.adminToken == "" {
		log.Printf("WARN DOORBELL_ADMIN_TOKEN is empty — this gateway accepts ANY client token")
	}
	return c
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

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

func (g *gateway) serveControl(ln net.Listener) {
	log.Printf("control: raw TCP listening on :%s", g.cfg.controlPort)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if g.closing.Load() {
				return // the listener was closed by shutdown, not a fault
			}
			log.Printf("control: accept: %v", err)
			continue
		}
		go g.handleClient(conn)
	}
}

func (g *gateway) handleClient(conn net.Conn) {
	remote := conn.RemoteAddr().String()

	// A peer that connects and then says nothing must not hold a slot forever.
	// Cleared once the handshake completes; yamux keepalives take over after.
	if err := conn.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		log.Printf("control: %s set deadline: %v", remote, err)
		conn.Close()
		return
	}

	// One buffered reader for the whole connection. Handing yamux a fresh
	// reader here would drop whatever this one already buffered past the
	// handshake newline, which shows up later as a hang on the first request.
	br := bufio.NewReader(conn)

	var hello tunnel.Hello
	if err := tunnel.ReadJSONLine(br, &hello); err != nil {
		log.Printf("control: %s handshake failed: %v", remote, err)
		conn.Close()
		return
	}

	if hello.Version != tunnel.ProtocolVersion {
		g.refuse(conn, fmt.Sprintf("protocol version mismatch: gateway speaks v%d, client sent v%d — upgrade the doorbell CLI",
			tunnel.ProtocolVersion, hello.Version))
		return
	}
	if g.cfg.adminToken != "" && hello.Token != g.cfg.adminToken {
		log.Printf("control: %s rejected: bad token", remote)
		g.refuse(conn, "invalid token")
		return
	}

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		log.Printf("control: %s clear deadline: %v", remote, err)
		conn.Close()
		return
	}

	// The gateway opens streams (a request arrived); the CLI accepts them. So
	// the gateway is the yamux client. See internal/tunnel for why.
	ycfg := yamux.DefaultConfig()
	ycfg.LogOutput = os.Stderr
	session, err := yamux.Client(&bufferedConn{Conn: conn, r: br}, ycfg)
	if err != nil {
		log.Printf("control: %s yamux: %v", remote, err)
		conn.Close()
		return
	}

	// A requested name has to be checked against the durable reservation table
	// before the in-memory registry, otherwise two containers could each hand
	// out the same name to different people.
	if hello.Subdomain != "" && g.db != nil {
		claimCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := g.db.ClaimName(claimCtx, hello.Subdomain, hello.Token)
		cancel()
		if errors.Is(err, persist.ErrNameHeld) {
			g.refuse(conn, fmt.Sprintf("the name %q is reserved by someone else", hello.Subdomain))
			return
		} else if err != nil {
			// A database wobble should not stop a tunnel opening; the worst
			// case is a name that is not durably reserved this session.
			log.Printf("control: %s name claim degraded: %v", remote, err)
		}
	}

	t, err := g.store.Add(hello.Subdomain, hello.LocalPort, session)
	if err != nil {
		log.Printf("control: %s register: %v", remote, err)
		g.refuse(conn, err.Error())
		session.Close()
		return
	}

	// Tell every other container where this tunnel lives.
	if g.route != nil {
		annCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := g.route.Announce(annCtx, t.ID); err != nil {
			log.Printf("control: announce %s: %v", t.ID, err)
		}
		cancel()
	}

	publicURL := g.urlFor(t.ID)
	if err := tunnel.WriteJSONLine(conn, tunnel.Welcome{
		Version: tunnel.ProtocolVersion,
		ID:      t.ID,
		URL:     publicURL,
		Mode:    g.cfg.mode,
	}); err != nil {
		log.Printf("control: %s welcome: %v", remote, err)
		g.store.Remove(t.ID)
		session.Close()
		return
	}

	log.Printf("tunnel OPEN  %s -> localhost:%d  (%s)  from %s", t.ID, hello.LocalPort, publicURL, remote)

	// Anything that arrived while this tunnel was away goes in now, before the
	// developer has a chance to wonder where their webhooks went.
	go g.drainMailbox(t.ID, session)

	// Block until the developer hits Ctrl-C or the network drops.
	<-session.CloseChan()
	g.store.Remove(t.ID)
	if g.route != nil {
		// Withdraw explicitly rather than waiting out the TTL, so sibling
		// containers stop forwarding here within milliseconds instead of
		// seconds.
		wCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := g.route.Withdraw(wCtx, t.ID); err != nil {
			log.Printf("control: withdraw %s: %v", t.ID, err)
		}
		cancel()
	}
	log.Printf("tunnel CLOSE %s after %s, %d requests", t.ID, time.Since(t.CreatedAt).Truncate(time.Second), t.Requests())
}

func (g *gateway) refuse(conn net.Conn, reason string) {
	_ = tunnel.WriteJSONLine(conn, tunnel.Welcome{Version: tunnel.ProtocolVersion, Error: reason})
	conn.Close()
}

// urlFor builds the address a developer pastes into Stripe.
func (g *gateway) urlFor(id string) string {
	if g.cfg.mode == modeWildcard {
		return fmt.Sprintf("https://%s.%s", id, g.cfg.baseDomain)
	}
	base := g.cfg.publicBase
	if base == "" {
		base = "http://localhost:" + g.cfg.httpPort
	}
	return fmt.Sprintf("%s/t/%s/", base, id)
}

// bufferedConn lets yamux read from the bufio.Reader that already consumed the
// handshake, while writes and connection lifecycle go straight to the socket.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) { return b.r.Read(p) }

// ─── data plane: the HTTP ingress ───────────────────────────────────────────

func (g *gateway) httpServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/api/info", g.handleInfo)
	mux.HandleFunc("/api/tunnels", g.handleListTunnels)
	mux.HandleFunc("/api/requests", g.handleListRequests)
	mux.HandleFunc("/api/stream", g.handleStream)
	mux.HandleFunc("/api/history", g.handleHistory)
	mux.HandleFunc("/api/mailbox", g.handleMailbox)
	mux.HandleFunc("/api/replay/", g.handleReplay)
	mux.HandleFunc("/t/", g.handleTunnelRequest)
	mux.Handle("/assets/", dashboard.AssetHandler())
	mux.HandleFunc("/dashboard", g.handleDashboard)
	mux.HandleFunc("/", g.handleIndex)

	srv := &http.Server{
		Addr:    ":" + g.cfg.httpPort,
		Handler: g.rateLimit(g.wildcardHost(mux)),
		// Deliberately no WriteTimeout: a tunnelled response is only as fast as
		// the developer's laptop, and a timeout here would sever long polls and
		// streaming responses that are otherwise working correctly.
		ReadHeaderTimeout: 10 * time.Second,
	}
	return srv
}

func (g *gateway) handleListTunnels(w http.ResponseWriter, _ *http.Request) {
	type row struct {
		ID        string    `json:"id"`
		LocalPort int       `json:"localPort"`
		CreatedAt time.Time `json:"createdAt"`
		Requests  int64     `json:"requests"`
		URL       string    `json:"url"`
	}
	list := g.store.List()
	out := make([]row, 0, len(list))
	for _, t := range list {
		out = append(out, row{t.ID, t.LocalPort, t.CreatedAt, t.Requests(), g.urlFor(t.ID)})
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		log.Printf("api: encode tunnels: %v", err)
	}
}

// handleIndex serves the landing page. Someone arriving at the gateway URL for
// the first time needs to be told what this is before being shown a table of
// HTTP requests, so the inspector lives at /dashboard instead of "/".
func (g *gateway) handleIndex(w http.ResponseWriter, r *http.Request) {
	// "/" is the only path that should render the landing page; anything else
	// reaching this catch-all is a genuine 404, not a silent fallback.
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeHTML(w, dashboard.Landing)
}

func (g *gateway) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, dashboard.HTML)
}

func writeHTML(w http.ResponseWriter, page []byte) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if _, err := w.Write(page); err != nil {
		log.Printf("dashboard: write: %v", err)
	}
}

// handleInfo backs the live counters on the landing page.
func (g *gateway) handleInfo(w http.ResponseWriter, r *http.Request) {
	info := map[string]any{
		"mode":        g.cfg.mode,
		"tunnels":     len(g.store.List()),
		"controlPort": g.cfg.controlPort,
		"persistence": g.db != nil,
		"routing":     g.route != nil,
	}
	if g.db != nil {
		if counts, err := g.db.PendingCount(r.Context()); err == nil {
			held := 0
			for _, n := range counts {
				held += n
			}
			info["held"] = held
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(info); err != nil {
		log.Printf("api: encode info: %v", err)
	}
}

func (g *gateway) handleListRequests(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(g.rec.Recent(200)); err != nil {
		log.Printf("api: encode requests: %v", err)
	}
}

// handleStream pushes live records to the dashboard over Server-Sent Events.
//
// THE HEARTBEAT IS LOad-BEARING, not politeness. Zerops' L7 balancer defaults
// send_timeout to 2 seconds — the maximum gap allowed between successive writes
// to a client. An idle SSE stream writes nothing, so the balancer would close
// the connection two seconds after the last request and the dashboard would
// flap between "live" and "reconnecting" forever. A comment frame every second
// keeps the stream under that ceiling without emitting anything the client has
// to parse.
func (g *gateway) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Belt and braces: tells any intermediate nginx not to buffer this response.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Send backlog first so a dashboard opened mid-session is not blank.
	hello, err := json.Marshal(map[string]any{
		"mode":   g.cfg.mode,
		"recent": g.rec.Recent(100),
	})
	if err != nil {
		log.Printf("stream: marshal hello: %v", err)
		return
	}
	fmt.Fprintf(w, "event: hello\ndata: %s\n\n", hello)
	flusher.Flush()

	events, release := g.rec.Subscribe()
	defer release()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case rec, ok := <-events:
			if !ok {
				return
			}
			payload, err := json.Marshal(rec)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: request\ndata: %s\n\n", payload)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

// rateLimit caps how fast any one client can drive the public ingress.
//
// /healthz is exempt: it is called by the platform's own health checker, and
// throttling it would make Zerops believe the service is unhealthy and restart
// a gateway that is merely popular.
func (g *gateway) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		if !g.limiter.Allow(ratelimit.ClientIP(r), time.Now()) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "doorbell: too many requests from your address", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// wildcardHost turns abc.your-domain.com into /t/abc/... before the mux sees it.
//
// Doing the translation here rather than in a separate handler means wildcard
// mode inherits everything path mode already has — peer forwarding, the
// mailbox, the inspector — instead of growing a parallel copy of it that would
// drift.
//
// Only the routing half of wildcard mode lives in Doorbell. Issuing the
// wildcard certificate is the deployer's job (ACME DNS-01 against a domain they
// control); this code assumes TLS has already been terminated in front of it.
func (g *gateway) wildcardHost(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if g.cfg.mode != modeWildcard || g.cfg.baseDomain == "" {
			next.ServeHTTP(w, r)
			return
		}

		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		suffix := "." + g.cfg.baseDomain
		if !strings.HasSuffix(host, suffix) {
			// The apex domain itself still serves the landing page and API.
			next.ServeHTTP(w, r)
			return
		}

		id := strings.TrimSuffix(host, suffix)
		// Only a single label is a tunnel. "a.b.example.com" is not "a-b".
		if id == "" || strings.Contains(id, ".") {
			next.ServeHTTP(w, r)
			return
		}

		r.URL.Path = "/t/" + id + r.URL.Path
		next.ServeHTTP(w, r)
	})
}

// handleTunnelRequest serves zero-config mode: /t/<id>/rest/of/path
func (g *gateway) handleTunnelRequest(w http.ResponseWriter, r *http.Request) {
	id, rest := splitTunnelPath(r.URL.Path)
	if id == "" {
		http.Error(w, "doorbell: malformed tunnel path, expected /t/<id>/...", http.StatusBadRequest)
		return
	}

	t, err := g.store.Get(id)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			// Not ours. It may belong to a sibling container — this is the
			// case that makes maxContainers > 1 work at all.
			if g.forwardToPeer(w, r, id) {
				return
			}
			// Nobody is home. If the owner reserved this name, hold the
			// request instead of dropping it — this is the behaviour no
			// other tunnel has.
			if g.bufferRequest(w, r, id, rest) {
				return
			}
			http.Error(w, fmt.Sprintf("doorbell: no live tunnel named %q", id), http.StatusNotFound)
			return
		}
		http.Error(w, "doorbell: registry error", http.StatusInternalServerError)
		return
	}

	session, ok := t.Session().(*yamux.Session)
	if !ok || session.IsClosed() {
		http.Error(w, "doorbell: tunnel session is closed", http.StatusBadGateway)
		return
	}

	prefix := "/t/" + id
	started := time.Now()

	// The record is filled in from two goroutines — the request body finishes
	// on the transport's write side, the response body on the read side — so
	// its fields are guarded and it is published exactly once, whichever
	// finishes last.
	record := &inspect.Record{
		TunnelID: id,
		At:       started.UTC(),
		Method:   r.Method,
		Path:     rest,
		Query:    r.URL.RawQuery,
		ReqHead:  inspect.FlattenHeader(r.Header),
	}
	var (
		recMu     sync.Mutex
		published sync.Once
	)
	// The RESPONSE is what decides when the record is complete.
	//
	// An earlier version also waited on the request body, which silently lost
	// every request that has none: Go's server never hands you a nil Body, so
	// the capture was installed on an empty reader that nothing ever reads, its
	// callback never fired, and no GET was ever recorded. Request-body capture
	// is therefore best-effort — it fills the field if it completes in time,
	// and a GET simply has nothing to fill it with.
	publish := func() { published.Do(func() { g.finishRecord(record) }) }

	if r.Body != nil {
		r.Body = inspect.NewCapture(r.Body, func(body string, cut bool) {
			recMu.Lock()
			record.ReqBody, record.ReqCut = body, cut
			recMu.Unlock()
		})
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "http"
			// The stream goes straight to the laptop, so Host is only a label.
			// "doorbell.local" keeps it obvious in the developer's own logs.
			req.URL.Host = "doorbell.local"
			req.URL.Path = rest
			// Tell the origin app what the outside world sees, so frameworks
			// that build absolute URLs get them right.
			req.Header.Set("X-Forwarded-Host", r.Host)
			req.Header.Set("X-Forwarded-Proto", schemeOf(r))
			req.Header.Set("X-Doorbell-Tunnel", id)
			req.Header.Set("X-Doorbell-Prefix", prefix)
			// Never let the internal hop marker reach the developer's app.
			req.Header.Del(peerHopHeader)
		},
		Transport: &http.Transport{
			// The whole trick: instead of dialling a network address, open a
			// new multiplexed stream on the socket the laptop already holds open.
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return session.Open()
			},
			ResponseHeaderTimeout: 60 * time.Second,
		},
		// Path-based routing's known weakness: a redirect to /login is correct
		// for the app and wrong for the tunnel. Rewriting Location covers the
		// common case. Absolute asset paths inside HTML bodies are NOT rewritten
		// here — that is the documented caveat, and the reason wildcard mode
		// exists as the upgrade.
		ModifyResponse: func(resp *http.Response) error {
			t.AddRequest()
			if loc := resp.Header.Get("Location"); strings.HasPrefix(loc, "/") && !strings.HasPrefix(loc, prefix) {
				resp.Header.Set("Location", prefix+loc)
			}

			recMu.Lock()
			record.Status = resp.StatusCode
			record.ResHead = inspect.FlattenHeader(resp.Header)
			record.DurMs = time.Since(started).Milliseconds()
			record.BodySize = resp.ContentLength
			recMu.Unlock()

			// Capture as the body flows. Reading it here would block a
			// streaming response until 32 KiB accumulated, which stalled SSE
			// and long polls completely.
			if resp.Body == nil {
				publish()
				return nil
			}
			resp.Body = inspect.NewCapture(resp.Body, func(body string, cut bool) {
				recMu.Lock()
				record.ResBody, record.ResCut = body, cut
				recMu.Unlock()
				publish()
			})
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			log.Printf("proxy %s: %v", id, err)
			recMu.Lock()
			record.Status = http.StatusBadGateway
			record.DurMs = time.Since(started).Milliseconds()
			record.Error = err.Error()
			recMu.Unlock()
			// The transport failed, so no response body will ever arrive;
			// publish now rather than waiting for a callback that cannot fire.
			publish()
			http.Error(w, fmt.Sprintf("doorbell: tunnel %q did not answer — is your local server still running on port %d?", id, t.LocalPort), http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, r)
}

// peerHopHeader marks a request that has already been forwarded once.
//
// Without it a stale Valkey route could bounce a request between two containers
// until something times out. One hop is always enough: the route names the
// container that actually holds the socket, so if that container also cannot
// serve it, the tunnel is genuinely gone.
const peerHopHeader = "X-Doorbell-Peer-Hop"

// forwardToPeer sends a request to the sibling container that owns the tunnel.
// It reports whether the request was handled.
func (g *gateway) forwardToPeer(w http.ResponseWriter, r *http.Request, id string) bool {
	if g.route == nil || r.Header.Get(peerHopHeader) != "" {
		return false
	}

	lookupCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	owner, err := g.route.Owner(lookupCtx, id)
	cancel()
	if err != nil {
		log.Printf("peer: owner lookup %s: %v", id, err)
		return false
	}
	if owner == "" || owner == g.route.Self() {
		// Either nobody claims it, or the route says us and our own registry
		// disagrees — in both cases forwarding would achieve nothing.
		return false
	}

	target := &url.URL{Scheme: "http", Host: owner}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = owner
		// Path is left exactly as received: the owning container runs the same
		// /t/<id>/... routing and will strip the prefix itself.
		req.Header.Set(peerHopHeader, g.route.Self())
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		log.Printf("peer: forward %s -> %s: %v", id, owner, err)
		http.Error(w, fmt.Sprintf("doorbell: tunnel %q lives on another gateway container that did not answer", id), http.StatusBadGateway)
	}
	log.Printf("peer: forwarding %s to %s", id, owner)
	proxy.ServeHTTP(w, r)
	return true
}

// splitTunnelPath turns "/t/quiet-frog/a/b" into ("quiet-frog", "/a/b").
func splitTunnelPath(p string) (id, rest string) {
	trimmed := strings.TrimPrefix(p, "/t/")
	if trimmed == p {
		return "", ""
	}
	if i := strings.IndexByte(trimmed, '/'); i >= 0 {
		return trimmed[:i], trimmed[i:]
	}
	// "/t/quiet-frog" with no trailing slash still means the app's root.
	return trimmed, "/"
}

func schemeOf(r *http.Request) string {
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		return proto
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

// finishRecord is the single exit point for a completed request record: store
// it locally, mirror it to sibling containers, and append a line of durable
// history.
//
// The two remote calls are fired asynchronously and deliberately do not share
// the request's context. By the time this runs the response is already on its
// way to the client, and a slow Valkey or Postgres must never be able to hold
// a user's HTTP request open.
func (g *gateway) finishRecord(rec *inspect.Record) {
	if g.route != nil {
		rec.Origin = g.route.Self()
	}
	g.rec.Add(rec)

	if g.route != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := g.route.PublishEvent(ctx, rec); err != nil {
				log.Printf("bus: publish: %v", err)
			}
		}()
	}

	if g.db != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := g.db.RecordRequest(ctx, rec.TunnelID, rec.At, rec.Method, rec.Path, rec.Status, rec.DurMs, rec.Error); err != nil {
				log.Printf("persist: record: %v", err)
			}
		}()
	}
}

// handleHistory serves durable request history from Postgres — the traffic the
// in-memory ring buffer forgot on the last deploy.
func (g *gateway) handleHistory(w http.ResponseWriter, r *http.Request) {
	if g.db == nil {
		http.Error(w, `{"error":"history unavailable: no database configured"}`, http.StatusServiceUnavailable)
		return
	}
	rows, err := g.db.History(r.Context(), 200)
	if err != nil {
		log.Printf("api: history: %v", err)
		http.Error(w, `{"error":"history query failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(rows); err != nil {
		log.Printf("api: encode history: %v", err)
	}
}
