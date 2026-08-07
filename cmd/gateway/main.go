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
	"os"
	"strings"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/danishalisiddiqui/doorbell/internal/registry"
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
}

func loadConfig() config {
	c := config{
		httpPort:    envOr("PORT", "3000"),
		controlPort: envOr("CONTROL_PORT", "7000"),
		mode:        envOr("DOORBELL_MODE", modeZeroConfig),
		baseDomain:  os.Getenv("DOORBELL_BASE_DOMAIN"),
		adminToken:  os.Getenv("DOORBELL_ADMIN_TOKEN"),
		publicBase:  strings.TrimSuffix(os.Getenv("DOORBELL_PUBLIC_BASE"), "/"),
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
}

func main() {
	cfg := loadConfig()
	// Seeded from the clock only here, at process start, so tunnel names differ
	// between runs while staying injectable for tests.
	g := &gateway{cfg: cfg, store: registry.NewMemory(time.Now().UnixNano())}

	go g.serveControl()
	g.serveHTTP()
}

// ─── control plane: the raw TCP port ────────────────────────────────────────

func (g *gateway) serveControl() {
	ln, err := net.Listen("tcp", ":"+g.cfg.controlPort)
	if err != nil {
		log.Fatalf("control: cannot bind :%s — %v", g.cfg.controlPort, err)
	}
	log.Printf("control: raw TCP listening on :%s", g.cfg.controlPort)

	for {
		conn, err := ln.Accept()
		if err != nil {
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

	t, err := g.store.Add(hello.Subdomain, hello.LocalPort, session)
	if err != nil {
		log.Printf("control: %s register: %v", remote, err)
		g.refuse(conn, err.Error())
		session.Close()
		return
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

	// Block until the developer hits Ctrl-C or the network drops.
	<-session.CloseChan()
	g.store.Remove(t.ID)
	log.Printf("tunnel CLOSE %s after %s, %d requests", t.ID, time.Since(t.CreatedAt).Truncate(time.Second), t.Requests)
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

func (g *gateway) serveHTTP() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/api/tunnels", g.handleListTunnels)
	mux.HandleFunc("/t/", g.handleTunnelRequest)
	mux.HandleFunc("/", g.handleIndex)

	srv := &http.Server{
		Addr:    ":" + g.cfg.httpPort,
		Handler: mux,
		// Deliberately no WriteTimeout: a tunnelled response is only as fast as
		// the developer's laptop, and a timeout here would sever long polls and
		// streaming responses that are otherwise working correctly.
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("ingress: http listening on :%s (mode=%s)", g.cfg.httpPort, g.cfg.mode)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("ingress: %v", err)
	}
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
		out = append(out, row{t.ID, t.LocalPort, t.CreatedAt, t.Requests, g.urlFor(t.ID)})
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		log.Printf("api: encode tunnels: %v", err)
	}
}

func (g *gateway) handleIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, `Doorbell gateway

mode          %s
control port  %s (raw TCP)
live tunnels  %d

Point a client at the control port:

    doorbell 3000

ngrok is a SaaS you rent. This is a network you own.
`, g.cfg.mode, g.cfg.controlPort, len(g.store.List()))
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
			t.Requests++
			if loc := resp.Header.Get("Location"); strings.HasPrefix(loc, "/") && !strings.HasPrefix(loc, prefix) {
				resp.Header.Set("Location", prefix+loc)
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			log.Printf("proxy %s: %v", id, err)
			http.Error(w, fmt.Sprintf("doorbell: tunnel %q did not answer — is your local server still running on port %d?", id, t.LocalPort), http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, r)
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
