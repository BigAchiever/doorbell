package main

// The HTTP server, its route table, and the middleware every request passes
// through.

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/BigAchiever/doorbell/internal/dashboard"
	"github.com/BigAchiever/doorbell/internal/ratelimit"
)

func (g *gateway) httpServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})
	// Public by design: the tunnel data path, the landing page, its assets, and
	// a counters-only status endpoint that exposes no traffic.
	mux.HandleFunc("/api/info", g.handleInfo)
	// Public on purpose: the overview draws the same tape as the dashboard, and
	// it carries only tunnel names, times and counts. See handleTimeline.
	mux.HandleFunc("/api/timeline", g.handleTimeline)
	mux.HandleFunc("/t/", g.handleTunnelRequest)
	mux.Handle("/assets/", dashboard.AssetHandler())
	mux.HandleFunc("/", g.handleIndex)

	// Operator surface. Everything here either reveals captured traffic or
	// changes state, so it sits behind the admin token.
	operator := func(h http.HandlerFunc) http.Handler { return g.requireOperator(h) }
	mux.Handle("/api/tunnels", operator(g.handleListTunnels))
	mux.Handle("/api/requests", operator(g.handleListRequests))
	mux.Handle("/api/stream", operator(g.handleStream))
	mux.Handle("/api/history", operator(g.handleHistory))
	mux.Handle("/api/mailbox", operator(g.handleMailbox))
	mux.Handle("/api/replay/", operator(g.handleReplay))
	mux.Handle("/dashboard", operator(g.handleDashboard))
	mux.Handle("/internal/sweep", operator(g.handleSweep))

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
