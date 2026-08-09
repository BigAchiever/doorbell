package main

// The data path: turning a public HTTP request into a stream on the right
// tunnel, or forwarding it to the container that holds one.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/BigAchiever/doorbell/internal/inspect"
	"github.com/BigAchiever/doorbell/internal/registry"
	"github.com/hashicorp/yamux"
)

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

func (g *gateway) handleTunnelRequest(w http.ResponseWriter, r *http.Request) {
	id, rest := splitTunnelPath(r.URL.Path)
	if id == "" {
		http.Error(w, "doorbell: malformed tunnel path, expected /t/<id>/...", http.StatusBadRequest)
		return
	}

	// A drain in flight means older requests are still being replayed. Send this
	// one to the back of that queue rather than ahead of it: "delivered in order"
	// has to mean the order they arrived in, not the order they happened to be
	// proxied in.
	if _, busy := g.draining.Load(id); busy {
		if g.bufferRequest(w, r, id, rest) {
			return
		}
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
	// recMu guards every write to it. Guarding the writes is only half of it:
	// nothing downstream takes recMu to read, so what actually makes this safe
	// is that publish() hands out a copy. See the comment on publish below.
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
	// Publish a COPY, never the record itself. Request-body capture is
	// best-effort and can still be running when the response completes, so the
	// original keeps being written after this point. Everything downstream —
	// the ring buffer, the SSE subscribers, the Valkey publish and the Postgres
	// write — reads without recMu, so handing out the live pointer means those
	// readers race the late callback. A shallow copy is enough: the two header
	// maps are built once, before anything is published, and never rewritten.
	publish := func() {
		published.Do(func() {
			recMu.Lock()
			snapshot := *record
			recMu.Unlock()
			g.finishRecord(&snapshot)
		})
	}

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
