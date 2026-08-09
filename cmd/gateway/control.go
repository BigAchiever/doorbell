package main

// The raw TCP control plane: accepting CLI connections, authenticating
// them, and handing the socket to yamux.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/BigAchiever/doorbell/internal/persist"
	"github.com/BigAchiever/doorbell/internal/tunnel"

	"github.com/hashicorp/yamux"
)

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
	if !g.tokenOK(hello.Token) {
		log.Printf("control: %s rejected: bad token", remote)
		g.refuse(conn, "invalid token")
		return
	}

	// The durable name claim happens BEFORE the multiplexer is started.
	//
	// It used to run after, which meant the refusal path wrote a plain JSON
	// line onto a socket yamux already owned — a keepalive frame could
	// interleave and the client would report "malformed handshake" instead of
	// the real reason — and it leaked the session, since refuse() only closes
	// the connection.
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
	//
	// The flag goes up BEFORE the goroutine starts, not inside it. Set it inside
	// and there is a window where the tunnel is already registered but not yet
	// marked draining, and a request arriving in that window is proxied straight
	// past a queue of older ones.
	g.draining.Store(t.ID, struct{}{})
	go func() {
		defer g.draining.Delete(t.ID)
		g.drainMailbox(t.ID, session)
	}()

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

// tokenOK reports whether a presented token matches the configured one.
//
// The comparison is constant-time. A plain == returns at the first differing
// byte, which on an internet-reachable port leaks the token one byte at a time
// to anyone willing to measure rejection latency.
//
// An empty configured token means this gateway is unauthenticated; loadConfig

func (g *gateway) refuse(conn net.Conn, reason string) {
	_ = tunnel.WriteJSONLine(conn, tunnel.Welcome{Version: tunnel.ProtocolVersion, Error: reason})
	conn.Close()
}

// urlFor builds the address a developer registers with whatever is calling

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) { return b.r.Read(p) }

// ─── data plane: the HTTP ingress ───────────────────────────────────────────
