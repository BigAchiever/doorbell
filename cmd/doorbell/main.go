// Command doorbell exposes a local port through a Doorbell gateway.
//
//	doorbell 3000
//
// It dials the gateway's raw TCP control port once and keeps that socket open.
// Every inbound request arrives as a new multiplexed stream on that same socket,
// which is what makes this work from behind NAT: the laptop is never dialled,
// it only ever dials out.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/BigAchiever/doorbell/internal/tunnel"
)

// clientVersion is a var, not a const: the linker's -X flag silently does
// nothing to constants, so `make release VERSION=x` used to produce binaries
// that all still reported 0.1.0 — making the version in the handshake useless
// for exactly the support case it exists for.
var clientVersion = "0.1.0"

func main() {
	log.SetFlags(0)

	var (
		gatewayHost = flag.String("gateway", envOr("DOORBELL_GATEWAY", ""), "gateway host (env DOORBELL_GATEWAY)")
		controlPort = flag.String("control-port", envOr("DOORBELL_CONTROL_PORT", "7000"), "gateway raw TCP control port")
		token       = flag.String("token", os.Getenv("DOORBELL_TOKEN"), "auth token (env DOORBELL_TOKEN)")
		subdomain   = flag.String("name", "", "request a stable tunnel name instead of a random one")
		localHost   = flag.String("local-host", "127.0.0.1", "host your app is listening on")
	)
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	localPort, err := strconv.Atoi(flag.Arg(0))
	if err != nil || localPort < 1 || localPort > 65535 {
		log.Fatalf("doorbell: %q is not a valid port", flag.Arg(0))
	}
	if *gatewayHost == "" {
		log.Fatal("doorbell: no gateway set. Pass -gateway <host> or set DOORBELL_GATEWAY.")
	}

	if err := run(*gatewayHost, *controlPort, *token, *subdomain, *localHost, localPort); err != nil {
		log.Fatalf("doorbell: %v", err)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `doorbell — expose a local port through your own tunnel network

usage:
  doorbell [flags] <local-port>

example:
  export DOORBELL_GATEWAY=203.0.113.10     # your gateway's control-port address
  doorbell 3000

DOORBELL_GATEWAY is where the raw TCP control port answers, which is not
always the host that serves the dashboard. On Zerops it never is: the
*.zerops.app name resolves to a shared HTTP balancer that will refuse
port 7000, while the control port is published on the project's own
public IP. The gateway's overview page prints the right address.

An IPv6 address goes in bare, without brackets — those get added here.

flags:
`)
	flag.PrintDefaults()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func run(gatewayHost, controlPort, token, subdomain, localHost string, localPort int) error {
	addr := net.JoinHostPort(gatewayHost, controlPort)

	conn, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		return fmt.Errorf("could not reach the control port at %s: %w\n\n"+
			"The control port is raw TCP, not HTTP. If this hangs, the most likely causes are\n"+
			"a firewall between you and the gateway, or the gateway having no public route for\n"+
			"that port — on Zerops a raw port needs public port routing and an IP that supports\n"+
			"it (a shared IPv4 does not; IPv6 or a dedicated IPv4 does)", addr, err)
	}
	defer conn.Close()

	if err := tunnel.WriteJSONLine(conn, tunnel.Hello{
		Version:       tunnel.ProtocolVersion,
		Token:         token,
		Subdomain:     subdomain,
		LocalPort:     localPort,
		ClientVersion: clientVersion,
	}); err != nil {
		return err
	}

	// Same buffered reader is handed to yamux afterwards, so no bytes are lost
	// between the handshake line and the start of the multiplexed session.
	br := bufio.NewReader(conn)
	var welcome tunnel.Welcome
	if err := tunnel.ReadJSONLine(br, &welcome); err != nil {
		return err
	}
	if welcome.Error != "" {
		return errors.New(welcome.Error)
	}

	session, err := yamux.Server(&bufferedConn{Conn: conn, r: br}, yamux.DefaultConfig())
	if err != nil {
		return fmt.Errorf("multiplex session: %w", err)
	}
	defer session.Close()

	fmt.Printf("\n  \033[1m%s\033[0m\n", welcome.URL)
	fmt.Printf("  → forwarding to %s\n", net.JoinHostPort(localHost, strconv.Itoa(localPort)))
	if welcome.Mode == "zeroconfig" {
		fmt.Printf("\n  \033[2mzero-config mode: the tunnel lives under a path. Webhooks work perfectly.\n" +
			"  Apps that request absolute asset paths (/css/app.css) may need wildcard mode.\033[0m\n")
	}
	fmt.Printf("\n  Ctrl-C to stop.\n\n")

	go acceptStreams(session, localHost, localPort)

	// Wait for Ctrl-C or for the gateway to drop the session.
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigC:
		fmt.Println("\n  tunnel closed.")
	case <-session.CloseChan():
		return errors.New("the gateway closed the session")
	}
	return nil
}

func acceptStreams(session *yamux.Session, localHost string, localPort int) {
	target := net.JoinHostPort(localHost, strconv.Itoa(localPort))
	for {
		stream, err := session.Accept()
		if err != nil {
			return // session closed; run() is already unwinding
		}
		go forward(stream, target)
	}
}

// forward pipes one inbound request stream to the local app and back.
//
// It is deliberately byte-for-byte with no HTTP parsing on this side: the
// gateway already produced a well-formed request, so anything the local server
// understands — chunked bodies, keep-alive, WebSocket upgrades — passes through
// untouched.
func forward(stream net.Conn, target string) {
	defer stream.Close()

	local, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		// Answer in HTTP so the caller sees a real error instead of a dead
		// socket — this is the "you stopped your dev server" case.
		fmt.Fprintf(stream, "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n"+
			"doorbell: nothing is listening on %s\n", target)
		log.Printf("  ! local %s refused the connection — is your app still running?", target)
		return
	}
	defer local.Close()

	// Observe, never intercept. Peek returns bytes without consuming them, so
	// both directions still stream byte-for-byte and this stays true for
	// upgrades and chunked bodies. Only requests the gateway marks as held are
	// reported; ordinary traffic passes in silence.
	reqSide := bufio.NewReader(stream)
	resSide := bufio.NewReader(local)
	held := peekHeld(reqSide)

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(local, reqSide); done <- struct{}{} }()
	go func() {
		if held != nil {
			held.report(peekStatus(resSide))
		}
		_, _ = io.Copy(stream, resSide)
		done <- struct{}{}
	}()
	<-done
}

/* ── reporting a drain ───────────────────────────────────────────────────── */

// The moment a tunnel reconnects, everything held while it was away arrives at
// once. Without this the most persuasive thing the product does happens in
// total silence.

var (
	drainMu   sync.Mutex
	drainOnce sync.Once
)

type heldReq struct {
	method, path string
	waited       time.Duration
}

// peekHeld returns details of a held request, or nil for ordinary traffic.
//
// Peek(1) blocks only until the first byte, then reports whatever the same read
// happened to buffer — in practice the whole request head. Asking for a fixed
// size instead would block waiting for bytes a small request never sends.
func peekHeld(br *bufio.Reader) *heldReq {
	if _, err := br.Peek(1); err != nil {
		return nil
	}
	head, err := br.Peek(br.Buffered())
	if err != nil && len(head) == 0 {
		return nil
	}

	lines := strings.Split(string(head), "\r\n")
	if len(lines) == 0 {
		return nil
	}
	parts := strings.Fields(lines[0])
	if len(parts) < 2 {
		return nil
	}

	out := &heldReq{method: parts[0], path: parts[1]}
	var replayed bool
	for _, ln := range lines[1:] {
		k, v, ok := strings.Cut(ln, ":")
		if !ok {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "x-doorbell-replay":
			replayed = strings.TrimSpace(v) == "buffered"
		case "x-doorbell-original-time":
			if t, err := time.Parse(time.RFC3339, strings.TrimSpace(v)); err == nil {
				out.waited = time.Since(t)
			}
		}
	}
	if !replayed {
		return nil
	}
	return out
}

// peekStatus reads the response status code without consuming it. Zero if the
// first line is not a status line we recognise.
func peekStatus(br *bufio.Reader) int {
	if _, err := br.Peek(1); err != nil {
		return 0
	}
	head, err := br.Peek(br.Buffered())
	if err != nil && len(head) == 0 {
		return 0
	}
	parts := strings.Fields(string(head))
	if len(parts) < 2 || !strings.HasPrefix(parts[0], "HTTP/") {
		return 0
	}
	code, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0
	}
	return code
}

func (h *heldReq) report(status int) {
	drainMu.Lock()
	defer drainMu.Unlock()

	drainOnce.Do(func() {
		fmt.Printf("\n  \033[33m▲ requests held while you were away are arriving now\033[0m\n")
	})

	mark, colour := "\033[32m✓\033[0m", "\033[32m"
	if status == 0 || status >= 400 {
		mark, colour = "\033[31m✕\033[0m", "\033[31m"
	}
	code := "—"
	if status > 0 {
		code = strconv.Itoa(status)
	}
	fmt.Printf("  %s %s %-6s %-34s %s%s\033[0m  \033[2mheld %s\033[0m\n",
		mark, time.Now().Format("15:04:05"), h.method, truncate(h.path, 34),
		colour, code, humanWait(h.waited))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func humanWait(d time.Duration) string {
	secs := int(d.Seconds())
	if secs < 1 {
		return "<1s"
	}
	if secs < 60 {
		return fmt.Sprintf("%ds", secs)
	}
	return fmt.Sprintf("%dm %02ds", secs/60, secs%60)
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) { return b.r.Read(p) }
