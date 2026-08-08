package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/BigAchiever/Doorbell/internal/inspect"
	"github.com/BigAchiever/Doorbell/internal/registry"
)

// newTestTunnel wires a real yamux session over an in-memory pipe and serves
// `origin` on the far side, exactly as the doorbell CLI does. Using the real
// multiplexer rather than a stub is the point: the bug these tests guard
// against lived in how bodies flow through it.
func newTestTunnel(t *testing.T, origin http.Handler) (*gateway, string) {
	t.Helper()

	gwConn, cliConn := net.Pipe()

	gwSession, err := yamux.Client(gwConn, yamux.DefaultConfig())
	if err != nil {
		t.Fatalf("yamux client: %v", err)
	}
	cliSession, err := yamux.Server(cliConn, yamux.DefaultConfig())
	if err != nil {
		t.Fatalf("yamux server: %v", err)
	}
	t.Cleanup(func() { _ = gwSession.Close(); _ = cliSession.Close() })

	// The CLI end: accept each stream and serve one HTTP request on it.
	go func() {
		srv := &http.Server{Handler: origin}
		_ = srv.Serve(streamListener{cliSession})
	}()

	g := &gateway{
		cfg:   config{mode: modeZeroConfig, httpPort: "3000"},
		store: registry.NewMemory(1),
		rec:   inspect.NewRecorder(50),
	}
	if _, err := g.store.Add("t1", 8080, gwSession); err != nil {
		t.Fatalf("register tunnel: %v", err)
	}
	return g, "t1"
}

// streamListener adapts a yamux session to net.Listener so http.Server can
// serve over it.
type streamListener struct{ s *yamux.Session }

func (l streamListener) Accept() (net.Conn, error) { return l.s.Accept() }
func (l streamListener) Close() error              { return l.s.Close() }
func (l streamListener) Addr() net.Addr            { return dummyAddr{} }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "yamux" }
func (dummyAddr) String() string  { return "doorbell.local" }

// waitForRecords polls the recorder, because the record is published from the
// body-capture callback rather than synchronously with the response.
func waitForRecords(t *testing.T, g *gateway, want int) []*inspect.Record {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got := g.rec.Recent(0); len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d record(s); recorder has %d", want, len(g.rec.Recent(0)))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestBodylessRequestIsRecorded is a regression guard for a real bug.
//
// The streaming-capture rework gated publication on BOTH the request and the
// response body finishing. Go's server never hands you a nil Body, so a GET got
// a capture installed on an empty reader that nothing ever read — the callback
// never fired, and no GET was ever recorded. The tunnel's own counter went up
// while the inspector stayed empty, which is exactly the kind of divergence
// nobody notices until a demo.
func TestBodylessRequestIsRecorded(t *testing.T) {
	g, id := newTestTunnel(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"ok":true}`)
	}))

	rec := httptest.NewRecorder()
	g.handleTunnelRequest(rec, httptest.NewRequest(http.MethodGet, "/t/"+id+"/api/users", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	records := waitForRecords(t, g, 1)
	if got := records[0].Method; got != http.MethodGet {
		t.Errorf("recorded method = %q, want GET", got)
	}
	if got := records[0].Path; got != "/api/users" {
		t.Errorf("recorded path = %q, want /api/users", got)
	}
}

func TestRequestAndResponseBodiesAreCaptured(t *testing.T) {
	g, id := newTestTunnel(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, `{"echo":%s}`, body)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/t/"+id+"/hook", strings.NewReader(`{"n":1}`))
	g.handleTunnelRequest(rec, req)

	records := waitForRecords(t, g, 1)
	if !strings.Contains(records[0].ReqBody, `"n":1`) {
		t.Errorf("request body not captured, got %q", records[0].ReqBody)
	}
	if !strings.Contains(records[0].ResBody, `"echo"`) {
		t.Errorf("response body not captured, got %q", records[0].ResBody)
	}
}

// streamRecorder is a ResponseWriter that is safe to observe while it is being
// written to. httptest.ResponseRecorder is not: reading its Body from the test
// goroutine while the proxy writes from another is a data race in the test
// harness, not in the code under test.
type streamRecorder struct {
	mu    sync.Mutex
	buf   strings.Builder
	hdr   http.Header
	first chan struct{}
	once  sync.Once
}

func newStreamRecorder() *streamRecorder {
	return &streamRecorder{hdr: http.Header{}, first: make(chan struct{})}
}

func (s *streamRecorder) Header() http.Header { return s.hdr }

func (s *streamRecorder) Write(p []byte) (int, error) {
	s.mu.Lock()
	n, err := s.buf.Write(p)
	s.mu.Unlock()
	if n > 0 {
		s.once.Do(func() { close(s.first) })
	}
	return n, err
}

func (s *streamRecorder) WriteHeader(int) {}
func (s *streamRecorder) Flush()          {}

func (s *streamRecorder) body() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestStreamingResponseIsNotBuffered proves the capture no longer blocks a
// stream. The origin writes one small chunk, flushes, and only finishes after
// the proxy has already delivered it — which cannot happen if the gateway is
// reading the whole body up front.
func TestStreamingResponseIsNotBuffered(t *testing.T) {
	release := make(chan struct{})
	g, id := newTestTunnel(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-release // hold the stream open
		fmt.Fprint(w, "data: last\n\n")
	}))

	rec := newStreamRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.handleTunnelRequest(rec, httptest.NewRequest(http.MethodGet, "/t/"+id+"/events", nil))
	}()

	// The first chunk must arrive while the origin is still holding the
	// response open. If the gateway were reading the body up front, nothing
	// would be written until after release is closed.
	select {
	case <-rec.first:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("first chunk never arrived — the capture is buffering the stream")
	}
	if got := rec.body(); !strings.Contains(got, "data: first") {
		close(release)
		t.Fatalf("first write was %q, want it to contain the first event", got)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("proxy did not finish after the stream closed")
	}
}

func TestUnknownTunnelStillReportsNotFound(t *testing.T) {
	g, _ := newTestTunnel(t, http.NotFoundHandler())

	rec := httptest.NewRecorder()
	g.handleTunnelRequest(rec, httptest.NewRequest(http.MethodGet, "/t/ghost/x", nil))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}
