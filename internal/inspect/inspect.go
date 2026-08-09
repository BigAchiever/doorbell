// Package inspect records what went through a tunnel and fans it out live.
//
// This is the half of Doorbell a developer actually looks at. A tunnel that
// works but shows you nothing is strictly worse than ngrok, whose request
// inspector is the reason many people pay for it.
//
// Design constraints that shaped this:
//
//   - Bounded memory. A tunnel left open overnight receiving webhooks must not
//     grow without limit, so records live in a fixed-size ring buffer and bodies
//     are truncated. Losing the oldest request is fine; an OOM on the gateway
//     drops every live tunnel at once, which is not.
//   - Never block the proxy. Publishing to a slow or vanished dashboard must not
//     stall a real HTTP request, so every subscriber has a buffered channel and
//     a full channel drops the event rather than waiting.
package inspect

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// MaxBodyCapture is how much of a request or response body is kept. Webhook
// payloads are comfortably under this; a file upload is not, and does not need
// to be — the point is to show what happened, not to mirror the traffic.
const MaxBodyCapture = 32 << 10 // 32 KiB

// Record is one request/response pair that crossed a tunnel.
type Record struct {
	ID       int64     `json:"id"`
	TunnelID string    `json:"tunnelId"`
	At       time.Time `json:"at"`
	// Origin is the address of the gateway container that proxied this
	// request. It exists so a container can recognise its own events coming
	// back around the Valkey bus and skip them, instead of showing every
	// request twice.
	Origin string `json:"origin,omitempty"`

	// Source marks a request the gateway generated rather than proxied live:
	// "buffered" for one drained from the mailbox on reconnect, "replay" for one
	// re-sent on demand. Empty for ordinary traffic.
	//
	// This is a field rather than a suffix glued onto Path. Path is consumed by
	// API clients and copied out of the inspector, and a decorated one is simply
	// wrong — it stopped being the path the sender asked for.
	Source string `json:"source,omitempty"`

	// PendingID links this record back to the stored request it came from, so
	// the dashboard can offer replay from the request list itself rather than
	// only from the mailbox panel. Zero for ordinary live traffic, which has no
	// stored copy to re-send.
	PendingID int64 `json:"pendingId,omitempty"`

	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Query   string            `json:"query,omitempty"`
	ReqHead map[string]string `json:"reqHead"`
	ReqBody string            `json:"reqBody,omitempty"`
	ReqCut  bool              `json:"reqCut,omitempty"`

	Status   int               `json:"status"`
	ResHead  map[string]string `json:"resHead"`
	ResBody  string            `json:"resBody,omitempty"`
	ResCut   bool              `json:"resCut,omitempty"`
	DurMs    int64             `json:"durMs"`
	Error    string            `json:"error,omitempty"`
	BodySize int64             `json:"bodySize"`
}

// Recorder is a bounded ring buffer plus a live subscriber set.
type Recorder struct {
	mu     sync.RWMutex
	ring   []*Record
	next   int // write cursor into ring
	filled bool
	seq    int64

	subs map[chan *Record]struct{}
}

// NewRecorder builds a recorder holding at most capacity records.
func NewRecorder(capacity int) *Recorder {
	if capacity <= 0 {
		capacity = 200
	}
	return &Recorder{
		ring: make([]*Record, capacity),
		subs: make(map[chan *Record]struct{}),
	}
}

// Add stores a record produced by this container and pushes it to every live
// subscriber.
func (r *Recorder) Add(rec *Record) { r.store(rec) }

// AddRemote stores a record that arrived from a sibling container over the
// Valkey bus. It behaves identically to Add — the distinction exists at the
// call site, where the caller must not re-publish it and start a loop.
func (r *Recorder) AddRemote(rec *Record) { r.store(rec) }

func (r *Recorder) store(rec *Record) {
	r.mu.Lock()
	// IDs are assigned locally even for remote records: sequences are per
	// container, so a remote ID could collide with a local one, and the
	// dashboard dedupes by ID.
	r.seq++
	rec.ID = r.seq
	r.ring[r.next] = rec
	r.next = (r.next + 1) % len(r.ring)
	if r.next == 0 {
		r.filled = true
	}
	// Snapshot subscribers under the lock, then send outside it — holding the
	// mutex across a channel send would let one stalled dashboard block every
	// in-flight request on the gateway.
	targets := make([]chan *Record, 0, len(r.subs))
	for ch := range r.subs {
		targets = append(targets, ch)
	}
	r.mu.Unlock()

	for _, ch := range targets {
		select {
		case ch <- rec:
		default: // subscriber is behind; drop rather than stall the proxy
		}
	}
}

// Recent returns stored records, newest first.
func (r *Recorder) Recent(limit int) []*Record {
	r.mu.RLock()
	defer r.mu.RUnlock()

	n := len(r.ring)
	count := n
	if !r.filled {
		count = r.next
	}
	if limit > 0 && limit < count {
		count = limit
	}

	out := make([]*Record, 0, count)
	for i := 0; i < count; i++ {
		// Walk backwards from the most recently written slot.
		idx := (r.next - 1 - i + n) % n
		if rec := r.ring[idx]; rec != nil {
			out = append(out, rec)
		}
	}
	return out
}

// Subscribe returns a channel of live records and a function to release it.
// The buffer absorbs bursts; a subscriber that falls further behind than this
// loses events rather than applying backpressure to real traffic.
func (r *Recorder) Subscribe() (<-chan *Record, func()) {
	ch := make(chan *Record, 64)
	r.mu.Lock()
	r.subs[ch] = struct{}{}
	r.mu.Unlock()

	return ch, func() {
		r.mu.Lock()
		delete(r.subs, ch)
		r.mu.Unlock()
		close(ch)
	}
}

// Subscribers reports how many dashboards are currently watching.
func (r *Recorder) Subscribers() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.subs)
}

// FlattenHeader collapses an http.Header for display, hiding values that should
// not be echoed onto a shared screen. The inspector is often on a projector or
// in a demo video, and a bearer token rendered in 14px is still a leaked token.
func FlattenHeader(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		if len(v) == 0 {
			continue
		}
		if IsSensitive(k) {
			out[k] = "•••• redacted"
			continue
		}
		out[k] = v[0]
	}
	return out
}

// IsSensitive reports whether a header must never be displayed or replayed.
//
// Exported because replay needs exactly the same judgement: it used to decide
// by sniffing the VALUE for the word "redacted", which both dropped legitimate
// headers and would have started replaying the placeholder as a real
// credential the moment that string changed.
func IsSensitive(key string) bool {
	k := http.CanonicalHeaderKey(key)

	switch k {
	case "Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie":
		return true
	}

	// Naming every provider is a losing game — the list was Stripe-only, so a
	// GitHub delivery put X-Hub-Signature-256 straight into Postgres in the
	// clear. Webhook signing headers converge on a handful of shapes, so match
	// the shape and the next provider is covered before anyone has heard of it.
	//
	// Deliberately broad. A header caught here is redacted AND dropped from a
	// replay, so over-matching costs a header the local app probably could not
	// have used anyway: signatures are computed over a body at a timestamp, and
	// a re-send is neither. Under-matching costs a leaked credential. The
	// asymmetry only points one way.
	for _, frag := range []string{"Signature", "Token", "Secret", "Api-Key", "Apikey", "Hmac"} {
		if strings.Contains(k, frag) {
			return true
		}
	}
	return false
}

// Truncate clips a body to MaxBodyCapture, reporting whether it was cut.
func Truncate(b []byte) (string, bool) {
	if len(b) <= MaxBodyCapture {
		return string(b), false
	}
	return string(b[:MaxBodyCapture]), true
}

// ─── streaming capture ──────────────────────────────────────────────────────

// Capture wraps a body and records the first MaxBodyCapture bytes AS THEY FLOW,
// instead of reading the whole thing up front.
//
// The up-front version was a real bug: io.ReadAll on a response body blocks
// until the origin produces MaxBodyCapture+1 bytes or closes the stream, so any
// tunnelled SSE endpoint, long poll or slow chunked response stalled completely
// — nothing reached the client until 32 KiB had accumulated. That silently
// defeated the deliberate absence of a WriteTimeout on the server, which exists
// precisely so streaming works.
//
// done fires exactly once, when the body reaches EOF or is closed, whichever
// happens first. For an ordinary response that is immediate; for a stream it is
// when the stream ends, so the inspector row for a long-lived stream appears
// when it finishes rather than when it starts. That is the correct trade: the
// developer's traffic must never wait on the inspector.
// mu guards buf and cut. Read runs on whichever goroutine is draining the body
// — for a request that is the transport's write side — while finish() can be
// reached from the handler goroutine via Close at the same moment. sync.Once
// only makes done fire once; it says nothing about the fields done is handed,
// and bytes.Buffer is not safe for concurrent use.
type Capture struct {
	inner io.ReadCloser
	mu    sync.Mutex
	buf   bytes.Buffer
	cut   bool
	once  sync.Once
	done  func(body string, cut bool)
}

// NewCapture wraps body. A nil body yields a nil Capture and done is never
// called, so callers must handle that case themselves.
func NewCapture(body io.ReadCloser, done func(body string, cut bool)) *Capture {
	if body == nil {
		return nil
	}
	return &Capture{inner: body, done: done}
}

func (c *Capture) Read(p []byte) (int, error) {
	n, err := c.inner.Read(p)
	if n > 0 {
		c.mu.Lock()
		if room := MaxBodyCapture - c.buf.Len(); room > 0 {
			if n <= room {
				c.buf.Write(p[:n])
			} else {
				c.buf.Write(p[:room])
				c.cut = true
			}
		} else {
			c.cut = true
		}
		c.mu.Unlock()
	}
	// io.EOF is the normal end of a body, not a failure — finish on any error
	// so a truncated upstream still produces a record.
	if err != nil {
		c.finish()
	}
	return n, err
}

func (c *Capture) Close() error {
	c.finish()
	return c.inner.Close()
}

func (c *Capture) finish() {
	c.once.Do(func() {
		if c.done != nil {
			// Snapshot under the lock, then call done outside it: done reaches
			// back into the proxy's own mutex, and holding both would order two
			// locks against each other for no reason.
			c.mu.Lock()
			body, cut := c.buf.String(), c.cut
			c.mu.Unlock()
			c.done(body, cut)
		}
	})
}
