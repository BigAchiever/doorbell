// Package ratelimit is a per-client token bucket for the public ingress.
//
// The gateway is reachable by anyone who learns its hostname, and it will
// happily proxy every request into someone's laptop. Without a ceiling, one
// script can saturate a developer's dev server through the tunnel, or fill the
// inspector's ring buffer with noise so real traffic scrolls away.
//
// The limits are deliberately generous. This exists to stop abuse, not to
// shape traffic — a developer testing webhooks or loading a page with fifty
// assets must never notice it.
package ratelimit

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Limiter is a per-key token bucket.
type Limiter struct {
	rate  float64 // tokens added per second
	burst float64 // maximum tokens held

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

// New builds a limiter allowing `rate` requests per second per key, tolerating
// bursts up to `burst`.
func New(rate, burst float64) *Limiter {
	return &Limiter{rate: rate, burst: burst, buckets: make(map[string]*bucket)}
}

// Allow reports whether this key may proceed, consuming a token if so.
func (l *Limiter) Allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		// A new client starts full, so a first request is never rejected.
		l.buckets[key] = &bucket{tokens: l.burst - 1, last: now}
		return true
	}

	l.refill(b, now)

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// refill credits a bucket for the time elapsed since it was last touched,
// capped at burst. Callers hold l.mu.
func (l *Limiter) refill(b *bucket, now time.Time) {
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}
}

// Sweep drops buckets that are idle AND fully refilled.
//
// Without this the map is an unbounded memory leak keyed by attacker-controlled
// input: every distinct source address would allocate a bucket forever.
//
// Refilling before the decision is essential. Reading b.tokens as it was last
// written would mean a bucket left empty stays "empty" forever in the map and
// is never eligible for eviction — precisely the leak this is meant to close.
// Once a bucket has refilled to burst, discarding it is indistinguishable from
// keeping it, because a new client also starts at burst.
func (l *Limiter) Sweep(now time.Time, idle time.Duration) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	removed := 0
	for key, b := range l.buckets {
		if now.Sub(b.last) <= idle {
			continue
		}
		l.refill(b, now)
		if b.tokens >= l.burst-0.001 {
			delete(l.buckets, key)
			removed++
		}
	}
	return removed
}

// Size reports how many buckets are tracked. Exposed for tests and diagnostics.
func (l *Limiter) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// StartSweeper evicts idle buckets periodically until done is closed.
func (l *Limiter) StartSweeper(done <-chan struct{}, every, idle time.Duration) {
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case now := <-t.C:
				l.Sweep(now, idle)
			}
		}
	}()
}

// ClientIP identifies the caller for rate-limiting purposes.
//
// THE ENTRY IT PICKS MATTERS. X-Forwarded-For is a list the client starts and
// each proxy appends to, so the LEFT-most entry is whatever the caller chose to
// send. Keying on it let anyone rotate the header and get a fresh bucket per
// request — measured at 599 of 600 requests slipping through a limiter that
// allowed 203 when the value was fixed.
//
// The RIGHT-most entry is the one appended by the proxy closest to us, which
// the caller cannot forge. On Zerops that is the project's L7 balancer, exactly
// one hop away, so the last entry is the real peer.
//
// X-Real-Ip is only consulted when there is no X-Forwarded-For at all: nginx
// overwrites it, but a deployment without that proxy in front would let a
// client supply it freely.
//
// With no proxy headers we fall back to the transport peer, which is
// unforgeable but collapses every caller behind a balancer into one bucket —
// hence the preference order above.
func ClientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		if last := strings.LastIndexByte(fwd, ','); last >= 0 {
			if ip := trimSpace(fwd[last+1:]); ip != "" {
				return ip
			}
		} else if ip := trimSpace(fwd); ip != "" {
			// A single entry means one hop: the balancer recorded the peer.
			return ip
		}
	}
	if ip := trimSpace(r.Header.Get("X-Real-Ip")); ip != "" {
		return ip
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
