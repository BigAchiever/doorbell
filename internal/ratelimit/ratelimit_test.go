package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBurstThenThrottle(t *testing.T) {
	l := New(10, 3) // 10/s, burst 3
	now := time.Now()

	for i := 0; i < 3; i++ {
		if !l.Allow("1.2.3.4", now) {
			t.Fatalf("request %d rejected inside the burst", i+1)
		}
	}
	if l.Allow("1.2.3.4", now) {
		t.Error("fourth request allowed — burst was not enforced")
	}
}

func TestTokensRefillOverTime(t *testing.T) {
	l := New(10, 2) // 10/s => a token every 100ms
	now := time.Now()

	l.Allow("ip", now)
	l.Allow("ip", now)
	if l.Allow("ip", now) {
		t.Fatal("bucket should be empty")
	}
	if !l.Allow("ip", now.Add(150*time.Millisecond)) {
		t.Error("bucket did not refill after 150ms")
	}
}

func TestClientsAreIndependent(t *testing.T) {
	l := New(10, 1)
	now := time.Now()

	if !l.Allow("a", now) || !l.Allow("b", now) {
		t.Fatal("two different clients should each get their first token")
	}
	if l.Allow("a", now) {
		t.Error("client a was not throttled")
	}
	if l.Allow("b", now) {
		t.Error("client b was not throttled")
	}
}

// TestSweepBoundsMemory guards against the map being an unbounded leak keyed by
// attacker-controlled input.
func TestSweepBoundsMemory(t *testing.T) {
	l := New(10, 1)
	base := time.Now()
	for i := 0; i < 100; i++ {
		l.Allow(string(rune('a'+i%26))+string(rune('0'+i/26)), base)
	}
	if l.Size() == 0 {
		t.Fatal("no buckets recorded")
	}
	// Long enough for every bucket to be idle AND fully refilled.
	l.Sweep(base.Add(time.Hour), time.Minute)
	if got := l.Size(); got != 0 {
		t.Errorf("%d buckets survived the sweep, want 0", got)
	}
}

// TestSweepKeepsStillThrottledClients pins the invariant that makes sweeping
// safe: a bucket may only be discarded once it has refilled to burst, because
// at that point keeping it and recreating it are indistinguishable. A client
// that has NOT had time to refill must survive the sweep, or it could reset its
// own limit just by pausing.
func TestSweepKeepsStillThrottledClients(t *testing.T) {
	// 0.001/s is one token per ~17 minutes, so after two minutes the client has
	// accrued 0.12 of a token — idle, but neither refilled nor owed a request.
	l := New(0.001, 5)
	base := time.Now()
	for i := 0; i < 5; i++ {
		l.Allow("spammer", base)
	}
	if l.Allow("spammer", base) {
		t.Fatal("expected the bucket to be empty")
	}

	l.Sweep(base.Add(2*time.Minute), time.Minute) // idle, but nowhere near refilled
	if l.Size() != 1 {
		t.Fatal("a still-throttled bucket was swept away, letting the client reset")
	}
	if l.Allow("spammer", base.Add(2*time.Minute)) {
		t.Error("the client regained a token it should not have")
	}
}

// TestSpoofedForwardedForCannotEscapeTheBucket is the regression guard for a
// measured bypass: rotating X-Forwarded-For got 599 of 600 requests through a
// limiter that allowed 203 with a fixed value.
func TestSpoofedForwardedForCannotEscapeTheBucket(t *testing.T) {
	l := New(10, 5)
	now := time.Now()

	allowed := 0
	for i := 0; i < 50; i++ {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = "10.0.0.1:5000"
		// Every request claims a different origin, but the trusted proxy's
		// appended entry is always the same real peer.
		r.Header.Set("X-Forwarded-For", "10.20.30."+string(rune('0'+i%10))+", 9.9.9.9")
		if l.Allow(ClientIP(r), now) {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("%d of 50 spoofed requests allowed, want exactly the burst of 5", allowed)
	}
}

func TestClientIPPrefersBalancerHeaders(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		remote  string
		want    string
	}{
		{"X-Real-Ip used when no XFF", map[string]string{"X-Real-Ip": "9.9.9.9"}, "10.0.0.1:5000", "9.9.9.9"},
		// The RIGHT-most entry is the one the trusted proxy appended. Taking the
		// left-most let a caller pick its own bucket by rotating the header.
		{"last X-Forwarded-For entry wins", map[string]string{"X-Forwarded-For": "1.1.1.1, 9.9.9.9"}, "10.0.0.1:5000", "9.9.9.9"},
		{"spoofed prefix is ignored", map[string]string{"X-Forwarded-For": "evil, spoof, 9.9.9.9"}, "10.0.0.1:5000", "9.9.9.9"},
		{"single entry is the peer", map[string]string{"X-Forwarded-For": "  9.9.9.9  "}, "10.0.0.1:5000", "9.9.9.9"},
		{"falls back to RemoteAddr", nil, "10.0.0.1:5000", "10.0.0.1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = tc.remote
			for k, v := range tc.headers {
				r.Header.Set(k, v)
			}
			if got := ClientIP(r); got != tc.want {
				t.Errorf("ClientIP() = %q, want %q", got, tc.want)
			}
		})
	}
}
