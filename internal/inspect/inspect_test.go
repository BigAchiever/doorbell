package inspect

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestRingBufferWrapsAndOrders is the test that matters most in this package:
// the ring buffer's index arithmetic is easy to get subtly wrong, and the
// symptom would be a dashboard quietly showing stale or duplicated requests.
func TestRingBufferWrapsAndOrders(t *testing.T) {
	r := NewRecorder(3)

	for _, path := range []string{"/a", "/b", "/c", "/d", "/e"} {
		r.Add(&Record{Path: path})
	}

	got := r.Recent(0)
	if len(got) != 3 {
		t.Fatalf("kept %d records, want 3 (capacity)", len(got))
	}
	// Newest first, and the two oldest must be gone.
	want := []string{"/e", "/d", "/c"}
	for i, w := range want {
		if got[i].Path != w {
			t.Errorf("Recent()[%d] = %q, want %q", i, got[i].Path, w)
		}
	}
}

func TestRecentBeforeWrap(t *testing.T) {
	r := NewRecorder(10)
	r.Add(&Record{Path: "/one"})
	r.Add(&Record{Path: "/two"})

	got := r.Recent(0)
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
	if got[0].Path != "/two" {
		t.Errorf("newest = %q, want /two", got[0].Path)
	}
}

func TestRecentRespectsLimit(t *testing.T) {
	r := NewRecorder(10)
	for _, p := range []string{"/1", "/2", "/3", "/4"} {
		r.Add(&Record{Path: p})
	}
	if got := r.Recent(2); len(got) != 2 {
		t.Errorf("Recent(2) returned %d records, want 2", len(got))
	}
}

func TestIDsAreAssignedAndUnique(t *testing.T) {
	r := NewRecorder(5)
	a := &Record{Path: "/a"}
	b := &Record{Path: "/b"}
	r.Add(a)
	r.AddRemote(b) // remote records must still get a local id, or the dashboard dedupes them away
	if a.ID == 0 || b.ID == 0 {
		t.Fatalf("ids not assigned: a=%d b=%d", a.ID, b.ID)
	}
	if a.ID == b.ID {
		t.Errorf("ids collide: %d", a.ID)
	}
}

// TestSubscriberDoesNotBlockProducer is the safety property that keeps a slow
// dashboard from stalling real HTTP traffic.
func TestSubscriberDoesNotBlockProducer(t *testing.T) {
	r := NewRecorder(100)
	_, release := r.Subscribe() // subscribe, then never read
	defer release()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 500; i++ { // far beyond the 64-slot channel buffer
			r.Add(&Record{Path: "/flood"})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("producer blocked on a slow subscriber")
	}
}

func TestFlattenHeaderRedactsSecrets(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer sk_live_realsecret")
	h.Set("Cookie", "session=abc123")
	h.Set("Stripe-Signature", "t=1,v1=deadbeef")
	h.Set("Content-Type", "application/json")

	got := FlattenHeader(h)

	for _, key := range []string{"Authorization", "Cookie", "Stripe-Signature"} {
		if v := got[key]; !strings.Contains(v, "redacted") {
			t.Errorf("%s = %q, want it redacted", key, v)
		}
	}
	if got["Content-Type"] != "application/json" {
		t.Errorf("Content-Type = %q, want it left alone", got["Content-Type"])
	}
	// The secret must not survive anywhere in the output.
	for k, v := range got {
		if strings.Contains(v, "sk_live_realsecret") || strings.Contains(v, "abc123") {
			t.Errorf("secret leaked through %s = %q", k, v)
		}
	}
}

func TestTruncate(t *testing.T) {
	small := []byte("hello")
	if s, cut := Truncate(small); s != "hello" || cut {
		t.Errorf("Truncate(small) = (%q, %v), want (hello, false)", s, cut)
	}

	big := make([]byte, MaxBodyCapture+100)
	for i := range big {
		big[i] = 'x'
	}
	s, cut := Truncate(big)
	if !cut {
		t.Error("oversized body not reported as cut")
	}
	if len(s) != MaxBodyCapture {
		t.Errorf("truncated to %d bytes, want %d", len(s), MaxBodyCapture)
	}
}
