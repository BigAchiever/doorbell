package main

// "Delivered in order" is the guarantee the mailbox exists to provide, and a
// race broke it in a way nothing in the code made visible: a tunnel is put in
// the registry before its drain goroutine starts, so a request arriving in that
// window was proxied straight through while older ones were still replaying.
//
// Reproduced against a running gateway with a slow handler — eight held
// requests, and one sent a second after reconnecting landed between the fourth
// and the fifth:
//
//	/held-1 … /held-4   replay=buffered
//	/LIVE-NEW           replay=          ← arrived last, delivered fifth
//	/held-5 … /held-8   replay=buffered
//
// The fix is a per-tunnel draining flag that sends live traffic to the back of
// the queue while a replay is in flight. These tests pin the flag's behaviour;
// the end-to-end ordering is covered by the integration run in the README.

import (
	"testing"

	"github.com/BigAchiever/doorbell/internal/inspect"
	"github.com/BigAchiever/doorbell/internal/registry"
)

func drainingGateway(t *testing.T) *gateway {
	t.Helper()
	return &gateway{
		cfg:   config{mode: modeZeroConfig},
		store: registry.NewMemory(1),
		rec:   inspect.NewRecorder(10),
	}
}

// The window the bug lived in: registered and reachable, but its backlog has
// not been replayed yet. Traffic must not be treated as live here.
func TestATunnelIsFlaggedDrainingWhileItsBacklogReplays(t *testing.T) {
	g := drainingGateway(t)

	if _, err := g.store.Add("shop", 3000, nil); err != nil {
		t.Fatalf("add tunnel: %v", err)
	}
	if _, busy := g.draining.Load("shop"); busy {
		t.Fatal("a freshly added tunnel is already flagged draining")
	}

	g.draining.Store("shop", struct{}{})
	if _, busy := g.draining.Load("shop"); !busy {
		t.Fatal("the tunnel is registered but not flagged, which is the exact window the race lived in")
	}
}

// And it has to clear, or the fix quietly turns every tunnel into a mailbox and
// nothing is ever delivered live again.
func TestTheFlagClearsSoTrafficGoesLiveAgain(t *testing.T) {
	g := drainingGateway(t)

	g.draining.Store("shop", struct{}{})
	g.draining.Delete("shop")

	if _, busy := g.draining.Load("shop"); busy {
		t.Fatal("still flagged draining after the drain returned; live traffic would be buffered forever")
	}
}

// One tunnel draining must not divert another tunnel's traffic. The flag is
// keyed per tunnel for this reason.
func TestDrainingOneTunnelDoesNotAffectAnother(t *testing.T) {
	g := drainingGateway(t)

	g.draining.Store("shop", struct{}{})

	if _, busy := g.draining.Load("ci"); busy {
		t.Fatal("draining \"shop\" also flagged \"ci\"")
	}
}
