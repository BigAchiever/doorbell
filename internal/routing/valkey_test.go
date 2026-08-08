package routing_test

// Valkey is what lets a request arriving at one gateway container reach a
// tunnel connected to a different one. Everything here is about that: who owns
// a tunnel, when they stop owning it, and whether events reach the other side.
//
// Skips when REDIS_URL is unset, so a laptop without Valkey can still run
// `go test ./...`. CI sets it.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/BigAchiever/Doorbell/internal/routing"
)

func connect(t *testing.T, self string) *routing.Client {
	t.Helper()
	dsn := os.Getenv("REDIS_URL")
	if dsn == "" {
		t.Skip("REDIS_URL not set; skipping the tests that need Valkey")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, err := routing.Connect(ctx, dsn, self)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func tunnelName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
}

func TestAnnouncedTunnelIsFoundByItsOwner(t *testing.T) {
	c := connect(t, "10.0.0.1:3000")
	ctx := context.Background()
	name := tunnelName(t)

	if err := c.Announce(ctx, name); err != nil {
		t.Fatalf("announce: %v", err)
	}
	owner, err := c.Owner(ctx, name)
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	if owner != c.Self() {
		t.Errorf("owner: got %q, want %q", owner, c.Self())
	}
}

// A tunnel nobody has announced must not resolve to a container. If it did, a
// sibling would forward a request into a gateway that cannot serve it.
func TestUnknownTunnelHasNoOwner(t *testing.T) {
	c := connect(t, "10.0.0.1:3000")

	owner, err := c.Owner(context.Background(), tunnelName(t))
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	if owner != "" {
		t.Errorf("an unannounced tunnel resolved to %q", owner)
	}
}

// Shutdown withdraws routes before it stops serving. If a withdrawal left the
// claim behind, siblings would keep proxying into a container that has gone.
func TestWithdrawnTunnelStopsResolving(t *testing.T) {
	c := connect(t, "10.0.0.2:3000")
	ctx := context.Background()
	name := tunnelName(t)

	if err := c.Announce(ctx, name); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if err := c.Withdraw(ctx, name); err != nil {
		t.Fatalf("withdraw: %v", err)
	}

	owner, err := c.Owner(ctx, name)
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	if owner != "" {
		t.Errorf("a withdrawn tunnel still resolves to %q", owner)
	}
}

// Two containers, one tunnel: the second announcement wins, because the tunnel
// really is connected to whoever announced most recently.
func TestLatestAnnouncerOwnsTheTunnel(t *testing.T) {
	first := connect(t, "10.0.0.3:3000")
	second := connect(t, "10.0.0.4:3000")
	ctx := context.Background()
	name := tunnelName(t)

	if err := first.Announce(ctx, name); err != nil {
		t.Fatalf("first announce: %v", err)
	}
	if err := second.Announce(ctx, name); err != nil {
		t.Fatalf("second announce: %v", err)
	}

	owner, err := first.Owner(ctx, name)
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	if owner != second.Self() {
		t.Errorf("owner: got %q, want the most recent announcer %q", owner, second.Self())
	}
}

// The event bus is how a dashboard on one container shows traffic proxied by
// another. Without it the inspector only ever sees its own container's work.
func TestAnEventPublishedOnOneClientReachesAnother(t *testing.T) {
	publisher := connect(t, "10.0.0.5:3000")
	subscriber := connect(t, "10.0.0.6:3000")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events := subscriber.SubscribeEvents(ctx)
	// Subscription is asynchronous; publishing immediately can beat it.
	time.Sleep(300 * time.Millisecond)

	want := tunnelName(t)
	if err := publisher.PublishEvent(ctx, map[string]string{"tunnelId": want}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case raw, ok := <-events:
			if !ok {
				t.Fatal("event channel closed before the message arrived")
			}
			var got map[string]string
			if err := json.Unmarshal(raw, &got); err != nil {
				continue // some other test's payload; keep waiting for ours
			}
			if got["tunnelId"] == want {
				return
			}
		case <-deadline:
			t.Fatal("published event never reached the subscriber")
		}
	}
}
