// Package routing is the shared state that makes more than one gateway
// container correct.
//
// THE PROBLEM IT EXISTS FOR:
//
// A tunnel is pinned to whichever gateway container holds its TCP socket. That
// is not a design choice, it is physics — the socket lives in one process. But
// the HTTP ingress is load-balanced across every container, so a request for
// tunnel "quiet-frog" can easily land on a container that does not hold it.
// With minContainers 1 you never see this. The moment Zerops scales to 2 it
// becomes a coin flip, and the failure looks like a random 404.
//
// So each container publishes "I own quiet-frog, reach me at <addr>" into
// Valkey with a short TTL, refreshed by heartbeat. Any container can then look
// up the owner and forward. That is the whole reason Valkey is in the stack,
// and it is why it is not decorative.
//
// It also carries the inspector event bus, so a dashboard served by container A
// still shows traffic that container B proxied.
//
// Everything degrades: with no Valkey the gateway runs single-container with an
// in-memory table and says so in the logs.
package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	routePrefix = "doorbell:route:"
	eventTopic  = "doorbell:inspect"

	// TTL is deliberately a small multiple of the heartbeat. If a container is
	// killed, its routes must expire fast enough that requests stop being sent
	// into a void — but not so fast that one slow heartbeat unregisters a
	// perfectly healthy tunnel.
	routeTTL      = 30 * time.Second
	HeartbeatMost = 10 * time.Second

	// Zerops' managed Valkey closes connections idle for 300s, and TCP
	// keepalive does NOT reset that timer — it is an application-level idle
	// timeout. A pub/sub subscriber is idle by nature between events, so
	// without an application-level PING it dies silently after five quiet
	// minutes and the dashboard simply stops updating with no error anywhere.
	// Same class of bug as the SSE heartbeat, different layer.
	pubsubPing = 60 * time.Second
)

type Client struct {
	rdb *redis.Client
	// self is how other containers should reach this one over the project's
	// private network.
	self string
}

// Connect dials Valkey. dsn is a redis:// URL; self is this container's
// privately reachable address.
func Connect(ctx context.Context, dsn, self string) (*Client, error) {
	opt, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, fmt.Errorf("routing: bad Valkey URL: %w", err)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("routing: ping: %w", err)
	}
	return &Client{rdb: rdb, self: self}, nil
}

func (c *Client) Close() error { return c.rdb.Close() }

// Self returns this container's address as advertised to peers.
func (c *Client) Self() string { return c.self }

// Announce claims ownership of a tunnel for routeTTL.
func (c *Client) Announce(ctx context.Context, tunnelID string) error {
	return c.rdb.Set(ctx, routePrefix+tunnelID, c.self, routeTTL).Err()
}

// Withdraw releases ownership immediately, rather than waiting for the TTL.
func (c *Client) Withdraw(ctx context.Context, tunnelID string) error {
	return c.rdb.Del(ctx, routePrefix+tunnelID).Err()
}

// Owner returns the address of the container holding tunnelID, or "" if the
// route is unknown or expired.
func (c *Client) Owner(ctx context.Context, tunnelID string) (string, error) {
	addr, err := c.rdb.Get(ctx, routePrefix+tunnelID).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("routing: owner lookup: %w", err)
	}
	return addr, nil
}

// Heartbeat refreshes the TTL on every tunnel this container owns, until ctx
// ends. ids() is called each tick so the caller can hand back a live list
// without this package needing to know about the registry.
func (c *Client) Heartbeat(ctx context.Context, ids func() []string) {
	ticker := time.NewTicker(HeartbeatMost)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, id := range ids() {
				if err := c.Announce(ctx, id); err != nil {
					// Losing one refresh is survivable — the TTL is three
					// heartbeats wide precisely so a blip does not evict a
					// healthy tunnel.
					return
				}
			}
		}
	}
}

// PublishEvent broadcasts an inspector record to every container.
func (c *Client) PublishEvent(ctx context.Context, payload any) error {
	blob, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("routing: marshal event: %w", err)
	}
	return c.rdb.Publish(ctx, eventTopic, blob).Err()
}

// SubscribeEvents delivers inspector records published by any container.
//
// The returned channel is closed when ctx ends. The keepalive goroutine is what
// stops Valkey's 300s idle timeout from silently killing this subscription.
func (c *Client) SubscribeEvents(ctx context.Context) <-chan []byte {
	sub := c.rdb.Subscribe(ctx, eventTopic)
	out := make(chan []byte, 64)

	go func() {
		ticker := time.NewTicker(pubsubPing)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := sub.Ping(ctx); err != nil {
					return
				}
			}
		}
	}()

	go func() {
		defer close(out)
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- []byte(msg.Payload):
				default: // consumer is behind; drop rather than stall the bus
				}
			}
		}
	}()

	return out
}
