// Package registry tracks which tunnels are live and which yamux session serves
// each one.
//
// Today this is in-memory, which is correct for exactly one gateway container.
// The moment the gateway scales to two, it stops being correct: a tunnel is
// pinned to whichever container holds its TCP socket, so a request landing on
// the other container finds nothing here and 404s.
//
// That is what Valkey is for in the import YAML, and why it is not decorative.
// The fix is a shared routing table (tunnel ID -> container ID) plus pub/sub to
// forward a request to the container that owns the socket. Store is the seam
// that swap goes through — the gateway only ever talks to this interface, so
// the Valkey implementation drops in without touching request handling.
package registry

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNotFound means no live tunnel answers to that ID.
var ErrNotFound = errors.New("registry: no such tunnel")

// ErrTaken means the requested subdomain is already serving another tunnel.
var ErrTaken = errors.New("registry: subdomain already in use")

// Tunnel is one live client connection.
type Tunnel struct {
	ID        string
	LocalPort int
	CreatedAt time.Time

	// requests is atomic because it is incremented on each request's own
	// goroutine (in the proxy's ModifyResponse) while /api/tunnels reads it
	// from another. The store's mutex guards the map, not the values inside
	// it, so a plain int64 here was a genuine data race.
	requests atomic.Int64

	// session is the multiplexed connection back to the developer's laptop.
	session any
}

// AddRequest counts one proxied request.
func (t *Tunnel) AddRequest() { t.requests.Add(1) }

// Requests reports how many requests have crossed this tunnel.
func (t *Tunnel) Requests() int64 { return t.requests.Load() }

// Session returns the underlying yamux session. The caller type-asserts it;
// keeping it as `any` here is what stops this package importing yamux.
func (t *Tunnel) Session() any { return t.session }

// Store is what the gateway depends on. The in-memory implementation below
// satisfies it; a Valkey-backed one will too.
type Store interface {
	Add(id string, localPort int, session any) (*Tunnel, error)
	Get(id string) (*Tunnel, error)
	Remove(id string)
	List() []*Tunnel
}

// Memory is a single-container Store.
type Memory struct {
	mu      sync.RWMutex
	tunnels map[string]*Tunnel
	rng     *rand.Rand
}

// NewMemory builds an empty in-memory store. seed is passed in rather than
// taken from the clock so tests can produce deterministic names.
func NewMemory(seed int64) *Memory {
	return &Memory{
		tunnels: make(map[string]*Tunnel),
		rng:     rand.New(rand.NewSource(seed)),
	}
}

// Add registers a tunnel. An empty id means "assign a random readable name".
func (m *Memory) Add(id string, localPort int, session any) (*Tunnel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if id == "" {
		var err error
		if id, err = m.freeNameLocked(); err != nil {
			return nil, err
		}
	} else if _, exists := m.tunnels[id]; exists {
		return nil, fmt.Errorf("%w: %s", ErrTaken, id)
	}

	t := &Tunnel{
		ID:        id,
		LocalPort: localPort,
		CreatedAt: time.Now().UTC(),
		session:   session,
	}
	m.tunnels[id] = t
	return t, nil
}

func (m *Memory) Get(id string) (*Tunnel, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tunnels[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return t, nil
}

func (m *Memory) Remove(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tunnels, id)
}

func (m *Memory) List() []*Tunnel {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Tunnel, 0, len(m.tunnels))
	for _, t := range m.tunnels {
		out = append(out, t)
	}
	return out
}

// Readable names beat random hex here. The tunnel ID ends up in a URL the
// developer reads aloud, types into a Stripe dashboard, and sends to a
// colleague — "quiet-frog" survives that trip and "a3f21c" does not.
var (
	adjectives = []string{
		"quiet", "brave", "sunny", "clever", "swift", "gentle", "bright",
		"calm", "eager", "kind", "lucky", "merry", "proud", "witty",
	}
	nouns = []string{
		"frog", "otter", "falcon", "cedar", "harbor", "lantern", "meadow",
		"pebble", "raven", "willow", "comet", "ember", "fjord", "moss",
	}
)

// freeNameLocked finds an unused adjective-noun pair. Callers hold m.mu.
func (m *Memory) freeNameLocked() (string, error) {
	// The pair space is len(adjectives)*len(nouns); a handful of attempts is
	// plenty at any realistic tunnel count, and the numeric suffix below is the
	// backstop rather than the main path.
	for attempt := 0; attempt < 24; attempt++ {
		name := fmt.Sprintf("%s-%s", adjectives[m.rng.Intn(len(adjectives))], nouns[m.rng.Intn(len(nouns))])
		if _, exists := m.tunnels[name]; !exists {
			return name, nil
		}
	}
	for suffix := 2; suffix < 1000; suffix++ {
		name := fmt.Sprintf("%s-%s-%d", adjectives[m.rng.Intn(len(adjectives))], nouns[m.rng.Intn(len(nouns))], suffix)
		if _, exists := m.tunnels[name]; !exists {
			return name, nil
		}
	}
	return "", errors.New("registry: exhausted name space")
}
