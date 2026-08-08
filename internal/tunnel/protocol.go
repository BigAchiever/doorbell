// Package tunnel defines the wire protocol spoken between the doorbell CLI and
// the gateway over the raw TCP control port.
//
// THE SHAPE, and why it is this shape:
//
//  1. The CLI dials the gateway's raw TCP port and sends one JSON line — Hello.
//  2. The gateway replies with one JSON line — Welcome — carrying the public URL.
//  3. Both sides then hand the SAME socket to yamux and stop speaking JSON.
//     From here the connection is a multiplexed session: the gateway opens one
//     stream per inbound HTTP request, and raw bytes are piped both ways.
//
// Why multiplexing at all: without it every inbound request would need its own
// TCP connection back to the laptop, which is impossible — the laptop is behind
// NAT and cannot be dialled. The whole point is that the CLI dials OUT once and
// the gateway reuses that one socket for everything afterwards.
//
// Why the gateway is the yamux CLIENT and the CLI is the yamux SERVER: streams
// are always opened by the gateway (a request arrived) and accepted by the CLI.
// yamux allows either side to open, but fixing the direction keeps stream-ID
// allocation unambiguous and makes the code easier to reason about.
//
// This handshake is deliberately line-delimited JSON rather than a binary frame
// format: it is debuggable with netcat, which mattered a great deal while
// proving the raw TCP port worked at all.
package tunnel

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ProtocolVersion is bumped whenever Hello or Welcome change shape. The gateway
// refuses a CLI it cannot speak to, with a message telling the user to upgrade,
// rather than failing somewhere deep in the stream layer.
const ProtocolVersion = 1

// MaxHandshakeBytes caps how much a peer can send before completing the
// handshake. Without this, anyone who finds the open control port could hold a
// connection and stream unbounded data into the gateway's memory.
const MaxHandshakeBytes = 4 << 10 // 4 KiB

// Hello is the CLI's opening message.
type Hello struct {
	Version int `json:"version"`
	// Token authenticates the CLI against this gateway. A gateway with no
	// configured token accepts any value — fine for a laptop, wrong for
	// anything public, and the gateway logs a warning when it happens.
	Token string `json:"token"`
	// Subdomain is an optional request for a stable name. Empty means "assign
	// me a random one". A stable name is the single most requested thing
	// missing from ngrok's free tier, so it is in the protocol from day one.
	Subdomain string `json:"subdomain,omitempty"`
	// LocalPort is informational — it lets the dashboard show what the
	// developer is actually exposing. The gateway never dials it; only the CLI
	// can reach localhost.
	LocalPort int `json:"localPort,omitempty"`
	// ClientVersion aids support when someone reports a bug from an old build.
	ClientVersion string `json:"clientVersion,omitempty"`
}

// Welcome is the gateway's reply. A non-empty Error means the tunnel was
// refused and the connection is about to close.
type Welcome struct {
	Version int    `json:"version"`
	Error   string `json:"error,omitempty"`

	// ID is the tunnel's assigned name, e.g. "quiet-frog".
	ID string `json:"id,omitempty"`
	// URL is what the developer registers with the sender. Its shape depends on the
	// gateway's mode: path-based in zeroconfig, subdomain-based with a domain.
	URL string `json:"url,omitempty"`
	// Mode is "zeroconfig" or "wildcard", so the CLI can warn about the
	// path-rewriting caveat only when it actually applies.
	Mode string `json:"mode,omitempty"`
}

// ErrHandshakeTooLarge is returned when a peer exceeds MaxHandshakeBytes.
var ErrHandshakeTooLarge = errors.New("tunnel: handshake exceeded size limit")

// WriteJSONLine writes v as a single newline-terminated JSON object.
func WriteJSONLine(w io.Writer, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("tunnel: marshal handshake: %w", err)
	}
	if _, err := w.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("tunnel: write handshake: %w", err)
	}
	return nil
}

// ReadJSONLine reads one newline-terminated JSON object into v.
//
// It takes a *bufio.Reader rather than an io.Reader on purpose. The same socket
// is handed to yamux immediately afterwards, and a reader that over-read past
// the newline would swallow the first bytes of the yamux session — a bug that
// presents as an inexplicable hang on the first request. Callers keep using
// this one buffered reader, and hand its remaining buffer to yamux.
func ReadJSONLine(r *bufio.Reader, v any) error {
	line, err := readLimitedLine(r, MaxHandshakeBytes)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(line, v); err != nil {
		return fmt.Errorf("tunnel: malformed handshake %q: %w", truncate(line, 120), err)
	}
	return nil
}

func readLimitedLine(r *bufio.Reader, limit int) ([]byte, error) {
	var buf []byte
	for {
		chunk, isPrefix, err := r.ReadLine()
		if err != nil {
			return nil, fmt.Errorf("tunnel: read handshake: %w", err)
		}
		buf = append(buf, chunk...)
		if len(buf) > limit {
			return nil, ErrHandshakeTooLarge
		}
		if !isPrefix {
			return buf, nil
		}
	}
}

func truncate(b []byte, n int) string {
	s := strings.TrimSpace(string(b))
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
