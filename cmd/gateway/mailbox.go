package main

// The mailbox is what makes Doorbell more than a tunnel.
//
// Every tunnel ever built — ngrok, frp, bore, chisel, Cloudflare Tunnel —
// drops requests on the floor when your laptop is not there. You close the lid,
// Stripe fires a webhook, it gets a 502, and you go and re-send it by hand from
// their dashboard.
//
// Doorbell catches it instead. If a request arrives for a RESERVED name with no
// live session, it is stored and the caller gets 202 Accepted. When the tunnel
// comes back the queue drains into it, in arrival order.
//
// The honest trade, which belongs in the pitch and not in the small print:
// answering 202 means "I have taken responsibility for this", not "your app
// processed it". For a DEVELOPMENT tunnel that is exactly the behaviour you
// want. For a production gateway it would be wrong, and Doorbell is not one.
//
// Two guards keep this from being an abuse surface:
//   - buffering happens only for names someone has reserved, so a stranger
//     cannot allocate storage by guessing URLs;
//   - each mailbox is capped, oldest dropped first.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/danishalisiddiqui/doorbell/internal/inspect"
	"github.com/danishalisiddiqui/doorbell/internal/persist"
)

// bufferRequest stores a request for a tunnel that is not currently connected.
// It reports whether it handled the response.
func (g *gateway) bufferRequest(w http.ResponseWriter, r *http.Request, id, rest string) bool {
	if g.db == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	reserved, err := g.db.IsReserved(ctx, id)
	if err != nil {
		log.Printf("mailbox: reservation check %s: %v", id, err)
		return false
	}
	if !reserved {
		// Unknown name. A 404 is the honest answer; buffering here would let
		// anyone fill the table with requests for tunnels that never existed.
		return false
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, inspect.MaxBodyCapture+1))
	if err != nil {
		return false
	}

	pendingID, err := g.db.Enqueue(ctx, id, r.Method, rest, r.URL.RawQuery, inspect.FlattenHeader(r.Header), body)
	if err != nil {
		log.Printf("mailbox: enqueue %s: %v", id, err)
		return false
	}

	log.Printf("mailbox: HELD %s %s for %q (id=%d) — tunnel offline", r.Method, rest, id, pendingID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"doorbell":  "buffered",
		"id":        pendingID,
		"tunnel":    id,
		"message":   "the tunnel is offline; this request is held and will be delivered when it reconnects",
		"replayUrl": fmt.Sprintf("%s/api/replay/%d", g.cfg.publicBase, pendingID),
	}); err != nil {
		log.Printf("mailbox: encode 202: %v", err)
	}
	return true
}

// drainMailbox delivers everything held for a tunnel that just reconnected.
//
// Delivery is strictly sequential. Webhook streams are usually causal —
// created, then updated, then deleted — and replaying them concurrently can
// leave an app in a state the real sequence would never have produced.
func (g *gateway) drainMailbox(id string, session *yamux.Session) {
	if g.db == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queued, err := g.db.Undelivered(ctx, id)
	if err != nil {
		log.Printf("mailbox: read queue %s: %v", id, err)
		return
	}
	if len(queued) == 0 {
		return
	}
	log.Printf("mailbox: draining %d held request(s) into %s", len(queued), id)

	for _, p := range queued {
		if session.IsClosed() {
			log.Printf("mailbox: %s went away mid-drain, %d still held", id, len(queued))
			return
		}
		status, err := g.deliver(ctx, session, p, "buffered")
		if err != nil {
			// Leave it undelivered so the next reconnect tries again. This is
			// the whole promise of the feature — do not silently discard.
			log.Printf("mailbox: deliver %d to %s failed, still held: %v", p.ID, id, err)
			return
		}
		if err := g.db.MarkDelivered(ctx, p.ID, status); err != nil {
			log.Printf("mailbox: mark delivered %d: %v", p.ID, err)
		}
	}
}

// deliver pushes one stored request through a live session and records it.
//
// Unlike a live proxy hop there is no downstream client waiting, so this uses a
// plain http.Client over a yamux-dialling transport rather than ReverseProxy.
func (g *gateway) deliver(ctx context.Context, session *yamux.Session, p persist.PendingRequest, source string) (int, error) {
	target := "http://doorbell.local" + p.Path
	if p.Query != "" {
		target += "?" + p.Query
	}

	req, err := http.NewRequestWithContext(ctx, p.Method, target, bytes.NewReader(p.Body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	for k, v := range p.Headers {
		// Redacted placeholders must not be replayed as if they were real
		// credentials — the app would reject them confusingly.
		if strings.Contains(v, "redacted") {
			continue
		}
		req.Header.Set(k, v)
	}
	req.Header.Set("X-Doorbell-Replay", source)
	req.Header.Set("X-Doorbell-Original-Time", p.ReceivedAt.UTC().Format(time.RFC3339))
	req.ContentLength = int64(len(p.Body))

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return session.Open()
			},
			ResponseHeaderTimeout: 60 * time.Second,
		},
		Timeout: 90 * time.Second,
	}

	started := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("deliver: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, inspect.MaxBodyCapture+1))
	reqBody, reqCut := inspect.Truncate(p.Body)
	resText, resCut := inspect.Truncate(respBody)

	// Surface it in the inspector exactly like live traffic, tagged so it is
	// obvious this was not someone hitting the URL just now.
	g.finishRecord(&inspect.Record{
		TunnelID: p.TunnelID,
		At:       started.UTC(),
		Method:   p.Method,
		Path:     p.Path + "  ⟲ " + source,
		Query:    p.Query,
		ReqHead:  p.Headers,
		ReqBody:  reqBody,
		ReqCut:   reqCut,
		Status:   resp.StatusCode,
		ResHead:  inspect.FlattenHeader(resp.Header),
		ResBody:  resText,
		ResCut:   resCut,
		DurMs:    time.Since(started).Milliseconds(),
	})
	return resp.StatusCode, nil
}

// handleReplay re-sends a stored request into whichever container currently
// holds the tunnel. POST /api/replay/{id}
func (g *gateway) handleReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"POST only"}`, http.StatusMethodNotAllowed)
		return
	}
	if g.db == nil {
		http.Error(w, `{"error":"replay needs a database"}`, http.StatusServiceUnavailable)
		return
	}

	raw := strings.TrimPrefix(r.URL.Path, "/api/replay/")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		http.Error(w, `{"error":"bad request id"}`, http.StatusBadRequest)
		return
	}

	p, err := g.db.Get(r.Context(), id)
	if err != nil {
		http.Error(w, `{"error":"no such stored request"}`, http.StatusNotFound)
		return
	}

	t, err := g.store.Get(p.TunnelID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"tunnel %q is not connected here"}`, p.TunnelID), http.StatusConflict)
		return
	}
	session, ok := t.Session().(*yamux.Session)
	if !ok || session.IsClosed() {
		http.Error(w, `{"error":"tunnel session is closed"}`, http.StatusConflict)
		return
	}

	status, err := g.deliver(r.Context(), session, *p, "replay")
	if err != nil {
		log.Printf("replay %d: %v", id, err)
		http.Error(w, `{"error":"replay failed"}`, http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"replayed": id, "status": status}); err != nil {
		log.Printf("api: encode replay: %v", err)
	}
}

// handleMailbox lists stored requests. GET /api/mailbox?tunnel=<id>
func (g *gateway) handleMailbox(w http.ResponseWriter, r *http.Request) {
	if g.db == nil {
		http.Error(w, `{"error":"mailbox needs a database"}`, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	if id := r.URL.Query().Get("tunnel"); id != "" {
		list, err := g.db.Mailbox(r.Context(), id, 50)
		if err != nil {
			http.Error(w, `{"error":"mailbox query failed"}`, http.StatusInternalServerError)
			return
		}
		if err := json.NewEncoder(w).Encode(list); err != nil {
			log.Printf("api: encode mailbox: %v", err)
		}
		return
	}

	counts, err := g.db.PendingCount(r.Context())
	if err != nil {
		http.Error(w, `{"error":"pending count failed"}`, http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(counts); err != nil {
		log.Printf("api: encode pending: %v", err)
	}
}
