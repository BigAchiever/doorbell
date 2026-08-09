package main

// The mailbox is what makes Doorbell more than a tunnel.
//
// Every tunnel ever built — ngrok, frp, bore, chisel, Cloudflare Tunnel —
// drops requests on the floor when your laptop is not there. You close the lid;
// the payment provider, the git host, the CI job reporting back all get a 502,
// and you go and re-send each one by hand from whatever console it came from —
// assuming that sender kept it at all.
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

	"github.com/BigAchiever/doorbell/internal/inspect"
	"github.com/BigAchiever/doorbell/internal/persist"
)

// bufferRequest stores a request for a tunnel that is not currently connected.
// It reports whether it handled the response.
func (g *gateway) bufferRequest(w http.ResponseWriter, r *http.Request, id, rest string) bool {
	if g.db == nil {
		return false
	}

	// Deliberately NOT r.Context(). The caller is a webhook sender with its own
	// short timeout; if it gives up while Postgres is slow, cancelling the
	// INSERT would drop the very request this feature exists to save. Once
	// Doorbell has decided to take responsibility, the write must outlive the
	// connection that delivered it.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 15*time.Second)
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

	// Read up to one byte past the limit so an oversized body is detectable.
	//
	// The mailbox must store bodies WHOLE. Clipping to the inspector's 32 KiB
	// display limit and delivering the prefix later would hand the app a
	// truncated payload with a matching Content-Length — syntactically invalid
	// JSON the sender never produced, and no way for anyone to tell. If it does
	// not fit, say so instead of corrupting it.
	body, err := io.ReadAll(io.LimitReader(r.Body, persist.MaxStoredBody+1))
	if err != nil {
		return false
	}
	if len(body) > persist.MaxStoredBody {
		log.Printf("mailbox: REFUSED %s %s for %q — body over %d bytes", r.Method, rest, id, persist.MaxStoredBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"doorbell": "not-buffered",
			"reason":   "body exceeds the mailbox limit, and storing a truncated copy would corrupt it",
			"limit":    persist.MaxStoredBody,
		}); err != nil {
			log.Printf("mailbox: encode 413: %v", err)
		}
		return true
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
	// One client for the whole drain. Building an http.Transport per request
	// allocated a fresh connection pool and its goroutines 200 times over.
	client := sessionClient(session)
	defer client.CloseIdleConnections()

	// Rounds, not one pass. While this runs the tunnel is flagged draining, so
	// anything arriving now is written to the mailbox rather than proxied past
	// the queue — which means the queue can grow while we work through it. One
	// pass would leave those sitting until the next reconnect.
	//
	// Bounded, because a tunnel under continuous load would otherwise never stop
	// draining and every request would take the long way round forever. Past the
	// bound the flag clears and traffic goes live again, which loses ordering
	// only for a tunnel that was never idle in the first place.
	const maxRounds = 20
	for round := 0; round < maxRounds; round++ {
		readCtx, cancelRead := context.WithTimeout(context.Background(), 15*time.Second)
		queued, err := g.db.Undelivered(readCtx, id)
		cancelRead()
		if err != nil {
			log.Printf("mailbox: read queue %s: %v", id, err)
			return
		}
		if len(queued) == 0 {
			return
		}
		log.Printf("mailbox: draining %d held request(s) into %s", len(queued), id)
		if !g.drainBatch(id, session, client, queued) {
			return
		}
	}
	log.Printf("mailbox: %s still receiving faster than it drains, going live", id)
}

// drainBatch delivers one read of the queue in order, reporting whether the
// caller should look for more. A false means stop: the session went away, the
// database refused, or another drain took over.
func (g *gateway) drainBatch(id string, session *yamux.Session, client *http.Client, queued []persist.PendingRequest) bool {
	delivered := 0
	for _, p := range queued {
		if session.IsClosed() {
			// Report what is actually left, not the size of the batch we
			// started with — the old message said "200 still held" after
			// successfully delivering 199 of them.
			log.Printf("mailbox: %s went away mid-drain, %d of %d still held", id, len(queued)-delivered, len(queued))
			return false
		}

		// Take the lease before sending. Without it, a second drain for the
		// same tunnel reads these same rows and delivers every webhook twice.
		claimCtx, cancelClaim := context.WithTimeout(context.Background(), 10*time.Second)
		won, err := g.db.Claim(claimCtx, p.ID)
		cancelClaim()
		if err != nil {
			log.Printf("mailbox: claim %d: %v", p.ID, err)
			return false
		}
		if !won {
			// Another drain owns this row. Skipping to the next one would
			// deliver a newer request while an older one is still in flight
			// elsewhere, so stop here and let that drain finish the queue.
			log.Printf("mailbox: %s row %d claimed by another drain, yielding the queue", id, p.ID)
			return false
		}
		status, err := func() (int, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			return g.deliver(ctx, session, client, p, "buffered")
		}()
		if err != nil {
			// Hand the lease back so the next reconnect retries immediately
			// rather than waiting it out. Never discard: that is the promise.
			relCtx, cancelRel := context.WithTimeout(context.Background(), 10*time.Second)
			if rerr := g.db.Release(relCtx, p.ID); rerr != nil {
				log.Printf("mailbox: release %d: %v", p.ID, rerr)
			}
			cancelRel()
			log.Printf("mailbox: deliver %d to %s failed, still held: %v", p.ID, id, err)
			return false
		}
		delivered++
		// Between here and the row being marked, the request has been delivered
		// but the database does not know it. Claim only tests delivered_at and
		// the lease age, so if this never lands the row becomes claimable again
		// once the lease expires and the laptop receives it a second time.
		//
		// One attempt was not enough for that: the common failure is a momentary
		// database blip, and a retry clears it. What survives a retry is
		// genuinely at-least-once, which is why the delivery carries a stable
		// X-Doorbell-Id for the receiver to dedupe on.
		if err := g.markDelivered(p.ID, status); err != nil {
			log.Printf("mailbox: mark delivered %d: %v — it may be delivered again after the lease expires", p.ID, err)
		}
	}
	return true
}

// markDelivered records a delivery, retrying a few times before giving up.
//
// Failing here never loses the request — it repeats it, which is the safer of
// the two directions, but a repeat is still a webhook the app has already
// processed. Three quick attempts cover the transient case without holding the
// drain loop open long enough to matter.
func (g *gateway) markDelivered(id int64, status int) error {
	var err error
	for attempt := range 3 {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 250 * time.Millisecond)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = g.db.MarkDelivered(ctx, id, status)
		cancel()
		if err == nil {
			return nil
		}
	}
	return err
}

// deliver pushes one stored request through a live session and records it.
//
// Unlike a live proxy hop there is no downstream client waiting, so this uses a
// plain http.Client over a yamux-dialling transport rather than ReverseProxy.
func (g *gateway) deliver(ctx context.Context, session *yamux.Session, client *http.Client, p persist.PendingRequest, source string) (int, error) {
	target := "http://doorbell.local" + p.Path
	if p.Query != "" {
		target += "?" + p.Query
	}

	req, err := http.NewRequestWithContext(ctx, p.Method, target, bytes.NewReader(p.Body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	for k, v := range p.Headers {
		// Skip by header NAME, using the same judgement that redacted it in the
		// first place. Matching on the value instead both dropped legitimate
		// headers that happened to contain the word and would have silently
		// started replaying the placeholder text as a real credential if that
		// string ever changed.
		if inspect.IsSensitive(k) {
			continue
		}
		// Hop-by-hop headers describe the connection that delivered the
		// original request, not this one; replaying them confuses the client.
		if isHopByHop(k) {
			continue
		}
		req.Header.Set(k, v)
	}
	req.Header.Set("X-Doorbell-Replay", source)
	req.Header.Set("X-Doorbell-Original-Time", p.ReceivedAt.UTC().Format(time.RFC3339))
	// Stable for the life of the stored row, and the same on every attempt at
	// it. Delivery is at-least-once once the database is allowed to fail, so
	// this is what lets a receiver tell a repeat from a new request.
	req.Header.Set("X-Doorbell-Id", strconv.FormatInt(p.ID, 10))
	req.ContentLength = int64(len(p.Body))

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
		TunnelID:  p.TunnelID,
		At:        started.UTC(),
		Method:    p.Method,
		Path:      p.Path,
		Source:    source,
		PendingID: p.ID,
		Query:     p.Query,
		ReqHead:   p.Headers,
		ReqBody:   reqBody,
		ReqCut:    reqCut,
		Status:    resp.StatusCode,
		ResHead:   inspect.FlattenHeader(resp.Header),
		ResBody:   resText,
		ResCut:    resCut,
		DurMs:     time.Since(started).Milliseconds(),
	})
	return resp.StatusCode, nil
}

// sessionClient builds an HTTP client whose connections are yamux streams on
// one tunnel session. Reuse it across a drain rather than per request.
func sessionClient(session *yamux.Session) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return session.Open()
			},
			ResponseHeaderTimeout: 60 * time.Second,
		},
		Timeout: 90 * time.Second,
	}
}

// isHopByHop reports headers that describe a single connection and must not be
// forwarded or replayed. RFC 9110 section 7.6.1.
func isHopByHop(key string) bool {
	switch http.CanonicalHeaderKey(key) {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Connection",
		"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Content-Length", "Host":
		return true
	}
	return false
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

	client := sessionClient(session)
	defer client.CloseIdleConnections()

	status, err := g.deliver(r.Context(), session, client, *p, "replay")
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
// handleTimeline is the public half of the mailbox: when each tunnel was
// offline and how much was held, and nothing else.
//
// The overview page draws the same tape as the dashboard, so a visitor sees the
// held requests before reading a word — which means this cannot sit behind the
// operator token. What it returns is deliberately thin: a tunnel name, two
// timestamps, a count. No paths, no headers, no bodies, no sizes. The tunnel
// names are already public, since they are the URL a caller posts to.
func (g *gateway) handleTimeline(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if g.db == nil {
		// No database means no mailbox, which is a flat tape rather than an error.
		_, _ = w.Write([]byte("[]"))
		return
	}

	mins := 30
	if v := r.URL.Query().Get("minutes"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			http.Error(w, `{"error":"minutes must be a positive number"}`, http.StatusBadRequest)
			return
		}
		mins = min(n, 360)
	}

	list, err := g.db.Since(r.Context(), time.Now().Add(-time.Duration(mins)*time.Minute), 0)
	if err != nil {
		http.Error(w, `{"error":"timeline query failed"}`, http.StatusInternalServerError)
		return
	}

	type span struct {
		TunnelID    string     `json:"tunnelId"`
		ReceivedAt  time.Time  `json:"receivedAt"`
		DeliveredAt *time.Time `json:"deliveredAt,omitempty"`
	}
	out := make([]span, 0, len(list))
	for _, p := range list {
		out = append(out, span{TunnelID: p.TunnelID, ReceivedAt: p.ReceivedAt, DeliveredAt: p.DeliveredAt})
	}
	if err := json.NewEncoder(w).Encode(out); err != nil {
		log.Printf("api: encode public timeline: %v", err)
	}
}

func (g *gateway) handleMailbox(w http.ResponseWriter, r *http.Request) {
	if g.db == nil {
		http.Error(w, `{"error":"mailbox needs a database"}`, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	// ?since=<minutes> feeds the dashboard timeline: every stored request in the
	// window, across all tunnels, so the client can reconstruct which stretches a
	// tunnel was offline. Bounded at 6 hours because the tape only ever draws a
	// window of that order, and an unbounded value here is a free table scan for
	// anyone holding the operator token.
	if v := r.URL.Query().Get("since"); v != "" {
		mins, err := strconv.Atoi(v)
		if err != nil || mins <= 0 {
			http.Error(w, `{"error":"since must be a positive number of minutes"}`, http.StatusBadRequest)
			return
		}
		if mins > 360 {
			mins = 360
		}
		list, err := g.db.Since(r.Context(), time.Now().Add(-time.Duration(mins)*time.Minute), 0)
		if err != nil {
			http.Error(w, `{"error":"timeline query failed"}`, http.StatusInternalServerError)
			return
		}
		if list == nil {
			list = []persist.PendingRequest{}
		}
		if err := json.NewEncoder(w).Encode(list); err != nil {
			log.Printf("api: encode timeline: %v", err)
		}
		return
	}

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
