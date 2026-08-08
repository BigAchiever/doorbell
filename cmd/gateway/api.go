package main

// JSON and HTML endpoints the dashboard is built on.

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/BigAchiever/Doorbell/internal/dashboard"
)

func (g *gateway) handleListTunnels(w http.ResponseWriter, _ *http.Request) {
	type row struct {
		ID        string    `json:"id"`
		LocalPort int       `json:"localPort"`
		CreatedAt time.Time `json:"createdAt"`
		Requests  int64     `json:"requests"`
		URL       string    `json:"url"`
	}
	list := g.store.List()
	out := make([]row, 0, len(list))
	for _, t := range list {
		out = append(out, row{t.ID, t.LocalPort, t.CreatedAt, t.Requests(), g.urlFor(t.ID)})
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		log.Printf("api: encode tunnels: %v", err)
	}
}

// handleIndex serves the landing page. Someone arriving at the gateway URL for
// the first time needs to be told what this is before being shown a table of
// HTTP requests, so the inspector lives at /dashboard instead of "/".
func (g *gateway) handleIndex(w http.ResponseWriter, r *http.Request) {
	// "/" is the only path that should render the landing page; anything else
	// reaching this catch-all is a genuine 404, not a silent fallback.
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeHTML(w, dashboard.Landing)
}

func (g *gateway) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, dashboard.HTML)
}

func writeHTML(w http.ResponseWriter, page []byte) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if _, err := w.Write(page); err != nil {
		log.Printf("dashboard: write: %v", err)
	}
}

// handleInfo backs the live counters on the landing page.
func (g *gateway) handleInfo(w http.ResponseWriter, r *http.Request) {
	info := map[string]any{
		"mode":        g.cfg.mode,
		"tunnels":     len(g.store.List()),
		"controlPort": g.cfg.controlPort,
		"persistence": g.db != nil,
		"routing":     g.route != nil,
	}
	if g.db != nil {
		if counts, err := g.db.PendingCount(r.Context()); err == nil {
			held := 0
			for _, n := range counts {
				held += n
			}
			info["held"] = held
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(info); err != nil {
		log.Printf("api: encode info: %v", err)
	}
}

func (g *gateway) handleListRequests(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(g.rec.Recent(200)); err != nil {
		log.Printf("api: encode requests: %v", err)
	}
}

// handleStream pushes live records to the dashboard over Server-Sent Events.
//
// THE HEARTBEAT IS LOad-BEARING, not politeness. Zerops' L7 balancer defaults
// send_timeout to 2 seconds — the maximum gap allowed between successive writes
// to a client. An idle SSE stream writes nothing, so the balancer would close
// the connection two seconds after the last request and the dashboard would
// flap between "live" and "reconnecting" forever. A comment frame every second
// keeps the stream under that ceiling without emitting anything the client has
// to parse.
func (g *gateway) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Belt and braces: tells any intermediate nginx not to buffer this response.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Send backlog first so a dashboard opened mid-session is not blank.
	hello, err := json.Marshal(map[string]any{
		"mode":   g.cfg.mode,
		"recent": g.rec.Recent(100),
	})
	if err != nil {
		log.Printf("stream: marshal hello: %v", err)
		return
	}
	fmt.Fprintf(w, "event: hello\ndata: %s\n\n", hello)
	flusher.Flush()

	events, release := g.rec.Subscribe()
	defer release()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case rec, ok := <-events:
			if !ok {
				return
			}
			payload, err := json.Marshal(rec)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: request\ndata: %s\n\n", payload)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

// rateLimit caps how fast any one client can drive the public ingress.
//
// /healthz is exempt: it is called by the platform's own health checker, and
// throttling it would make Zerops believe the service is unhealthy and restart

func (g *gateway) handleHistory(w http.ResponseWriter, r *http.Request) {
	if g.db == nil {
		http.Error(w, `{"error":"history unavailable: no database configured"}`, http.StatusServiceUnavailable)
		return
	}
	rows, err := g.db.History(r.Context(), 200)
	if err != nil {
		log.Printf("api: history: %v", err)
		http.Error(w, `{"error":"history query failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(rows); err != nil {
		log.Printf("api: encode history: %v", err)
	}
}
