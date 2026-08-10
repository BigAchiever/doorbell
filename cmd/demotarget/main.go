// Command demotarget is the "laptop" sitting behind the public demo tunnel.
//
// Everything else in this repo can be verified with two curl commands against
// the gateway: an offline tunnel answers 202, an unknown one answers 404. What
// those cannot show is the other half — a request actually arriving somewhere.
// For that you need something on the far side of a tunnel, and until this
// existed the only way to see it was to install the CLI yourself.
//
// So this is the far side: a plain HTTP server that describes what reached it.
// It runs on a Zerops container next to the gateway, and the same published
// `doorbell` binary points a tunnel at it. Nothing here is special-cased in the
// gateway — to the gateway this is an ordinary client, indistinguishable from a
// dev server on someone's laptop.
//
// The useful part is the delivery block in the response. When a request was
// held while the tunnel was down, the gateway replays it with X-Doorbell-Replay
// and X-Doorbell-Original-Time set, and this echoes both back. That turns "your
// webhook waited four minutes and then arrived intact" from a claim in a README
// into something the caller can read in the response body.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

// maxEcho bounds how much of a request body comes back. Generous for a webhook
// payload, small enough that this can never be used to bounce a large body
// around at someone else's expense.
const maxEcho = 4 << 10

type response struct {
	OK       bool     `json:"ok"`
	What     string   `json:"what"`
	ServedBy string   `json:"servedBy"`
	ServedAt string   `json:"servedAt"`
	Request  reqInfo  `json:"request"`
	Delivery delivery `json:"delivery"`
}

type reqInfo struct {
	Method      string `json:"method"`
	Path        string `json:"path"`
	ContentType string `json:"contentType,omitempty"`
	UserAgent   string `json:"userAgent,omitempty"`
	Body        string `json:"body,omitempty"`
	Headers     int    `json:"headerCount"`
}

type delivery struct {
	Held       bool   `json:"held"`
	Explain    string `json:"explain"`
	HeldFor    string `json:"heldFor,omitempty"`
	ReceivedAt string `json:"gatewayReceivedAt,omitempty"`
	Tunnel     string `json:"tunnel,omitempty"`
}

func main() {
	log.SetFlags(0)

	port := os.Getenv("DEMO_PORT")
	if port == "" {
		port = "8080"
	}
	host, _ := os.Hostname()

	mux := http.NewServeMux()

	// Zerops probes this before routing anything at the container. It is
	// deliberately separate from the echo handler so a readiness probe never
	// shows up in the demo output as if it were a visitor's request.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, maxEcho))

		d := delivery{
			Explain: "Delivered live: the tunnel was connected when this request arrived.",
			Tunnel:  r.Header.Get("X-Doorbell-Tunnel"),
		}
		// Set by the gateway only on a replay, so its presence is the signal.
		if r.Header.Get("X-Doorbell-Replay") != "" {
			d.Held = true
			d.ReceivedAt = r.Header.Get("X-Doorbell-Original-Time")
			d.Explain = "Held: this request arrived while the tunnel was down, waited in Postgres, and was replayed on reconnect."
			if t, err := time.Parse(time.RFC3339, d.ReceivedAt); err == nil {
				d.HeldFor = time.Since(t).Round(time.Second).String()
			}
		}

		out := response{
			OK:       true,
			What:     "You reached a process on a Zerops container, through a Doorbell tunnel. Nothing between you and this line was HTTP-hosted — it came down a raw TCP socket the container opened outbound.",
			ServedBy: host,
			ServedAt: time.Now().UTC().Format(time.RFC3339),
			Request: reqInfo{
				Method:      r.Method,
				Path:        r.URL.Path,
				ContentType: r.Header.Get("Content-Type"),
				UserAgent:   r.Header.Get("User-Agent"),
				Body:        string(body),
				Headers:     len(r.Header),
			},
			Delivery: d,
		}

		// Only the headers above are echoed. Authorization, Cookie and anything
		// else a caller sends stays out of the response and out of the log: the
		// dashboard captures bodies already, and this should not become a second
		// place secrets pile up.
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			log.Printf("  ! encode: %v", err)
		}

		state := "live"
		if d.Held {
			state = "held " + d.HeldFor
		}
		log.Printf("  %s %-6s %-30s (%s)", time.Now().Format("15:04:05"), r.Method, r.URL.Path, state)
	})

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("demotarget: listening on :%s", port)
	log.Fatal(srv.ListenAndServe())
}
