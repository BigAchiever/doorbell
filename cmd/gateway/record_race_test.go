package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A published record must never be written to again.
//
// The dashboard marshals records straight off the subscriber channel, and the
// Valkey and Postgres writers take them into their own goroutines — none of
// them holds the proxy's recMu. So anything still mutating a record after it
// has been handed over is a live data race, and this test is the thing that
// says so. It only fails under -race, which CI runs.
//
// The shape that used to break it: an origin that answers before the request
// body has been drained. The response completes, the record is published, and
// the request-body capture callback then writes ReqBody into the same struct a
// subscriber is reading.
func TestPublishedRecordIsNotWrittenAgain(t *testing.T) {
	origin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Deliberately does not read r.Body.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	g, id := newTestTunnel(t, origin)

	events, release := g.rec.Subscribe()
	defer release()

	marshalled := make(chan struct{}, 16)
	go func() {
		for rec := range events {
			// Exactly what handleStream does with it.
			if _, err := json.Marshal(rec); err != nil {
				continue
			}
			select {
			case marshalled <- struct{}{}:
			default:
			}
		}
	}()

	req := httptest.NewRequest(http.MethodPost, "/t/"+id+"/x", io.NopCloser(endlessDribble{}))
	rw := httptest.NewRecorder()
	g.handleTunnelRequest(rw, req)

	select {
	case <-marshalled:
	case <-time.After(5 * time.Second):
		t.Fatal("no record reached the subscriber; the test is no longer exercising the publish path")
	}

	// Give the still-running request-body capture time to write into whatever
	// it holds. If that is the same struct the subscriber marshalled, -race
	// reports it here.
	time.Sleep(300 * time.Millisecond)

	if got := rw.Result().StatusCode; got != http.StatusOK {
		t.Fatalf("status = %d, want %d", got, http.StatusOK)
	}
	if n := len(g.rec.Recent(10)); n == 0 {
		t.Fatal("recorder holds no records, so nothing was published")
	}
}

// endlessDribble hands back a few bytes at a time and never reaches EOF, so the
// request body is guaranteed to still be mid-Read when the response finishes
// and the handler closes it. A reader that ends on its own would let the
// capture settle first and the test would pass against the buggy code.
type endlessDribble struct{}

func (endlessDribble) Read(p []byte) (int, error) {
	time.Sleep(time.Millisecond)
	n := min(len(p), 512)
	for i := range n {
		p[i] = 'a'
	}
	return n, nil
}
