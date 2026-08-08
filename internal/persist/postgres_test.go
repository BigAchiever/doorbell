package persist_test

// These are the only tests in the project that need a real database, and they
// cover the promises that cannot be checked any other way: that a request
// survives being stored, that a queue drains oldest first, and — the one that
// matters most — that two gateways draining the same mailbox at the same moment
// cannot both deliver the same webhook.
//
// They skip when DATABASE_URL is unset, so `go test ./...` still works on a
// laptop with no Postgres. CI sets it, so they run on every push.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/danishalisiddiqui/doorbell/internal/persist"
)

func openDB(t *testing.T) *persist.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping the tests that need Postgres")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := persist.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

// Each test works inside its own tunnel name so they never see each other's
// rows, however the suite is scheduled.
func tunnelName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
}

func TestStoredRequestSurvivesAndComesBackWhole(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	name := tunnelName(t)

	body := []byte(`{"event":"payment.succeeded","amount":4999}`)
	headers := map[string]string{"Content-Type": "application/json"}

	id, err := db.Enqueue(ctx, name, "POST", "/hooks/pay", "retry=1", headers, body)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := db.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.Body) != string(body) {
		t.Errorf("body round-trip: got %q, want %q", got.Body, body)
	}
	if got.Method != "POST" || got.Path != "/hooks/pay" || got.Query != "retry=1" {
		t.Errorf("request line round-trip: got %s %s?%s", got.Method, got.Path, got.Query)
	}
	if got.Headers["Content-Type"] != "application/json" {
		t.Errorf("headers round-trip: got %v", got.Headers)
	}
	if got.DeliveredAt != nil {
		t.Error("a freshly stored request must not be marked delivered")
	}
}

func TestQueueDrainsOldestFirst(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	name := tunnelName(t)

	// created, then updated, then deleted: the only order a handler can make
	// sense of, and the order the queue must preserve.
	for _, ev := range []string{"created", "updated", "deleted"} {
		if _, err := db.Enqueue(ctx, name, "POST", "/hook", "", nil, []byte(ev)); err != nil {
			t.Fatalf("enqueue %s: %v", ev, err)
		}
	}

	queued, err := db.Undelivered(ctx, name)
	if err != nil {
		t.Fatalf("undelivered: %v", err)
	}
	if len(queued) != 3 {
		t.Fatalf("expected 3 held requests, got %d", len(queued))
	}
	for i, want := range []string{"created", "updated", "deleted"} {
		if got := string(queued[i].Body); got != want {
			t.Errorf("position %d: got %q, want %q", i, got, want)
		}
	}
}

// The guarantee the product calls "delivered once". Two reconnects racing to
// drain the same mailbox must not both send the same webhook: exactly one
// Claim per row may win.
func TestOnlyOneClaimerWinsARow(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	name := tunnelName(t)

	id, err := db.Enqueue(ctx, name, "POST", "/hook", "", nil, []byte("once"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	const racers = 8
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		won  int
		errs []error
	)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release them together, so the claims genuinely overlap
			ok, err := db.Claim(ctx, id)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			if ok {
				won++
			}
		}()
	}
	close(start)
	wg.Wait()

	for _, err := range errs {
		t.Errorf("claim returned an error: %v", err)
	}
	if won != 1 {
		t.Fatalf("%d of %d claimers won the same row; every extra winner is a webhook delivered twice", won, racers)
	}
}

// A failed delivery must hand the row back rather than swallow it, so the next
// reconnect retries immediately instead of waiting out the lease.
func TestReleaseMakesARowClaimableAgain(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	name := tunnelName(t)

	id, err := db.Enqueue(ctx, name, "POST", "/hook", "", nil, []byte("retry me"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if won, err := db.Claim(ctx, id); err != nil || !won {
		t.Fatalf("first claim: won=%v err=%v", won, err)
	}
	if won, err := db.Claim(ctx, id); err != nil || won {
		t.Fatalf("second claim should lose while the lease is held: won=%v err=%v", won, err)
	}
	if err := db.Release(ctx, id); err != nil {
		t.Fatalf("release: %v", err)
	}
	if won, err := db.Claim(ctx, id); err != nil || !won {
		t.Fatalf("after release the row must be claimable again: won=%v err=%v", won, err)
	}
}

func TestDeliveredRequestsLeaveTheQueue(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	name := tunnelName(t)

	id, err := db.Enqueue(ctx, name, "POST", "/hook", "", nil, []byte("x"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if counts, err := db.PendingCount(ctx); err != nil {
		t.Fatalf("pending count: %v", err)
	} else if counts[name] != 1 {
		t.Fatalf("expected 1 pending for %s, got %d", name, counts[name])
	}

	if _, err := db.Claim(ctx, id); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := db.MarkDelivered(ctx, id, 200); err != nil {
		t.Fatalf("mark delivered: %v", err)
	}

	queued, err := db.Undelivered(ctx, name)
	if err != nil {
		t.Fatalf("undelivered: %v", err)
	}
	if len(queued) != 0 {
		t.Errorf("a delivered request must leave the queue, %d still held", len(queued))
	}

	// It stays readable, though — replay depends on the stored copy outliving
	// its delivery.
	got, err := db.Get(ctx, id)
	if err != nil {
		t.Fatalf("get after delivery: %v", err)
	}
	if got.DeliveredAt == nil {
		t.Error("delivered_at was not recorded")
	}
	if got.LastStatus == nil || *got.LastStatus != 200 {
		t.Errorf("last status: got %v, want 200", got.LastStatus)
	}
}

// Reservations are what make a tunnel URL stable across restarts, and what stop
// a stranger taking a name someone else is using.
func TestAReservedNameCannotBeTakenByAnotherToken(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	name := tunnelName(t)

	if err := db.ClaimName(ctx, name, "token-a"); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := db.ClaimName(ctx, name, "token-a"); err != nil {
		t.Fatalf("the same owner must be able to reclaim its own name: %v", err)
	}
	if err := db.ClaimName(ctx, name, "token-b"); err == nil {
		t.Fatal("a different token claimed a reserved name; URLs would not be stable")
	}

	reserved, err := db.IsReserved(ctx, name)
	if err != nil {
		t.Fatalf("is reserved: %v", err)
	}
	if !reserved {
		t.Error("a claimed name must report as reserved — only reserved names are held")
	}
}

func TestTimelineWindowExcludesOlderRows(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	name := tunnelName(t)

	if _, err := db.Enqueue(ctx, name, "POST", "/hook", "", nil, []byte("now")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	recent, err := db.Since(ctx, time.Now().Add(-time.Minute), 0)
	if err != nil {
		t.Fatalf("since: %v", err)
	}
	if !containsTunnel(recent, name) {
		t.Error("a request stored a moment ago is missing from the last minute")
	}

	future, err := db.Since(ctx, time.Now().Add(time.Minute), 0)
	if err != nil {
		t.Fatalf("since (future): %v", err)
	}
	if containsTunnel(future, name) {
		t.Error("a request appeared in a window that starts after it was stored")
	}
}

func containsTunnel(rows []persist.PendingRequest, name string) bool {
	for _, r := range rows {
		if r.TunnelID == name {
			return true
		}
	}
	return false
}
