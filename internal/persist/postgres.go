// Package persist holds the state that must outlive a gateway restart.
//
// Almost nothing in Doorbell needs a database. Live tunnels are, by definition,
// live — when the process dies every socket dies with it, so persisting them
// would only produce lies. Two things genuinely do need to survive:
//
//  1. RESERVED NAMES. "Your URL changes every time you restart" is the single
//     most-cited complaint about ngrok's free tier. Fixing it means a claim on
//     a name has to outlive the process holding it, which is a database.
//  2. REQUEST HISTORY. A webhook that failed at 3am is worth more than one that
//     succeeded now, and the in-memory ring buffer forgets on deploy.
//
// Everything here degrades: if Postgres is unreachable the gateway logs it once
// and runs with random names and in-memory history. A tunnel is more useful
// than a database.
package persist

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNameHeld means the name is reserved by a different token.
var ErrNameHeld = errors.New("persist: name is reserved by someone else")

type DB struct{ pool *pgxpool.Pool }

// Open connects and applies the schema. The caller decides what to do on
// failure; the gateway treats it as non-fatal.
func Open(ctx context.Context, dsn string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("persist: bad DSN: %w", err)
	}
	// A gateway holding hundreds of tunnels still touches the database rarely —
	// on claim and on request write. A small pool is right, and keeps headroom
	// on a 0.25 GB Postgres container.
	cfg.MaxConns = 8
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("persist: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("persist: ping: %w", err)
	}

	db := &DB{pool: pool}
	if err := db.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return db, nil
}

func (d *DB) Close() { d.pool.Close() }

// migrate is idempotent so every container can run it at boot without
// coordination — there is no separate migration step to forget.
func (d *DB) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS reservations (
  name        TEXT PRIMARY KEY,
  owner_hash  TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS requests (
  id          BIGSERIAL PRIMARY KEY,
  tunnel_id   TEXT NOT NULL,
  at          TIMESTAMPTZ NOT NULL,
  method      TEXT NOT NULL,
  path        TEXT NOT NULL,
  status      INT  NOT NULL,
  dur_ms      BIGINT NOT NULL,
  err         TEXT
);
CREATE INDEX IF NOT EXISTS requests_at_idx ON requests (at DESC);

-- The mailbox. Every tunnel that has anything replayable owns rows here.
--
-- delivered_at IS NULL means "arrived while nobody was home" — the caller got
-- a 202 and this is waiting for the tunnel to come back. A non-null value
-- means it has been through the tunnel at least once and now exists purely so
-- it can be replayed.
--
-- Bodies ARE stored here, unlike in the requests table. That is the whole
-- point: you cannot replay a request you did not keep. It is also why
-- retention is capped per tunnel rather than left to grow.
CREATE TABLE IF NOT EXISTS pending (
  id           BIGSERIAL PRIMARY KEY,
  tunnel_id    TEXT NOT NULL,
  received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  method       TEXT NOT NULL,
  path         TEXT NOT NULL,
  query        TEXT NOT NULL DEFAULT '',
  headers      JSONB NOT NULL DEFAULT '{}'::jsonb,
  body         BYTEA,
  delivered_at TIMESTAMPTZ,
  attempts     INT NOT NULL DEFAULT 0,
  last_status  INT
);
-- Partial index: the drain query only ever asks for undelivered rows of one
-- tunnel, in arrival order.
CREATE INDEX IF NOT EXISTS pending_undelivered_idx
  ON pending (tunnel_id, id) WHERE delivered_at IS NULL;
`
	if _, err := d.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("persist: migrate: %w", err)
	}
	return nil
}

// hashOwner keeps raw tokens out of the database. A leaked backup should not
// hand anyone the ability to steal a reserved name.
func hashOwner(token string) string {
	sum := sha256.Sum256([]byte("doorbell-owner:" + token))
	return hex.EncodeToString(sum[:])
}

// ClaimName reserves name for the holder of token, or refreshes an existing
// claim by the same holder. It returns ErrNameHeld if someone else owns it.
//
// The INSERT ... ON CONFLICT DO UPDATE ... WHERE clause makes this atomic
// across containers: two gateways racing for the same name cannot both win,
// because the conflicting update only applies when the owner hash matches.
func (d *DB) ClaimName(ctx context.Context, name, token string) error {
	const q = `
INSERT INTO reservations (name, owner_hash)
VALUES ($1, $2)
ON CONFLICT (name) DO UPDATE
  SET last_seen = now()
  WHERE reservations.owner_hash = EXCLUDED.owner_hash
RETURNING name`

	var got string
	err := d.pool.QueryRow(ctx, q, name, hashOwner(token)).Scan(&got)
	if err != nil {
		// No row returned means the WHERE guard rejected the update, i.e. the
		// name exists under a different owner.
		return ErrNameHeld
	}
	return nil
}

// RecordRequest appends one line of history. Bodies are deliberately NOT stored:
// they are the most sensitive and largest part of the traffic, and the live
// inspector already shows them for the current session.
func (d *DB) RecordRequest(ctx context.Context, tunnelID string, at time.Time, method, path string, status int, durMs int64, errText string) error {
	const q = `INSERT INTO requests (tunnel_id, at, method, path, status, dur_ms, err)
	           VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''))`
	if _, err := d.pool.Exec(ctx, q, tunnelID, at, method, path, status, durMs, errText); err != nil {
		return fmt.Errorf("persist: record request: %w", err)
	}
	return nil
}

// HistoryRow is one persisted request.
type HistoryRow struct {
	TunnelID string    `json:"tunnelId"`
	At       time.Time `json:"at"`
	Method   string    `json:"method"`
	Path     string    `json:"path"`
	Status   int       `json:"status"`
	DurMs    int64     `json:"durMs"`
	Error    string    `json:"error,omitempty"`
}

// History returns the most recent persisted requests, newest first.
func (d *DB) History(ctx context.Context, limit int) ([]HistoryRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	const q = `SELECT tunnel_id, at, method, path, status, dur_ms, COALESCE(err,'')
	           FROM requests ORDER BY at DESC LIMIT $1`
	rows, err := d.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("persist: history: %w", err)
	}
	defer rows.Close()

	var out []HistoryRow
	for rows.Next() {
		var r HistoryRow
		if err := rows.Scan(&r.TunnelID, &r.At, &r.Method, &r.Path, &r.Status, &r.DurMs, &r.Error); err != nil {
			return nil, fmt.Errorf("persist: scan history: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ─── the mailbox: buffered requests and replay ──────────────────────────────

// MaxPendingPerTunnel caps how much a single tunnel can accumulate.
//
// Without a cap this table is a denial-of-service surface: anyone who learns a
// reserved tunnel name could point a load generator at it while the owner is
// offline and fill the disk. The oldest rows are dropped first, which is the
// right trade for webhook development — the newest events are the ones you
// were waiting for.
const MaxPendingPerTunnel = 200

// MaxStoredBody is the largest request body the mailbox will hold.
//
// It is deliberately much larger than the inspector's display limit. The
// inspector may show a prefix — nobody is harmed by a clipped preview. The
// mailbox may not: a body stored short is delivered short, with a matching
// Content-Length, and the receiving app sees malformed input that the sender
// never sent. Anything over this is refused outright rather than corrupted.
const MaxStoredBody = 1 << 20 // 1 MiB

// PendingRequest is one stored request, either awaiting first delivery or kept
// around so it can be replayed.
type PendingRequest struct {
	ID          int64             `json:"id"`
	TunnelID    string            `json:"tunnelId"`
	ReceivedAt  time.Time         `json:"receivedAt"`
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	Query       string            `json:"query,omitempty"`
	Headers     map[string]string `json:"headers"`
	Body        []byte            `json:"-"`
	BodySize    int               `json:"bodySize"`
	DeliveredAt *time.Time        `json:"deliveredAt,omitempty"`
	Attempts    int               `json:"attempts"`
	LastStatus  *int              `json:"lastStatus,omitempty"`
}

// Enqueue stores a request that arrived while no tunnel session was live, then
// trims the tunnel's mailbox back to MaxPendingPerTunnel.
func (d *DB) Enqueue(ctx context.Context, tunnelID, method, path, query string, headers map[string]string, body []byte) (int64, error) {
	head, err := json.Marshal(headers)
	if err != nil {
		return 0, fmt.Errorf("persist: marshal headers: %w", err)
	}

	var id int64
	const q = `INSERT INTO pending (tunnel_id, method, path, query, headers, body)
	           VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`
	if err := d.pool.QueryRow(ctx, q, tunnelID, method, path, query, head, body).Scan(&id); err != nil {
		return 0, fmt.Errorf("persist: enqueue: %w", err)
	}

	// Trim oldest beyond the cap. Best-effort: a failure here must not fail
	// the enqueue that already succeeded.
	const trim = `DELETE FROM pending WHERE id IN (
	                SELECT id FROM pending WHERE tunnel_id = $1
	                ORDER BY id DESC OFFSET $2)`
	if _, err := d.pool.Exec(ctx, trim, tunnelID, MaxPendingPerTunnel); err != nil {
		return id, nil
	}
	return id, nil
}

// Undelivered returns requests awaiting first delivery, oldest first.
//
// Order matters more than it looks: webhook streams are frequently causal
// (created → updated → deleted), and replaying them out of order can leave an
// app in a state the real sequence would never have produced.
func (d *DB) Undelivered(ctx context.Context, tunnelID string) ([]PendingRequest, error) {
	const q = `SELECT id, tunnel_id, received_at, method, path, query, headers, body, attempts, delivered_at, last_status
	           FROM pending WHERE tunnel_id = $1 AND delivered_at IS NULL ORDER BY id ASC`
	return d.scanPending(ctx, q, tunnelID)
}

// Get fetches one stored request by id, delivered or not.
func (d *DB) Get(ctx context.Context, id int64) (*PendingRequest, error) {
	const q = `SELECT id, tunnel_id, received_at, method, path, query, headers, body, attempts, delivered_at, last_status
	           FROM pending WHERE id = $1`
	list, err := d.scanPending(ctx, q, id)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("persist: no stored request %d", id)
	}
	return &list[0], nil
}

// Mailbox lists a tunnel's stored requests, newest first, for the dashboard.
func (d *DB) Mailbox(ctx context.Context, tunnelID string, limit int) ([]PendingRequest, error) {
	if limit <= 0 || limit > MaxPendingPerTunnel {
		limit = 50
	}
	const q = `SELECT id, tunnel_id, received_at, method, path, query, headers, body, attempts, delivered_at, last_status
	           FROM pending WHERE tunnel_id = $1 ORDER BY id DESC LIMIT $2`
	return d.scanPending(ctx, q, tunnelID, limit)
}

// PendingCount reports how many requests are waiting, across all tunnels.
func (d *DB) PendingCount(ctx context.Context) (map[string]int, error) {
	const q = `SELECT tunnel_id, count(*) FROM pending
	           WHERE delivered_at IS NULL GROUP BY tunnel_id`
	rows, err := d.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("persist: pending count: %w", err)
	}
	defer rows.Close()

	out := map[string]int{}
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("persist: scan pending count: %w", err)
		}
		out[id] = n
	}
	return out, rows.Err()
}

// MarkDelivered records a successful (or attempted) delivery.
func (d *DB) MarkDelivered(ctx context.Context, id int64, status int) error {
	const q = `UPDATE pending
	           SET delivered_at = now(), attempts = attempts + 1, last_status = $2
	           WHERE id = $1`
	if _, err := d.pool.Exec(ctx, q, id, status); err != nil {
		return fmt.Errorf("persist: mark delivered: %w", err)
	}
	return nil
}

func (d *DB) scanPending(ctx context.Context, q string, args ...any) ([]PendingRequest, error) {
	rows, err := d.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("persist: query pending: %w", err)
	}
	defer rows.Close()

	var out []PendingRequest
	for rows.Next() {
		var p PendingRequest
		var head []byte
		if err := rows.Scan(&p.ID, &p.TunnelID, &p.ReceivedAt, &p.Method, &p.Path,
			&p.Query, &head, &p.Body, &p.Attempts, &p.DeliveredAt, &p.LastStatus); err != nil {
			return nil, fmt.Errorf("persist: scan pending: %w", err)
		}
		if err := json.Unmarshal(head, &p.Headers); err != nil {
			p.Headers = map[string]string{}
		}
		p.BodySize = len(p.Body)
		out = append(out, p)
	}
	return out, rows.Err()
}

// IsReserved reports whether anyone has claimed this name.
//
// This is the gate on buffering. Doorbell will hold requests for a name its
// owner registered, and will not hold anything for a name nobody claimed —
// otherwise every typo and every drive-by scan would allocate storage.
func (d *DB) IsReserved(ctx context.Context, name string) (bool, error) {
	var exists bool
	const q = `SELECT EXISTS(SELECT 1 FROM reservations WHERE name = $1)`
	if err := d.pool.QueryRow(ctx, q, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("persist: is reserved: %w", err)
	}
	return exists, nil
}
