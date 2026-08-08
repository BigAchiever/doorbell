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
