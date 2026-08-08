package main

// Operator authentication for the dashboard and the JSON API.
//
// Tunnels themselves are not gated by this — only the surfaces that expose
// captured request and response bodies.

import (
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/BigAchiever/Doorbell/internal/dashboard"
)

func (g *gateway) tokenOK(presented string) bool {
	if g.cfg.adminToken == "" {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(g.cfg.adminToken)) == 1
}

// operatorCookie carries the dashboard session. HttpOnly because no script
// needs to read it, and its only job is to ride along with same-origin fetches.
const operatorCookie = "doorbell_operator"

// requireOperator gates the dashboard and the operator API.
//
// These endpoints expose captured request and response bodies — whole webhook
// payloads, whatever the tunnel proxied — and /api/replay re-fires a stored
// request into the developer's app. All of it was reachable by anyone who knew
// the hostname, while the control port that merely opens a tunnel demanded a
// token. This closes that gap.
//
// The tunnel data path (/t/…), the landing page, /healthz and /assets stay
// public: those are for the outside world by design.
func (g *gateway) requireOperator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if g.cfg.adminToken == "" {
			next.ServeHTTP(w, r) // unauthenticated gateway; already warned at boot
			return
		}

		// A ?token= query sets the cookie once, so the operator can open the
		// dashboard from a link instead of hand-crafting a header.
		if q := r.URL.Query().Get("token"); q != "" {
			if !g.tokenOK(q) {
				g.denyOperator(w, r)
				return
			}
			http.SetCookie(w, &http.Cookie{
				Name:     operatorCookie,
				Value:    q,
				Path:     "/",
				HttpOnly: true,
				Secure:   g.servedOverTLS(r),
				SameSite: http.SameSiteLaxMode,
				MaxAge:   7 * 24 * 3600,
			})
			// Redirect so the token does not linger in the address bar, in
			// history, or in any Referer this page later sends.
			clean := *r.URL
			q := clean.Query()
			q.Del("token")
			clean.RawQuery = q.Encode()
			http.Redirect(w, r, clean.RequestURI(), http.StatusSeeOther)
			return
		}

		if presented, ok := operatorToken(r); ok && g.tokenOK(presented) {
			next.ServeHTTP(w, r)
			return
		}
		g.denyOperator(w, r)
	})
}

// servedOverTLS reports whether this gateway is actually reached over HTTPS.
//
// schemeOf() is not usable here. Zerops terminates TLS at the L7 balancer and
// forwards to the app in plaintext, so it sets X-Forwarded-Proto: http — the
// scheme of the INTERNAL hop, not the one the browser used. Trusting it left
// the operator cookie without a Secure flag on a site served over HTTPS,
// verified against the live deployment.
//
// DOORBELL_PUBLIC_BASE is the operator's own statement of the externally
// visible origin, so it is the honest source. The request scheme is only a
// fallback for local development, where the base is unset.
func (g *gateway) servedOverTLS(r *http.Request) bool {
	if g.cfg.publicBase != "" {
		return strings.HasPrefix(g.cfg.publicBase, "https://")
	}
	return schemeOf(r) == "https"
}

// operatorToken pulls the credential from an Authorization header (for curl and
// scripts) or the cookie (for the dashboard).
func operatorToken(r *http.Request) (string, bool) {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer "), true
	}
	if c, err := r.Cookie(operatorCookie); err == nil {
		return c.Value, true
	}
	return "", false
}

func (g *gateway) denyOperator(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "operator token required — send Authorization: Bearer <DOORBELL_ADMIN_TOKEN>, or open /dashboard?token=<token> once to set a cookie",
		})
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusUnauthorized)
	if _, err := w.Write(dashboard.Locked); err != nil {
		log.Printf("dashboard: write locked page: %v", err)
	}
}
