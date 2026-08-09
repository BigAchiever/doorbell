package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/BigAchiever/doorbell/internal/inspect"
	"github.com/BigAchiever/doorbell/internal/registry"
)

func TestSplitTunnelPath(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		wantID   string
		wantRest string
	}{
		{"root with slash", "/t/quiet-frog/", "quiet-frog", "/"},
		// No trailing slash still means the app's root, not a missing path.
		// Getting this wrong sends "" upstream and the origin 404s its own index.
		{"root without slash", "/t/quiet-frog", "quiet-frog", "/"},
		{"nested path", "/t/quiet-frog/api/users", "quiet-frog", "/api/users"},
		{"deep path", "/t/shop/a/b/c/d", "shop", "/a/b/c/d"},
		{"path with dots", "/t/shop/static/app.min.css", "shop", "/static/app.min.css"},
		{"not a tunnel path", "/dashboard", "", ""},
		{"bare prefix", "/t/", "", "/"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, rest := splitTunnelPath(tc.in)
			if id != tc.wantID || rest != tc.wantRest {
				t.Errorf("splitTunnelPath(%q) = (%q, %q), want (%q, %q)",
					tc.in, id, rest, tc.wantID, tc.wantRest)
			}
		})
	}
}

// TestWildcardHostRewrite covers the translation from abc.example.com to
// /t/abc/... — including the cases that must NOT be rewritten, because a
// false positive here would swallow the landing page and the API.
func TestWildcardHostRewrite(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		domain   string
		host     string
		path     string
		wantPath string
	}{
		{"rewrites a tunnel subdomain", modeWildcard, "example.com", "abc.example.com", "/api/users", "/t/abc/api/users"},
		{"keeps the port out of the id", modeWildcard, "example.com", "abc.example.com:3000", "/x", "/t/abc/x"},
		{"apex domain is not a tunnel", modeWildcard, "example.com", "example.com", "/", "/"},
		{"multi-label subdomain is not a tunnel", modeWildcard, "example.com", "a.b.example.com", "/", "/"},
		{"unrelated host untouched", modeWildcard, "example.com", "evil.test", "/", "/"},
		{"zeroconfig never rewrites", modeZeroConfig, "example.com", "abc.example.com", "/", "/"},
		{"wildcard without a domain never rewrites", modeWildcard, "", "abc.example.com", "/", "/"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := &gateway{cfg: config{mode: tc.mode, baseDomain: tc.domain}}

			var got string
			h := g.wildcardHost(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				got = r.URL.Path
			}))

			req := httptest.NewRequest(http.MethodGet, "http://"+tc.host+tc.path, nil)
			req.Host = tc.host
			h.ServeHTTP(httptest.NewRecorder(), req)

			if got != tc.wantPath {
				t.Errorf("host %q path %q -> %q, want %q", tc.host, tc.path, got, tc.wantPath)
			}
		})
	}
}

// TestUnknownTunnelIs404 guards the degraded path: with no database and no
// Valkey there is nothing to buffer into and no peer to forward to, so an
// unknown name must be an honest 404 rather than a hang or a 502.
func TestUnknownTunnelIs404(t *testing.T) {
	g := &gateway{
		cfg:   config{mode: modeZeroConfig, httpPort: "3000"},
		store: registry.NewMemory(1),
		rec:   inspect.NewRecorder(10),
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/t/nobody-home/x", nil)
	g.handleTunnelRequest(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
