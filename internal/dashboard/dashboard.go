// Package dashboard serves Doorbell's web interface: a landing page for people
// arriving cold, a live request inspector, and the shared stylesheet and script
// both are built from.
//
// The HTML lives beside this file rather than in a top-level web/ directory
// because go:embed cannot reach outside its own package directory — a parent
// path like ../../web is rejected at compile time, not runtime.
package dashboard

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
)

// Landing is the public front door: the pitch, live counts, and how to connect.
// Someone opening the gateway URL cold lands here, not on an unexplained table
// of HTTP requests.
//
//go:embed landing.html
var Landing []byte

// HTML is the live request inspector, served at /dashboard.
//
//go:embed dashboard.html
var HTML []byte

// assets holds the design system and shared script. Keeping them as real files
// rather than inlining into each page means the browser caches them once and
// any page added later inherits the same components instead of copying them.
//
//go:embed assets
var assets embed.FS

// etags maps an asset path to a hash of its contents, computed once at start.
// Embedded files have no useful modification time, so a content hash is the
// only honest validator — and it changes exactly when a deploy changes the file.
var etags = map[string]string{}

func init() {
	_ = fs.WalkDir(assets, "assets", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		body, err := assets.ReadFile(p)
		if err != nil {
			return nil
		}
		sum := sha256.Sum256(body)
		etags[p] = `"` + hex.EncodeToString(sum[:8]) + `"`
		return nil
	})
}

// AssetHandler serves /assets/* from the embedded filesystem.
//
// It answers conditional requests with 304 so a dashboard left open all weekend
// re-fetches the stylesheet only when it has actually changed.
func AssetHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// path.Clean plus the prefix check keeps "/assets/../../etc/passwd"
		// from escaping the embedded tree.
		name := path.Clean("assets/" + strings.TrimPrefix(r.URL.Path, "/assets/"))
		if !strings.HasPrefix(name, "assets/") {
			http.NotFound(w, r)
			return
		}

		body, err := assets.ReadFile(name)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		tag := etags[name]
		if tag != "" {
			w.Header().Set("ETag", tag)
			// no-cache means "revalidate", not "do not store": the browser keeps
			// the file and we answer the revalidation with a 26-byte 304.
			w.Header().Set("Cache-Control", "no-cache")
			if match := r.Header.Get("If-None-Match"); match == tag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}

		if ct := mime.TypeByExtension(path.Ext(name)); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		if _, err := w.Write(body); err != nil {
			return // client went away mid-response; nothing useful to log
		}
	})
}
