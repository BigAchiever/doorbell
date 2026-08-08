// Package dashboard serves Doorbell's two pages: a landing page that explains
// what this is to someone who arrived cold, and the live request inspector.
//
// The HTML lives beside this file rather than in a top-level web/ directory
// because go:embed cannot reach outside its own package directory — a parent
// path like ../../web is rejected at compile time, not runtime.
package dashboard

import _ "embed"

// Landing is the public front door: the pitch, live counts, and how to connect.
// A judge or a stranger opening the gateway URL cold lands here, not on an
// unexplained table of HTTP requests.
//
//go:embed landing.html
var Landing []byte

// HTML is the live request inspector, served at /dashboard.
//
//go:embed dashboard.html
var HTML []byte
