// Package dashboard serves Doorbell's single-page request inspector.
//
// The HTML lives beside this file rather than in a top-level web/ directory
// because go:embed cannot reach outside its own package directory — a parent
// path like ../../web is rejected at compile time, not runtime.
package dashboard

import _ "embed"

// HTML is the compiled-in dashboard page.
//
//go:embed dashboard.html
var HTML []byte
