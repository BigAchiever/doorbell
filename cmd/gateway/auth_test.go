package main

// Opening a tunnel and reading the inspector are two different privileges, and
// the gateway used to check the same token for both. That meant anyone you let
// forward a port could also read every captured request and response body on
// the gateway, and re-fire any of them. These tests pin the two apart.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/BigAchiever/Doorbell/internal/inspect"
	"github.com/BigAchiever/Doorbell/internal/registry"
)

func gatewayWithTokens(client, admin string) *gateway {
	return &gateway{
		cfg:   config{clientToken: client, adminToken: admin},
		store: registry.NewMemory(1),
		rec:   inspect.NewRecorder(10),
	}
}

// reachedOperator reports whether a request got past requireOperator, and the
// status the middleware wrote if it did not.
func reachedOperator(g *gateway, r *http.Request) (bool, int) {
	var arrived bool
	h := g.requireOperator(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		arrived = true
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return arrived, rec.Code
}

// The posture a public gateway runs in: anyone may tunnel, nobody may snoop.
func TestPublicGatewayAcceptsAnyClientButStillGuardsTheInspector(t *testing.T) {
	g := gatewayWithTokens("", "operator-secret")

	if !g.tokenOK("") {
		t.Error("a gateway with no client token must accept a client that presents none")
	}
	if !g.tokenOK("anything at all") {
		t.Error("a gateway with no client token must accept any token a client presents")
	}

	// ...but the operator surface stays shut to that same caller.
	got, code := reachedOperator(g, httptest.NewRequest(http.MethodGet, "/api/mailbox", nil))
	if got {
		t.Error("an unauthenticated request reached the operator API on a public gateway")
	}
	if code == http.StatusOK {
		t.Error("the operator API answered 200 to an unauthenticated request")
	}
}

// The hole this pair of checks exists to prevent: on a public gateway the
// client check accepts anything, so if ?token= were validated against it, the
// first visitor to guess the parameter would be handed an operator cookie.
func TestGuessedQueryTokenCannotUnlockTheDashboard(t *testing.T) {
	g := gatewayWithTokens("", "operator-secret")

	got, _ := reachedOperator(g, httptest.NewRequest(http.MethodGet, "/dashboard?token=anything", nil))
	if got {
		t.Fatal("?token=anything unlocked the dashboard on a public gateway")
	}

	// The real one still works, and answers with the cookie-setting redirect.
	_, code := reachedOperator(g, httptest.NewRequest(http.MethodGet, "/dashboard?token=operator-secret", nil))
	if code != http.StatusSeeOther {
		t.Errorf("the operator's own ?token= link answered %d, want %d", code, http.StatusSeeOther)
	}
}

// The regression itself: the operator token must not double as a tunnel key.
func TestOperatorTokenIsNotAClientToken(t *testing.T) {
	g := gatewayWithTokens("client-key", "operator-secret")

	if !g.tokenOK("client-key") {
		t.Error("the client token must open a tunnel")
	}
	if g.tokenOK("operator-secret") {
		t.Error("the operator token opened a tunnel; the two privileges are conflated again")
	}
	if g.tokenOK("wrong") {
		t.Error("an unrelated token opened a tunnel")
	}
	if g.tokenOK("") {
		t.Error("an empty token opened a tunnel on a gateway that requires one")
	}
}

// A client token must not unlock the inspector either — the leak in reverse.
func TestClientTokenCannotReachTheOperatorAPI(t *testing.T) {
	g := gatewayWithTokens("client-key", "operator-secret")

	req := httptest.NewRequest(http.MethodGet, "/api/mailbox", nil)
	req.Header.Set("Authorization", "Bearer client-key")

	if got, _ := reachedOperator(g, req); got {
		t.Error("a tunnel client's token was accepted by the operator API")
	}
}

func TestOperatorTokenReachesTheOperatorAPI(t *testing.T) {
	g := gatewayWithTokens("client-key", "operator-secret")

	req := httptest.NewRequest(http.MethodGet, "/api/mailbox", nil)
	req.Header.Set("Authorization", "Bearer operator-secret")

	got, code := reachedOperator(g, req)
	if !got {
		t.Errorf("the operator was locked out of their own API (status %d)", code)
	}
}
