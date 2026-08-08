package registry

import (
	"errors"
	"strings"
	"testing"
)

func TestAddAssignsAReadableName(t *testing.T) {
	m := NewMemory(42)

	tun, err := m.Add("", 3000, "session")
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	// The name ends up in a URL people read aloud and type into a Stripe
	// dashboard, so "adjective-noun" is a requirement, not decoration.
	if !strings.Contains(tun.ID, "-") {
		t.Errorf("generated name %q is not adjective-noun", tun.ID)
	}
	if tun.LocalPort != 3000 {
		t.Errorf("LocalPort = %d, want 3000", tun.LocalPort)
	}
}

func TestGeneratedNamesDoNotCollide(t *testing.T) {
	m := NewMemory(7)
	seen := map[string]bool{}

	// Comfortably more than the adjective×noun space is unnecessary; this is
	// enough to exercise the retry loop and then the numeric-suffix fallback.
	for i := 0; i < 150; i++ {
		tun, err := m.Add("", 3000, "s")
		if err != nil {
			t.Fatalf("Add #%d: %v", i, err)
		}
		if seen[tun.ID] {
			t.Fatalf("duplicate name issued: %q", tun.ID)
		}
		seen[tun.ID] = true
	}
}

func TestRequestedNameIsHonoured(t *testing.T) {
	m := NewMemory(1)
	tun, err := m.Add("shop", 8080, "s")
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if tun.ID != "shop" {
		t.Errorf("ID = %q, want shop", tun.ID)
	}
}

func TestSecondClaimOnALiveNameIsRejected(t *testing.T) {
	m := NewMemory(1)
	if _, err := m.Add("shop", 8080, "s1"); err != nil {
		t.Fatalf("first Add: %v", err)
	}
	_, err := m.Add("shop", 9090, "s2")
	if !errors.Is(err, ErrTaken) {
		t.Errorf("second Add error = %v, want ErrTaken", err)
	}
}

func TestGetAndRemove(t *testing.T) {
	m := NewMemory(1)
	if _, err := m.Add("shop", 8080, "s"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := m.Get("shop"); err != nil {
		t.Errorf("Get after Add: %v", err)
	}

	m.Remove("shop")

	_, err := m.Get("shop")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Remove = %v, want ErrNotFound", err)
	}
	// A removed name must become claimable again, otherwise reconnecting with
	// a reserved name would fail for the lifetime of the process.
	if _, err := m.Add("shop", 8080, "s2"); err != nil {
		t.Errorf("re-Add after Remove: %v", err)
	}
}

func TestListReflectsLiveTunnels(t *testing.T) {
	m := NewMemory(1)
	for _, n := range []string{"a", "b", "c"} {
		if _, err := m.Add(n, 3000, "s"); err != nil {
			t.Fatalf("Add %s: %v", n, err)
		}
	}
	if got := len(m.List()); got != 3 {
		t.Errorf("List() has %d entries, want 3", got)
	}
	m.Remove("b")
	if got := len(m.List()); got != 2 {
		t.Errorf("after Remove, List() has %d entries, want 2", got)
	}
}
