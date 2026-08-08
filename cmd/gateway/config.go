package main

// Configuration read from the environment at start-up.

import (
	"log"
	"os"
	"strings"
)

const (
	modeZeroConfig = "zeroconfig"
	modeWildcard   = "wildcard"
)

type config struct {
	httpPort    string
	controlPort string
	mode        string
	baseDomain  string

	// Two tokens, because opening a tunnel and reading the inspector are not
	// the same privilege. clientToken lets someone forward their own port;
	// adminToken lets someone read every captured body on the gateway and
	// re-fire any stored request. Sharing one value for both means everyone
	// you let tunnel can also read everyone else's payloads.
	clientToken string // gates the control port; empty means anyone may connect
	adminToken  string // gates the dashboard and the operator API

	publicBase string // externally visible origin, for building tunnel URLs

	// Where clients should dial the control port, when that is not the same
	// host the dashboard is served from. On Zerops it never is: the *.zerops.app
	// name points at the shared L7 balancer, which speaks HTTP only, while the
	// raw TCP port is published on the project's own public IP. Telling a
	// visitor to dial the address in their URL bar sends them somewhere that
	// will refuse the connection.
	controlHost string
	databaseURL string
	redisURL    string
}

func loadConfig() config {
	c := config{
		httpPort:    envOr("PORT", "3000"),
		controlPort: envOr("CONTROL_PORT", "7000"),
		mode:        envOr("DOORBELL_MODE", modeZeroConfig),
		baseDomain:  os.Getenv("DOORBELL_BASE_DOMAIN"),
		clientToken: os.Getenv("DOORBELL_CLIENT_TOKEN"),
		adminToken:  os.Getenv("DOORBELL_ADMIN_TOKEN"),
		publicBase:  strings.TrimSuffix(os.Getenv("DOORBELL_PUBLIC_BASE"), "/"),
		controlHost: os.Getenv("DOORBELL_CONTROL_HOST"),
		databaseURL: os.Getenv("DATABASE_URL"),
		redisURL:    os.Getenv("REDIS_URL"),
	}
	// Wildcard mode without a domain is a misconfiguration that would produce
	// broken URLs on every tunnel. Fall back loudly rather than serving them.
	if c.mode == modeWildcard && c.baseDomain == "" {
		log.Printf("WARN mode=wildcard but DOORBELL_BASE_DOMAIN is empty; falling back to zeroconfig")
		c.mode = modeZeroConfig
	}
	// An unset admin token is the dangerous one: it leaves captured request and
	// response bodies, and the replay endpoint, open to the internet.
	if c.adminToken == "" {
		log.Printf("WARN DOORBELL_ADMIN_TOKEN is empty — the dashboard and operator API are UNPROTECTED")
	}
	// An unset client token is a deliberate posture, not a mistake: it is what a
	// public gateway wants. Say so once so it is never a surprise.
	if c.clientToken == "" {
		log.Printf("control: DOORBELL_CLIENT_TOKEN unset — anyone may open a tunnel on this gateway")
	}
	return c
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
