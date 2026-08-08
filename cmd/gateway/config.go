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
	adminToken  string
	publicBase  string // externally visible origin, for building tunnel URLs
	databaseURL string
	redisURL    string
}

func loadConfig() config {
	c := config{
		httpPort:    envOr("PORT", "3000"),
		controlPort: envOr("CONTROL_PORT", "7000"),
		mode:        envOr("DOORBELL_MODE", modeZeroConfig),
		baseDomain:  os.Getenv("DOORBELL_BASE_DOMAIN"),
		adminToken:  os.Getenv("DOORBELL_ADMIN_TOKEN"),
		publicBase:  strings.TrimSuffix(os.Getenv("DOORBELL_PUBLIC_BASE"), "/"),
		databaseURL: os.Getenv("DATABASE_URL"),
		redisURL:    os.Getenv("REDIS_URL"),
	}
	// Wildcard mode without a domain is a misconfiguration that would produce
	// broken URLs on every tunnel. Fall back loudly rather than serving them.
	if c.mode == modeWildcard && c.baseDomain == "" {
		log.Printf("WARN mode=wildcard but DOORBELL_BASE_DOMAIN is empty; falling back to zeroconfig")
		c.mode = modeZeroConfig
	}
	if c.adminToken == "" {
		log.Printf("WARN DOORBELL_ADMIN_TOKEN is empty — this gateway accepts ANY client token")
	}
	return c
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
