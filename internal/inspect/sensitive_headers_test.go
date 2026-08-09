package inspect_test

import (
	"testing"

	"github.com/BigAchiever/doorbell/internal/inspect"
)

func TestIsSensitiveCoversRealProviders(t *testing.T) {
	secret := []string{
		"Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie",
		"X-Api-Key", "X-Auth-Token",
		"Stripe-Signature",      // Stripe
		"X-Hub-Signature-256",   // GitHub
		"X-Hub-Signature",       // GitHub, legacy
		"X-Gitlab-Token",        // GitLab CI
		"X-Slack-Signature",     // Slack
		"X-Shopify-Hmac-Sha256", // Shopify
		"X-Twilio-Signature",    // Twilio
		"X-Razorpay-Signature",  // Razorpay
		"Svix-Signature",        // Svix, and everything built on it
		"Webhook-Signature",     // the generic spelling
		"X-Webhook-Secret",
	}
	for _, h := range secret {
		if !inspect.IsSensitive(h) {
			t.Errorf("IsSensitive(%q) = false, want true — this header would reach Postgres in the clear", h)
		}
	}

	ordinary := []string{
		"Content-Type", "Content-Length", "User-Agent", "Accept",
		"X-Request-Id", "X-Forwarded-For", "X-Github-Event", "Host", "Date",
	}
	for _, h := range ordinary {
		if inspect.IsSensitive(h) {
			t.Errorf("IsSensitive(%q) = true, want false — redacting this drops it from replay for no reason", h)
		}
	}
}
