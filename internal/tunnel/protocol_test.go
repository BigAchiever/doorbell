package tunnel

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestHandshakeRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	sent := Hello{
		Version:       ProtocolVersion,
		Token:         "secret",
		Subdomain:     "shop",
		LocalPort:     3000,
		ClientVersion: "0.1.0",
	}
	if err := WriteJSONLine(&buf, sent); err != nil {
		t.Fatalf("WriteJSONLine: %v", err)
	}

	var got Hello
	if err := ReadJSONLine(bufio.NewReader(&buf), &got); err != nil {
		t.Fatalf("ReadJSONLine: %v", err)
	}
	if got != sent {
		t.Errorf("round trip changed the message:\n got %+v\nwant %+v", got, sent)
	}
}

// TestReaderStopsAtTheNewline is the regression guard for the subtlest bug in
// this codebase. The same socket is handed to yamux immediately after the
// handshake, so if the reader consumes past the newline those bytes vanish and
// the session hangs on its first frame — with no error anywhere.
func TestReaderStopsAtTheNewline(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteJSONLine(&buf, Welcome{Version: ProtocolVersion, ID: "shop"}); err != nil {
		t.Fatalf("WriteJSONLine: %v", err)
	}
	// Whatever follows stands in for the first bytes of the yamux session.
	buf.WriteString("YAMUX-FRAME-BYTES")

	br := bufio.NewReader(&buf)
	var w Welcome
	if err := ReadJSONLine(br, &w); err != nil {
		t.Fatalf("ReadJSONLine: %v", err)
	}

	rest, err := br.ReadString('\n')
	if err != nil && rest == "" {
		t.Fatal("nothing left for yamux — the handshake reader swallowed the stream")
	}
	if rest != "YAMUX-FRAME-BYTES" {
		t.Errorf("bytes after the handshake = %q, want the frame intact", rest)
	}
}

func TestOversizedHandshakeIsRefused(t *testing.T) {
	// An unauthenticated peer must not be able to stream unbounded data into
	// the gateway's memory before saying who it is.
	flood := `{"version":1,"token":"` + strings.Repeat("A", MaxHandshakeBytes+1024) + `"}` + "\n"

	var h Hello
	err := ReadJSONLine(bufio.NewReader(strings.NewReader(flood)), &h)
	if !errors.Is(err, ErrHandshakeTooLarge) {
		t.Errorf("error = %v, want ErrHandshakeTooLarge", err)
	}
}

func TestMalformedHandshakeIsAnError(t *testing.T) {
	var h Hello
	err := ReadJSONLine(bufio.NewReader(strings.NewReader("not json at all\n")), &h)
	if err == nil {
		t.Fatal("malformed handshake accepted")
	}
	// The message should quote what arrived, so a bad client is debuggable.
	if !strings.Contains(err.Error(), "not json at all") {
		t.Errorf("error %q does not include the offending input", err)
	}
}

func TestWelcomeCarriesRefusalReason(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteJSONLine(&buf, Welcome{Version: ProtocolVersion, Error: "invalid token"}); err != nil {
		t.Fatalf("WriteJSONLine: %v", err)
	}
	var got Welcome
	if err := ReadJSONLine(bufio.NewReader(&buf), &got); err != nil {
		t.Fatalf("ReadJSONLine: %v", err)
	}
	if got.Error != "invalid token" {
		t.Errorf("Error = %q, want %q", got.Error, "invalid token")
	}
	if got.ID != "" {
		t.Errorf("a refusal should carry no tunnel id, got %q", got.ID)
	}
}
