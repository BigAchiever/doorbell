// Hour-0 gate for Doorbell.
//
// Doorbell's entire premise is that a client can hold a raw TCP socket open to
// a Zerops service from the public internet. The docs say ports 10-65435 are
// available to runtime services. That is documented, not measured — and it is
// the one assumption that, if wrong, kills the project.
//
// So: prove it in ten minutes, not at hour 30.
//
// This listens on two ports at once:
//
//	:3000  HTTP  — should be reachable at the free https://*.zerops.app subdomain
//	:7000  TCP   — the real test. Echoes back whatever you send.
//
// GREEN if, from your laptop:  nc <host> 7000   echoes your typing back.
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

func main() {
	httpPort := envOr("PORT", "3000")
	tcpPort := envOr("CONTROL_PORT", "7000")

	go serveHTTP(httpPort, tcpPort)
	serveTCP(tcpPort)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func serveHTTP(port, tcpPort string) {
	mux := http.NewServeMux()

	// Zerops deploy readinessCheck hits this.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `Doorbell raw-TCP gate.

HTTP on :%s works — you are reading this, so the Zerops subdomain and managed
TLS are fine. That was never in doubt.

THE ACTUAL TEST is the raw TCP port. From your laptop:

    nc <this-host> %s

Type anything and press enter. If it echoes back, raw public TCP works and
Doorbell is buildable. If it hangs or refuses, the control port is not publicly
reachable and Doorbell dies here instead of at hour 30.
`, port, tcpPort)
	})

	log.Printf("http listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("http listener died: %v", err)
	}
}

func serveTCP(port string) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("could not bind tcp :%s — %v", port, err)
	}
	log.Printf("raw tcp listening on :%s", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept failed: %v", err)
			continue
		}
		go echo(conn)
	}
}

// echo mirrors input back with a timestamp. The timestamp matters: it proves the
// connection is genuinely persistent rather than a single request/response, which
// is exactly the property Doorbell depends on and serverless cannot provide.
func echo(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	log.Printf("connection opened from %s", remote)

	fmt.Fprintf(conn, "doorbell gate: connected. type something.\n")

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		log.Printf("%s sent %q", remote, line)
		fmt.Fprintf(conn, "[%s] echo: %s\n", time.Now().UTC().Format("15:04:05"), line)
	}

	log.Printf("connection closed from %s", remote)
}
