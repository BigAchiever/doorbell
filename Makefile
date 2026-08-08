# Doorbell — build and release helpers.
BINDIR ?= dist
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.clientVersion=$(VERSION)

.PHONY: all build cli gateway test fmt vet clean release install

all: test build

build: gateway cli

gateway:
	go build -ldflags '$(LDFLAGS)' -o $(BINDIR)/doorbell-gw ./cmd/gateway

cli:
	go build -ldflags '$(LDFLAGS)' -o $(BINDIR)/doorbell ./cmd/doorbell

# Install the CLI onto this machine's PATH (needs Go).
install:
	go install -ldflags '$(LDFLAGS)' ./cmd/doorbell

test:
	go test ./...

fmt:
	gofmt -l -w .

vet:
	go vet ./...

# Cross-compiled CLI binaries, so someone can try Doorbell without Go installed.
release:
	@mkdir -p $(BINDIR)
	@for target in darwin/arm64 darwin/amd64 linux/amd64 linux/arm64 windows/amd64; do \
		os=$${target%/*}; arch=$${target#*/}; ext=""; \
		[ "$$os" = "windows" ] && ext=".exe"; \
		echo "  building doorbell-$$os-$$arch$$ext"; \
		GOOS=$$os GOARCH=$$arch go build -ldflags '$(LDFLAGS)' \
			-o $(BINDIR)/doorbell-$$os-$$arch$$ext ./cmd/doorbell || exit 1; \
	done
	@echo "binaries in $(BINDIR)/"

clean:
	rm -rf $(BINDIR)
