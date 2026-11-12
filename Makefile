GO ?= go
NPM ?= npm
GOLANGCI_LINT ?= golangci-lint
WEB_DIR := web

# Scope Go tooling to first-party packages so generated/vendored trees
# (web/node_modules, web/dist) are never scanned.
GO_PACKAGES := ./cmd/... ./internal/...
GO_SOURCES := $(shell find cmd internal -type f -name '*.go')

.DEFAULT_GOAL := verify

.PHONY: up down run web genenv test verify \
        fmt vet lint go-test web-test go-build web-build typecheck build

# --- Local stack -----------------------------------------------------------

genenv: # Generate a gitignored .env with random local-only credentials
	sh ./scripts/genenv.sh

up: genenv # Start the full local stack (Postgres, MinIO, API, worker, web)
	docker compose up -d --build

down: # Stop and remove the local stack
	docker compose down

run: # Run the API process directly on the host
	$(GO) run ./cmd/api

web: # Run the web dev server with hot reload
	cd $(WEB_DIR) && $(NPM) run dev

# --- Verification ----------------------------------------------------------

verify: fmt vet lint test typecheck build
	@echo "verify: OK"

fmt: # Check Go and web formatting without writing changes
	@unformatted="$$(gofmt -l $(GO_SOURCES))"; \
	if [ -n "$$unformatted" ]; then \
		echo "Go files need formatting (run gofmt -w):"; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	cd $(WEB_DIR) && $(NPM) run format:check

vet: # Run go vet
	$(GO) vet $(GO_PACKAGES)

lint: # Run the Go linter (golangci-lint when available) and web ESLint
	@if command -v $(GOLANGCI_LINT) >/dev/null 2>&1; then \
		$(GOLANGCI_LINT) run --timeout=5m $(GO_PACKAGES); \
	else \
		echo "golangci-lint not found; using 'go vet' for Go lint"; \
		$(GO) vet $(GO_PACKAGES); \
	fi
	cd $(WEB_DIR) && $(NPM) run lint

test: go-test web-test # Run Go and web unit tests

go-test:
	$(GO) test -race $(GO_PACKAGES)

web-test:
	cd $(WEB_DIR) && $(NPM) run test

typecheck: # Typecheck the web app
	cd $(WEB_DIR) && $(NPM) run typecheck

build: go-build web-build # Build Go binaries and the web bundle

go-build:
	$(GO) build $(GO_PACKAGES)

web-build:
	cd $(WEB_DIR) && $(NPM) run build
