GOLANGCI_LINT_VERSION := v2.6.2
GOLANGCI_LINT_PATH := $(HOME)/.local/bin/golangci-lint-$(subst v,,$(GOLANGCI_LINT_VERSION))

.PHONY: test-e2e fixtures-check-matrix lint tests all

all: lint tests

GO_VERSIONS := 1.21.13 1.22.12 1.23.12 1.24.10 1.25.4
PLATFORMS   := linux/amd64 linux/arm64

tests:
	@echo "===> Running end-to-end tests"
	go test -v ./...

$(GOLANGCI_LINT_PATH):
	@echo "===> Installing golangci-lint $(GOLANGCI_LINT_VERSION)"
	@mkdir -p $(HOME)/.local/bin
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b /tmp $(GOLANGCI_LINT_VERSION)
	@mv /tmp/golangci-lint $(GOLANGCI_LINT_PATH)

lint: $(GOLANGCI_LINT_PATH)
	@echo "===> Running lint"
	@$(GOLANGCI_LINT_PATH) run

fmt: $(GOLANGCI_LINT_PATH)
	@echo "===> Running fmt"
	@$(GOLANGCI_LINT_PATH) fmt
