.PHONY: test-e2e fixtures-check-matrix lint fmt vet tests

all: lint fmt vet tests
tests: test-e2e fixtures-check-matrix

GO_VERSIONS := 1.21.13 1.22.12 1.23.12 1.24.10 1.25.4
PLATFORMS   := linux/amd64 linux/arm64

test-e2e:
	@echo "===> Running end-to-end tests"
	go test -v ./...

fixtures-check-matrix:
	@echo "===> Running fixtures check matrix"
	@for v in $(GO_VERSIONS); do \
	  for p in $(PLATFORMS); do \
	    echo "===> Fixtures check: Go $$v on $$p"; \
	    docker run --rm --platform=$$p \
	      -e GO_VERSION=$$v \
	      -v "$$PWD":/workspace \
	      -w /workspace \
	      golang:$$v-alpine \
	      go run ./cmd/ext4-fixtures check || exit $$?; \
	  done; \
	done

lint:
	@echo "===> Running lint"
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not installed. Run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
		exit 1; \
	}
	@golangci-lint run --exclude-files e2e_test.go,tmp/

fmt:
	@echo "===> Running fmt"
	@go fmt ./...

vet:
	@echo "===> Running vet"
	@go vet ./...
