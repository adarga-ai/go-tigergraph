# Linting
GOLANGCI_LINT_VERSION=1.52.2

# Run test suite
.PHONY: test
test:
	go test -v ./...

# The linting gods must be obeyed
.PHONY: lint
lint: ./bin/$(GOLANGCI_LINT_VERSION)/golangci-lint
	./bin/$(GOLANGCI_LINT_VERSION)/golangci-lint run

./bin/$(GOLANGCI_LINT_VERSION)/golangci-lint:
	mkdir -p ./bin/$(GOLANGCI_LINT_VERSION)/
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./bin/$(GOLANGCI_LINT_VERSION) v$(GOLANGCI_LINT_VERSION)

