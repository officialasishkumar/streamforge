GO ?= go

.PHONY: build test integration-test lint e2e chaos bench perf-baseline perf-regression docker-build helm-lint

build:
	$(GO) build ./...

test:
	$(GO) test ./...

integration-test:
	$(GO) test -tags=integration ./...

lint:
	golangci-lint run

e2e:
	@echo "e2e target not yet implemented"
	@exit 0

chaos:
	@echo "chaos target not yet implemented"
	@exit 0

bench:
	@echo "bench target not yet implemented"
	@exit 0

perf-baseline:
	@echo "perf-baseline target not yet implemented"
	@exit 0

perf-regression:
	@echo "perf-regression target not yet implemented"
	@exit 0

docker-build:
	@echo "docker-build target not yet implemented"
	@exit 0

helm-lint:
	@echo "helm-lint target not yet implemented"
	@exit 0
