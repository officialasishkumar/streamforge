GO ?= go
APP_IMAGE ?= streamforge:local
CURRENT_RESULT ?= bench/results/current.json
K8S_VERSION ?= 1.30.0
K6_IMAGE ?= grafana/k6:0.50.0

.PHONY: build test integration-test lint e2e chaos bench perf-baseline perf-regression docker-build helm-lint

build:
	$(GO) build ./...

test:
	$(GO) test -race -coverprofile=coverage.out ./...

integration-test:
	$(GO) test -tags=integration ./...

lint:
	golangci-lint run

e2e:
	docker compose config >/dev/null
	$(GO) test -run TestEventJSONRoundTrip ./internal/types

chaos:
	bash chaos/run_all.sh

bench:
	docker run --rm --network host -v "$(PWD)/bench/k6:/scripts:ro" $(K6_IMAGE) run --duration 1m --vus 50 /scripts/ingest_baseline.js

perf-baseline:
	test -f "$(CURRENT_RESULT)"
	python3 bench/compare.py bench/results/baseline.json "$(CURRENT_RESULT)"

perf-regression:
	test -f "$(CURRENT_RESULT)"
	python3 bench/compare.py bench/results/baseline.json "$(CURRENT_RESULT)"

docker-build:
	docker build -t $(APP_IMAGE) .

helm-lint:
	helm lint deploy/helm/streamforge
	helm template streamforge deploy/helm/streamforge > rendered.yaml
	@if command -v kubeconform >/dev/null 2>&1; then \
		kubeconform -kubernetes-version "$(K8S_VERSION)" rendered.yaml; \
	else \
		echo "kubeconform not found; rendered Helm manifests but skipped schema validation"; \
	fi
