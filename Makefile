GO ?= go
SERVICES := pulsar-gateway pulsar-core pulsar-chrono pulsar-payment pulsar-horizon
K6_IMAGE := grafana/k6:1.4.0
KIND_CLUSTER ?= pulsarpass
PROM_CHART_VERSION := 29.27.0
GRAFANA_CHART_VERSION := 10.5.15

CORE_DB      ?= postgres://pulsar:pulsar@localhost:5432/pulsar_core?sslmode=disable
PAYMENT_DB   ?= postgres://pulsar:pulsar@localhost:5432/pulsar_payment?sslmode=disable

# Load-test knobs (see deployments/k6/flash-sale.js).
CAPACITY     ?= 1000
EVENT_ID     ?=
BASE_URL     ?= http://localhost:8080

.PHONY: all build run-gateway run-core run-chrono run-payment run-horizon vet fmt lint test test-cover tidy migrate-core-up migrate-core-down migrate-payment-up migrate-payment-down compose-up compose-down docker-build load-seed load-run load-verify cluster-up cluster-down deploy-infra deploy-services release-check release-build release-snapshot clean

all: lint build test

build:
	@mkdir -p bin
	@for svc in $(SERVICES); do \
		$(GO) build -o bin/$$svc ./cmd/$$svc || exit 1; \
	done
	@echo "binaries: $(SERVICES) -> bin/"

run-gateway:
	$(GO) run ./cmd/pulsar-gateway

run-core:
	$(GO) run ./cmd/pulsar-core

run-chrono:
	$(GO) run ./cmd/pulsar-chrono

run-payment:
	$(GO) run ./cmd/pulsar-payment

run-horizon:
	$(GO) run ./cmd/pulsar-horizon

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint is not installed"; exit 1; }
	golangci-lint run

# Parity with the CI test job: the race detector runs by default.
# SKIP_E2E=1 skips the testcontainers suite for a faster local loop.
test:
	$(GO) test -race ./...

test-cover:
	$(GO) test -race -covermode=atomic -coverprofile=coverage.out ./...

tidy:
	$(GO) mod tidy

migrate-core-up:
	migrate -path migrations/core -database "$(CORE_DB)" up

migrate-core-down:
	migrate -path migrations/core -database "$(CORE_DB)" down 1

migrate-payment-up:
	migrate -path migrations/payment -database "$(PAYMENT_DB)" up

migrate-payment-down:
	migrate -path migrations/payment -database "$(PAYMENT_DB)" down 1

compose-up:
	docker compose -f deployments/docker-compose.yml up -d

compose-down:
	docker compose -f deployments/docker-compose.yml down

docker-build:
	@for svc in $(SERVICES); do \
		docker build -f deployments/docker/Dockerfile --build-arg SVC=$$svc -t pulsarpass/$$svc:dev . || exit 1; \
	done

# Load test: compose-up + the five `make run-*` services must be up.
# load-seed prints the EVENT_ID; pass it back to load-run.load-seed:
	@docker compose -f deployments/docker-compose.yml exec -T postgres \
		psql -U pulsar -d pulsar_core -q -tA -v capacity=$(CAPACITY) -f - < deployments/k6/seed.sql

load-run:
	@if [ -z "$(EVENT_ID)" ]; then echo "EVENT_ID is required (see make load-seed)"; exit 1; fi
	docker run --rm --network host \
		-v $(CURDIR)/deployments/k6:/scripts:ro \
		$(K6_IMAGE) run /scripts/flash-sale.js \
		-e BASE_URL=$(BASE_URL) -e EVENT_ID=$(EVENT_ID)

# Inventory invariants after a run; prints the violation count (0 = sound).
load-verify:
	@docker compose -f deployments/docker-compose.yml exec -T postgres \
		psql -U pulsar -d pulsar_core -tA -f - < deployments/k6/verify.sql

# kind cluster for the deploy (cycle 6): cluster-up creates it with the
# host port mappings; deploy-infra provisions the data stores (manifests)
# and the observability stack (pinned upstream charts).
cluster-up:
	kind create cluster --name $(KIND_CLUSTER) --config deployments/kind/config.yaml

cluster-down:
	kind delete cluster --name $(KIND_CLUSTER)

deploy-infra:
	kubectl apply -f deployments/cluster/
	helm upgrade --install prometheus prometheus-community/prometheus \
		--version $(PROM_CHART_VERSION) -n monitoring --create-namespace \
		-f deployments/cluster/helm/prometheus-values.yaml --wait --timeout 5m
	helm upgrade --install grafana grafana/grafana \
		--version $(GRAFANA_CHART_VERSION) -n monitoring --create-namespace \
		-f deployments/cluster/helm/grafana-values.yaml --wait --timeout 5m
	@echo "infra up: postgres/redis/nats in pulsarpass, prometheus/grafana/jaeger in monitoring"

# Services from the release pipeline images (GHCR). For a local smoke,
# build dev images, load them into kind and pass IMAGE_TAG=dev:
#   make docker-build && kind load docker-image pulsarpass/<svc>:dev --name pulsarpass
#   make deploy-services IMAGE_TAG=dev
IMAGE_TAG ?= latest

deploy-services:
	helm upgrade --install pulsar-pass deployments/helm/pulsar-pass \
		-n pulsarpass --create-namespace \
		--set image.tag=$(IMAGE_TAG) --wait --timeout 5m

release-check:
	@command -v goreleaser >/dev/null 2>&1 || { echo "goreleaser is not installed"; exit 1; }
	goreleaser check

release-build:
	@command -v goreleaser >/dev/null 2>&1 || { echo "goreleaser is not installed"; exit 1; }
	goreleaser build --snapshot --clean

release-snapshot:
	@command -v goreleaser >/dev/null 2>&1 || { echo "goreleaser is not installed"; exit 1; }
	goreleaser release --snapshot --clean

clean:
	rm -rf bin dist
