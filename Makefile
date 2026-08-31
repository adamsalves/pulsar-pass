GO ?= go
SERVICES := pulsar-gateway pulsar-core pulsar-chrono pulsar-payment pulsar-horizon

CORE_DB      ?= postgres://pulsar:pulsar@localhost:5432/pulsar_core?sslmode=disable
PAYMENT_DB   ?= postgres://pulsar:pulsar@localhost:5432/pulsar_payment?sslmode=disable

.PHONY: all build run-gateway run-core run-chrono run-payment run-horizon vet fmt lint test tidy migrate-core-up migrate-core-down migrate-payment-up migrate-payment-down compose-up compose-down docker-build release-check release-build release-snapshot clean

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

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint is not installed"; exit 1; }
	golangci-lint run

test:
	$(GO) test ./...

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
