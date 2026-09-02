# PulsarPass

Sistema de reserva de ingressos para eventos de alta demanda (*flash sales*): milhares de usuários disputando estoque limitado no mesmo segundo, com retenção temporária de assento (TTL), pagamento assíncrono e **zero overbooking**.

> Blueprint completo: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) · Diagramas: [`docs/diagrams/`](docs/diagrams/) · Diário de ciclos: [`docs/CYCLES.md`](docs/CYCLES.md)

## Stack

Go 1.24+ · NATS JetStream · PostgreSQL · Redis · Docker Compose

## Serviços

| Serviço | Papel |
|---|---|
| `pulsar-gateway` | API HTTP de ingresso: validação, idempotência, rate limiting |
| `pulsar-core` | Estoque + máquina de estados da reserva (autoridade no Postgres) |
| `pulsar-chrono` | TTL worker: sweep de reservas expiradas |
| `pulsar-payment` | Processador de pagamento (acquirer simulado no MVP) |
| `pulsar-horizon` | Outbox relay: garantia de entrega dos eventos |

## Quickstart

Pré-requisitos: Go 1.24+, Docker Compose, [golang-migrate](https://github.com/golang-migrate/migrate) (opcional, para migrations).

```bash
# 1. Infra local (Postgres, Redis, NATS)
make compose-up

# 2. Migrations (bases pulsar_core e pulsar_payment)
make migrate-core-up
make migrate-payment-up

# 3. Workers e API (cada um em um terminal, ou com & )
make run-horizon    # relay do outbox → JetStream
make run-core       # estoque + máquina de estados
make run-payment    # acquirer simulado
make run-chrono     # sweeper de TTL
make run-gateway    # API :8080

# 4. Fluxo completo: reservar → pagar
RES=$(curl -s -X POST localhost:8080/v1/reservations \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-001" \
  -H "X-User-Id: user-1" \
  -d '{"event_id":"11111111-1111-1111-1111-111111111111","quantity":2}')
echo "$RES"
curl -s -X POST localhost:8080/v1/reservations/<id>/payment \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: pay-001" \
  -d '{"payment_method_token":"tok-1"}'
```

Health/readiness por serviço: `GET :9091..9095/healthz` e `/readyz`. Métricas Prometheus por serviço em `/metrics` nas mesmas portas. Sem NATS no ar em modo desenvolvimento, o gateway cai para bus in-memory (`BUS_MODE=memory` força isso).

## Observabilidade

Com `make compose-up` + os serviços rodando (`make run-*`):

- **Grafana** (`http://localhost:3000`, login anônimo) — dashboard "PulsarPass — Saga overview" provisionado com os cinco sinais do blueprint (p99 do gateway, esgotamento, lag das outboxes, idade de PENDING, DLQ) e o estado do acelerador Redis.
- **Prometheus** (`http://localhost:9090`) — coleta os 5 serviços via `/metrics` (host.docker.internal).
- **Jaeger** (`http://localhost:16686`) — tracing distribuído OTLP; exporte `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317` antes de subir os serviços para habilitar (unset = sem traces, métricas seguem). A saga aparece como um trace único: gateway → core → payment → core. Sob carga, limite o volume com `OTEL_TRACE_SAMPLE_RATIO` (ex. `0.1`).

## Prova de carga

Perfil de referência: 300 VUs de pico, ramp 2min + platô 3min + ramp-down 1min, p99 do ingress < 250ms. Os knobs são env no script (`deployments/k6/flash-sale.js`).

```bash
make compose-up                                    # Postgres, Redis, NATS (+ Jaeger/Prometheus/Grafana)
make migrate-core-up && make migrate-payment-up
make build

# recomendado sob carga: acquirer rápido e relay folgado
SIMULATED_CHARGE_DELAY=1ms make run-payment &
RELAY_BATCH=500 POLL_INTERVAL=200ms make run-horizon &
make run-core & make run-chrono & make run-gateway &

EVENT_ID=$(make load-seed CAPACITY=1000)           # semeia o evento e imprime o id
make load-run EVENT_ID=$EVENT_ID                   # suíte k6 (thresholds: p99, 0% falhas)
make load-verify                                   # invariantes de inventário: imprime 0 se sãos
```

Enquanto roda, acompanhe o dashboard "PulsarPass — Saga overview" no Grafana (`:3000`): `sold_out` dispara no pico, o backlog das outboxes precisa drenar a zero no ramp-down e o breaker não pode abrir sem Redis fora.

## Desenvolvimento

```bash
make fmt vet lint test   # qualidade (-race por padrão, paridade com o CI)
SKIP_E2E=1 make test     # iteração rápida sem Docker (pula a suíte testcontainers)
make test-cover          # cobertura, igual ao job de test do CI
make docker-build        # imagens dos 5 serviços
make compose-down        # infra
```

### Git conventions

- Branch names and commit messages **in English**, following [Conventional Commits](https://www.conventionalcommits.org) (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`).
- Branch naming: `feat/<scope>`, `fix/<scope>`, `chore/<scope>`, `docs/<scope>`.
- Atomic commits: one logical change per commit, tests included with the code they cover.
- Pull request descriptions are written in Portuguese and kept detailed (context, per-service changes, tests, trade-offs).

## CI & Releases

| Workflow | Gatilho | O que faz |
|---|---|---|
| `CI` | push em `main` e PRs | lint (golangci-lint), testes com race detector + coverage, build dos 5 binários, smoke de imagem Docker (matrix por serviço) e checagem do título do PR (Conventional Commits) |
| `Release` | tag `v*.*.*` | quality gate → GitHub Release com binários multi-arch + checksums (GoReleaser) → imagens Docker `linux/amd64` + `linux/arm64` publicadas no GHCR |

Cortar uma release:

```bash
git tag -a v0.1.0 -m "release v0.1.0"
git push origin v0.1.0        # dispara o workflow de Release
```

Imagens publicadas: `ghcr.io/adamsalves/pulsar-pass/<serviço>:{version,sha,latest}`. Metadados de build ficam embutidos nos binários (`pkg/version`) e expostos via header `X-Version` em `/healthz` e `/readyz`.

Validação local do release (sem publicar):

```bash
make release-check     # valida .goreleaser.yaml
make release-snapshot  # compila tudo em dist/ (snapshot 0.0.1-next)
```

## Layout

```
cmd/<serviço>/        # binários
internal/<serviço>/   # código privado (domain → application → adapters)
pkg/                  # libs compartilhadas (envelope, eventbus, health...)
migrations/           # SQL versionado por serviço
deployments/          # docker compose + Dockerfile
docs/                 # blueprint de arquitetura e diagramas
```

## Roadmap

Ciclo 0 (fundação) → adaptadores Postgres + NATS JetStream → saga ponta a ponta com testcontainers → observabilidade (OTel) → prova de carga (k6) → CI + releases ✅ → deploy/hardening. Detalhes em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#12-roadmap-de-ciclos) e [`docs/CYCLES.md`](docs/CYCLES.md) (o que foi feito em cada ciclo, com referências ao código).
