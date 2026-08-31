# PulsarPass

Sistema de reserva de ingressos para eventos de alta demanda (*flash sales*): milhares de usuários disputando estoque limitado no mesmo segundo, com retenção temporária de assento (TTL), pagamento assíncrono e **zero overbooking**.

> Blueprint completo: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) · Diagramas: [`docs/diagrams/`](docs/diagrams/)

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

# 3. Buildar tudo
make build

# 4. Subir o gateway (loopback, in-memory bus no ciclo 0)
make run-gateway

# 5. Smoke test
curl -i -X POST localhost:8080/v1/reservations \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-001" \
  -d '{"event_id":"11111111-1111-1111-1111-111111111111","quantity":2}'
```

Health/readiness por serviço: `GET :9091..9095/healthz` e `/readyz`.

## Desenvolvimento

```bash
make fmt vet lint test   # qualidade
make docker-build        # imagens dos 5 serviços
make compose-down        # infra
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

Ciclo 0 (esta base) → adaptadores Postgres + NATS JetStream → saga ponta a ponta com testcontainers → observabilidade (OTel) → prova de carga (k6) → CI/CD. Detalhes em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#12-roadmap-de-ciclos).
