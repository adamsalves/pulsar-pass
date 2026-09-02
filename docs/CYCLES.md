# PulsarPass — Diário de Ciclos

Registro **ciclo a ciclo** do que foi construído, com referências diretas ao código (arquivo · símbolo) e aos commits. Cada ciclo tem: objetivo, entregas por serviço, garantias implementadas, testes e commits.

> **Como manter este documento:** ao encerrar um ciclo, adicione uma nova seção seguindo o template do final deste arquivo (objetivo → entregas com referências de código → testes/garantias → commits). O documento é o retrato *state of the art* do projeto: a seção mais recente descreve o estado atual.

## Índice

| Ciclo | Tema | Status |
|---|---|---|
| [0 — Fundação](#ciclo-0--fundação) | Blueprint, esqueleto dos 5 serviços, migrations, infra local | ✅ concluído |
| [1 — Adaptadores reais](#ciclo-1--adaptadores-reais) | Postgres (capacidade atômica, outbox, dedup) + NATS JetStream | ✅ concluído |
| [1½ — Hardening de consistência](#ciclo-1½--hardening-de-consistência) | Correções de idempotência, atomicidade e pricing pós-Ciclo 1 | ✅ concluído |
| [5 — CI + Release (antecipado)](#ciclo-5--ci--release-antecipado) | GitHub Actions, GoReleaser, GHCR multi-arch, dependabot | ✅ concluído |
| [2 — Saga ponta a ponta](#ciclo-2--saga-ponta-a-ponta) | Saga e2e com testcontainers + acelerador Redis | ✅ concluído |
| [2½ — Hardening pós-Ciclo 2](#ciclo-2½--hardening-pós-ciclo-2) | Posse da reserva no payment, `-race` no `make test`, circuit breaker do hold | ✅ concluído |
| [3 — Observabilidade](#ciclo-3--observabilidade) | OTel + métricas Prometheus + traces OTLP + dashboards | ✅ concluído |
| [4 — Prova de carga](#ciclo-4--prova-de-carga) | k6 a 300 VUs: p99, zero overbooking e hardening do broker | ✅ concluído |

---

## Ciclo 0 — Fundação

**Objetivo:** definir a arquitetura e erguer o esqueleto completo dos 5 serviços, migraciones e infraestrutura local — apenas stdlib (ADR-7), com portas prontas para os adaptadores reais.

**Commits:** `ddc3e39` → `ea18730`

### Entregas

**Blueprint e docs**
- `docs/ARCHITECTURE.md` — blueprint completo (garantias, ADRs 1–7, topologia JetStream, fluxos, máquina de estados, contrato de eventos, roadmap de ciclos).
- `docs/diagrams/` — `success-flow.mmd`, `timeout-compensation.mmd`, `reservation-state-machine.mmd`, `component-topology.mmd`.

**Plataforma compartilhada (`pkg/`)**
- `pkg/config/config.go` — leitura de env com fallbacks (`String`, `Int`, `Duration`), padrão usado por todos os serviços.
- `pkg/logger/logger.go` — `New(env)`: `log/slog` text em development, JSON em production.
- `pkg/health/health.go` — `Server` com `/healthz` (liveness) e `/readyz` (readiness, `SetReady`), porta por serviço `:9091–:9095`.
- `pkg/uid/uid.go` — geração/validação de UUID v4 (`New`, `IsValid`).
- `pkg/envelope/envelope.go` — envelope versionado único (`Envelope[T]`, `New`, `SubjectFor`) e constantes de `event_type`/subject — fonte única de contratos do monorepo.
- `pkg/eventbus/eventbus.go` — portas `Publisher`/`Subscriber` + implementação `Memory` (in-memory, usada em dev e testes).

**Domínio e aplicação (`internal/core/`)**
- `internal/core/domain/reservation.go` — máquina de estados `PENDING → CONFIRMED / EXPIRED / FAILED / CANCELLED`; transições inválidas retornam erro (`Confirm`, `Expire`, `MarkFailed`, `Cancel`) — base da idempotência por reentrega.
- `internal/core/domain/event.go` — `Event.Reserve/Release/ConfirmSold/SaleIsOpen` (guardas de capacidade e janela de venda no domínio).
- `internal/core/domain/ticket.go` — ciclo de vida do ingresso (`AVAILABLE → RESERVED → SOLD`).
- `internal/core/application/ports.go` — portas `ReservationRepository`, `InventoryRepository`, `OutboxRepository`, `TxRunner`, `Clock` (Clean Architecture: domínio sem I/O).
- `internal/core/application/service.go` — `ReservationService` com casos de uso `Reserve`, `Confirm`, `Expire`, `Fail`, `Cancel` (esqueleto; versão transacional no Ciclo 1).

**Esqueletos dos serviços**
- `internal/gateway/` — HTTP ingress: `middleware.go` (`RequestID`, `Logging`, `Recover`), `routes.go` (método + path params na stdlib), `handler.go` (POST `/v1/reservations`, GET, POST `/v1/reservations/{id}/payment`), `server.go` (shutdown graceful).
- `internal/chrono/sweeper.go` — `Sweeper` com porta `ReservationSource` (`FindExpired`) e payload `reservation.expired`.
- `internal/payment/processor.go` + `ports.go` — `Processor` e porta `Acquirer` (`ChargeRequest`/`ChargeResult`).
- `internal/horizon/relay.go` — `Relay` com porta `OutboxStore` (`FetchBatch`/`MarkProcessed`).
- `cmd/<serviço>/main.go` — wiring inicial dos 5 binários com signal handling.

**Dados e infra**
- `migrations/core/000001_init.up.sql` — `events` (capacidade + `reserved_count`/`sold_count`), `reservations` (índice único parcial `reservations_one_active_per_user`, índice `expires_at` para PENDING), `tickets`, `outbox_events` (índice parcial `unprocessed`), `processed_messages` (dedup por consumidor).
- `migrations/payment/000001_init.up.sql` — `payments` (idempotency key única), `outbox_events`, `processed_messages`.
- `deployments/docker-compose.yml` — Postgres (2 bases), Redis, NATS; `deployments/Dockerfile` multi-stage.
- `Makefile` — targets `build/vet/fmt/lint/test`, `run-*` por serviço, `migrate-*-up/down`, `compose-up/down`.
- `README.md` — quickstart e convenções.

### Marca deixada
Esqueleto rodável ponta a ponta em modo dev (bus in-memory, Postgres real), com todas as portas já nomeadas conforme o blueprint — os adaptadores do Ciclo 1 só implementaram interfaces existentes.

---

## Ciclo 1 — Adaptadores reais

**Objetivo:** trocar os esqueletos por adaptadores reais: PostgreSQL como autoridade de inventário (zero overbooking), outbox transacional nas duas bases e NATS JetStream com dedup, consumidores durables e DLQ.

**Commits:** `00f2b9a` → `1ebdf2f`

### Entregas

**Fundação de dados (`pkg/`)**
- `pkg/pgpool/pgpool.go` — pool pgx com defaults saneados (`New`, `Options`).
- `pkg/pgtx/pgtx.go` — propagação de transação por contexto: `Manager.WithinTx`, `QuerierFrom`, `TxFromContext`. Padrão-chave: adapters compartilham a mesma transação sem conhecer quem a abriu.
- deps: `pgx/v5`, `nats.go`, `testcontainers-go` (`00f2b9a`).

**Core — use cases transacionais**
- `internal/core/application/service.go` — `reserve` roda dentro de `inTx`: valida ID do cliente como UUID (`pkg/uid.IsValid`), preço derivado do evento (nunca do cliente), `Inventory.ReserveCapacity` + `Reservations.Create` + `Outbox.Enqueue` na mesma transação (`727eeef`, `0547846`).
- `internal/core/application/events.go` — payloads `TicketReserved/TicketConfirmed/TicketReleased` (contrato consumido pelo payment).

**Core — adaptadores Postgres**
- `internal/core/adapter/postgres/inventory.go` — `ReserveCapacity`: `UPDATE ... WHERE reserved_count + sold_count + $n <= capacity` (0 linhas = esgotado → rejeição imediata, zero overbooking); `ReleaseCapacity`/`CommitSold` espelham a máquina de estados.
- `internal/core/adapter/postgres/reservations.go` — `Create/Get/Update` com lock otimista (`version`) e mapeamento de unique violation.
- `internal/core/adapter/postgres/outbox.go` — `Enqueue` escreve na `outbox_events` usando a transação do contexto (`pgtx`).
- Testes de integração com testcontainers: `inventory_test.go` (`TestReserveCapacityZeroOverbookingUnderConcurrency` — race de capacidade sob goroutines concorrentes), `outbox_test.go` (`TestOutboxEnqueueRollsBackWithTransaction` — atomicidade estado+evento), `reservations_test.go`.

**Event bus real (`pkg/eventbus/`)**
- `pkg/eventbus/jetstream.go` — `ConnectJetStream` (streams `RESERVATIONS`/`PAYMENTS`), `Publish` com `Nats-Msg-Id` (dedup server-side), `Subscribe` com durable + queue group, `consumeLoop` com ack/nak, `listenDLQ` para advisories de `max_deliver` esgotado (`53595ec`).
- `internal/broker/broker.go` — `Streams()` (spec declarativa dos streams) e `Connect()` compartilhado.
- Testes com servidor NATS embutido (`jetstream_test.go`): roundtrip, dedup por message id, redelivery em falha de handler.

**Wiring por serviço**
- `cmd/pulsar-core/main.go` — assina `reservation.reserve`, `payment.succeeded`, `payment.failed`, `reservation.expired` (durables `core-*`) e instancia os adapters Postgres (`deae181`).
- `internal/gateway/` — publica comandos via JetStream; fallback in-memory quando NATS indisponível em dev (`BUS_MODE=memory`) (`49df5ca`).
- `internal/payment/` — `adapter/postgres/payments.go` (`Create`, `GetByIdempotencyKey`, `UpdateStatus`), `contexts.go` (`reservation_context` upsert/get), `outbox.go`; `migrations/payment/000002_reservation_context.up.sql` (contexto do pedido: `amount_cents`, `currency`, `expires_at`); `cmd/pulsar-payment/main.go` projeta `ticket.reserved` → `reservation_context` (durable `payment-context`) e consome `payment.process` (`d9b03d2`).
- `internal/payment/simulator.go` — `SimulatedAcquirer` (MVP: aprova/recusa deterministicamente) + wiring JetStream (`d8bf41a`).
- `internal/payment/processor.go` — fluxo completo: idempotency key → pagamento existente ou novo, checagem da janela TTL, charge no acquirer, outcome no outbox.
- `internal/chrono/adapter/postgres/source.go` — `FindExpired` com `FOR UPDATE SKIP LOCKED LIMIT $1` sobre `reservations` PENDING vencidas; `cmd/pulsar-chrono/main.go` publica `reservation.expired` (`705c754`).
- `internal/horizon/adapter/postgres/store.go` — `FetchBatch` (SKIP LOCKED) e `MarkProcessed`; relay dual-database (outbox do core **e** do payment na mesma instância, `internal/horizon/config.go` com dois DSNs) (`85dd0d5`).

**Consolidação**
- `814d118` — blueprint/README/Makefile atualizados p/ ciclo 1 + `.env.example`.
- `1ebdf2f` — convenções de git (branch/commit em inglês, Conventional Commits, PRs em PT).
- `5bb7b61` — errcheck em batch closes + pin do nats-server p/ Go 1.25.

### Garantias implementadas
| Garantia | Onde vive |
|---|---|
| Zero overbooking | `inventory.go` (UPDATE condicional) + teste de concorrência |
| Outbox transacional | `outbox.go` (core e payment) + relay dual-DB |
| Dedup de mensagens | `Nats-Msg-Id` no JetStream + `processed_messages` |
| DLQ | advisory listener em `jetstream.go` |
| TTL sem leak | sweeper com `SKIP LOCKED` em `source.go` |

---

## Ciclo 1½ — Hardening de consistência

**Objetivo:** fechar furos de consistência descobertos revisando o fluxo ponta a ponta do Ciclo 1 — pricing, idempotência e atomicidade de outcomes.

**Commits:** `56050d2` → `1c45185`

### Correções

1. **Pricing só do contexto** (`56050d2`) — o valor cobrado vem exclusivamente de `reservation_context.amount_cents` (escrito pelo core via `ticket.reserved`); payload do comando não influencia o charge. Testes: `processor_test.go` (`TestHandleApproved`, `TestHandleDeclined`).
2. **Outcomes atômicos e idempotency keys resumíveis** (`0eda780`) — `Processor.finish` grava status do pagamento + evento de outcome **na mesma transação** (`TestFinishWritesStatusAndOutboxInTheSameTransaction`); reentrega de `payment.process` com chave já registrada **retoma** o charge pendente (`TestHandleRedeliveryResumesPendingCharge`) e republica o outcome de pagamento já falhado (`TestHandleRedeliveryOfFailedPaymentRepublishesOutcome`).
3. **Consumidores idempotentes e confirmação tardia honrada** (`7446237`) — no core, replay de `Confirm`/`Expire`/`Fail` em reserva já finalizada é no-op com sucesso (a máquina de estados do domínio rejeita a transição, o handler trata como já-processado: `TestConfirmReplayIsIdempotent`, `TestReleaseReplayIsIdempotent`); `payment.succeeded` que chega após o vencimento porém antes do sweep ainda confirma (`TestConfirmAfterExpiryIsHonored`).
4. **Falhas de publicação visíveis no relay** (`d46cde0`) — `internal/horizon/relay.go` loga erro de publish (antes era engolido) e a janela de dedup do stream foi documentada explicitamente.
5. **Pricing explícito no schema** (`1c45185`) — `migrations/core/000002_event_price.up.sql`: `price_cents BIGINT NOT NULL CHECK (price_cents > 0)` **sem default**; impossibilita evento a preço zero, o que violaria o `CHECK (amount_cents > 0)` dos payments.

---

## Ciclo 5 — CI + Release (antecipado)

**Objetivo:** pipeline de qualidade e releases reproduzíveis antes de seguir para o Ciclo 2 — roadmap item 5 entregue fora de ordem de propósito: CI verde sustenta os ciclos seguintes.

**Commits:** `199c09e` → `462b3f8`

### Entregas

**CI (`.github/workflows/ci.yml`, `199c09e`)**
- `lint` — golangci-lint (config no bootstrap do Ciclo 0).
- `test` — `make test` com race detector + artefato de coverage.
- `build` — compila os 5 binários.
- `docker` — smoke de imagem por serviço (matrix `pulsar-gateway|core|chrono|payment|horizon`).
- `pr-title` — enforcement de Conventional Commits no título do PR.
- Concurrency group por ref (cancela runs redundantes).

**Dependabot (`.github/dependabot.yml`, `db24cb4`)** — go_modules, actions e docker, com regra para **manter nats-server pinado em 2.12** (compat Go 1.25, `cad6aed`).

**Versionamento (`pkg/version/version.go`, `ee41367`, `f965e3f`)**
- Versão injetada via `-ldflags` no build; exposta no log de startup e no header `X-Version` de `/healthz` e `/readyz` (`pkg/health/health.go` — `SetVersion` com `atomic.Pointer`, thread-safe).

**Release (`.goreleaser.yaml`, `.github/workflows/release.yml`, `5ad65a4`, `36c1b75`)**
- Gatilho: tag `v*.*.*` → quality gate (lint+test) → GitHub Release com binários multi-arch + checksums (GoReleaser) → imagens `linux/amd64`+`linux/arm64` publicadas no GHCR (`ghcr.io/adamsalves/pulsar-pass/<svc>:{version,sha,latest}`).
- `Makefile`: `release-check`, `release-build`, `release-snapshot` para validação local.
- Docker: versão injetada nas imagens e toolchain unificada em Go 1.25 (`ad39671`, `c261452`); `Dockerfile` passa a copiar `go.sum` (builds resolvem checksums, `1dcb51d`).
- Docs: seção "CI & Releases" no README (`ccc34e1`).

**Pós-release (estabilização)**
- `da24555`/`25bc83f` — sincronização da flag de falha do handler no teste de redelivery do JetStream (race no teste, não no produto).
- `cad6aed` — re-pin nats-server 2.12 + dependabot ensinado a preservar o pin.
- Bumps de actions via dependabot (`88d9a61`, `f7e9fe4`, `c07e9c9`, `de245f9`, `28f8b48`, `368ccc4`).

---

## Ciclo 2 — Saga ponta a ponta

**Objetivo:** validar a saga completa (gateway → core → payment → core) contra infraestrutura real — sucesso, recusa e expiração — e implementar o acelerador Redis (`hold:{reservation_id}`) com degradação graciosa, mantendo o PostgreSQL como autoridade.

**Commits:** `f849b59` → `e0fc9b0`

### Entregas

**Acelerador Redis (`internal/holds/`)**
- `internal/holds/holds.go` — `Store` com `Set`/`Release`/`Exists`: grava `hold:{id}` com o TTL restante, remove no estado terminal. Degradação graciosa por construção: erro do Redis vira log de warning e o fluxo segue (`degraded`); **cada operação é limitada a 200ms** (`opTimeout`) — um Redis fora custa no máximo isso por chamada no hot path; `REDIS_ADDR=` (vazio) desliga o acelerador de verdade via `config.StringAllowEmpty` (distingue unset de vazio — o fallback de `config.String` tornava o kill switch inalcançável); `REDIS_ADDR` inalcançável degrada com warnings. Testes cobrem ciclo de vida, TTL não positivo, store desabilitado e Redis inalcançável dentro do cap (`holds_test.go`).
- `internal/core/application/ports.go` — porta `HoldStore` (`Set`/`Release`), documentada como acelerador opcional; PostgreSQL permanece autoridade.
- `internal/core/application/service.go` — `NewReservationService` recebe o hold store opcional (nil desliga). `Reserve` grava o hold **após o commit** (`Reserve`), `Confirm`/`Fail`/`Expire`/`Cancel` removem via `releaseHold` — cleanup nunca bloqueia nem falha a compensação.
- `internal/chrono/sweeper.go` — porta `HoldCleaner`; o sweep publica **todos** os eventos `reservation.expired` do lote e só então faz `DEL hold:{id}` (higiene do diagrama §5.2; a chave expira sozinha de qualquer forma) — Redis lento pode atrasar a limpeza da chave, nunca a liberação dos assentos.
- Wiring: `cmd/pulsar-core/main.go` e `cmd/pulsar-chrono/main.go` instanciam o store; `internal/chrono/config.go` ganha `REDIS_ADDR`; `pkg/config` ganha `StringAllowEmpty` (testado em `config_test.go`).

**Consumidores extraídos para reuso (refactor)**
- `internal/core/subscribers.go` — `Subscribers.Register`: os 4 consumidores do core (`core-reserve`, `core-payment-succeeded`, `core-payment-failed`, `core-reservation-expired`) saem do `main.go`; decode + use case + log agora são código de biblioteca.
- `internal/payment/subscribers.go` — `Subscribers.Register`: projeção `payment-context` e comando `payment-process` idem.
- Mains (`cmd/pulsar-core`, `cmd/pulsar-payment`) ficam só com wiring de infraestrutura; a suíte e2e registra **exatamente os mesmos consumidores** da produção.

**Saga e2e com testcontainers (`internal/e2e/`)**
- `internal/e2e/harness_test.go` — `boot` monta a topologia completa: Postgres real com as duas bases (`pulsar_core_e2e` + `pulsar_payment_e2e`) e migrations aplicadas, Redis real, NATS JetStream embutido (padrão já usado em `pkg/eventbus/jetstream_test.go`), gateway HTTP em `httptest`, relay do horizon nas duas outboxes e sweeper do chrono rodando de verdade. Só o acquirer é simulado (`SimulatedAcquirer` com taxa de falha 0; token `fail-me` força recusa).
- `internal/e2e/saga_test.go` — três caminhos, todos via HTTP do gateway:
  - `TestSagaSuccessPath` — reserva → projeção do contexto (pricing `2 × price`) → hold no Redis → charge aprovado → `CONFIRMED`, `reserved_count`/`sold_count` corretos, hold liberado e as duas outboxes drenadas.
  - `TestSagaPaymentDeclined` — charge recusado → `FAILED`, assento de volta ao pool, `payment` reprovado e outbox do core drenada.
  - `TestSagaExpirationTTL` — TTL vence naturalmente (sem força bruta no banco, que dessincronizaria a projeção do payment) → sweeper expira, hold limpo e pagamento tardio rejeitado pela checagem da janela.

### Garantias implementadas
| Garantia | Onde vive |
|---|---|
| Hold é acelerador, nunca fonte da verdade | `internal/holds/holds.go` (degradação embutida, 200ms/op) + porta `HoldStore` opcional |
| Hold fora da transação | `Reserve`/`releaseHold` em `service.go` chamam após o commit do `inTx` |
| Redis nunca atrasa compensação | `sweep` publica os eventos antes da higiene dos holds (`sweeper.go`) |
| Sem assento vazado mesmo com Redis morto | sweeper do Postgres (Ciclo 1) segue como rede de segurança; Redis só acelera |
| Saga e2e executada de verdade | `internal/e2e/` com os mesmos consumidores dos binários |

### Testes
- `internal/holds/holds_test.go` — 4 testes (lifecycle, TTL inválido, desabilitado, Redis inalcançável).
- `internal/e2e/saga_test.go` — 3 testes ponta a ponta com testcontainers (sucesso, recusa, expiração), validando estado no Postgres (core e payment), chaves no Redis e drenagem das outboxes.

---

## Ciclo 2½ — Hardening pós-Ciclo 2

**Objetivo:** fechar os follow-ups do review do PR #14 — autorização de posse no payment, paridade do race detector entre loop local e CI, e custo de um outage contínuo do acelerador Redis.

**Commits:** `cd87a42` → `f48bf81`

### Entregas

**Posse da reserva no payment (#15, PR #19)**
- `internal/payment/processor.go` — `Handle` compara `req.UserID` com `ctxData.UserID` (fonte autoritativa: projeção do `ticket.reserved`) e rejeita com o sentinel `ErrNotOwner` **antes de qualquer write**; `user_id` passa a ser obrigatório e o fallback anônimo do `loadOrCreate` (vazio → dono) foi removido — era bypass silencioso. Decisão de design: rejeição *side-effect free* — sem payment row, sem `payment.failed` (um impostor não pode liberar a reserva da vítima); o comando NAKado esgota `MaxDeliver` e cai na DLQ, dando visibilidade do ataque.
- `internal/e2e/harness_test.go` — `createReservation` retorna o `X-User-Id` criado; `payReservation` recebe o pagador (o harness deixou de codificar o cruzamento de usuários); helper `paymentCount`.
- `internal/e2e/saga_test.go` — `TestSagaPaymentByNonOwnerRejected`: impostor tenta pagar → reserva permanece PENDING com zero payments; dono paga depois → CONFIRMED.

**Paridade de race detector (#17, PR #20)**
- `Makefile` — `test` roda `-race` por padrão (paridade com o job de test do CI); `test-cover` espelha o job exato (`-covermode=atomic -coverprofile`).
- `internal/e2e/harness_test.go` — `SKIP_E2E=1` pula a suíte testcontainers via `t.Skip` em `bootTTL` — sem build tag, os testes seguem rodando por padrão e o CI não muda. Custo medido: e2e sob `-race` ~24s local.
- `README.md` — seção de desenvolvimento com os três caminhos (`test`, `SKIP_E2E=1`, `test-cover`).

**Circuit breaker do hold Redis (#16, PR #21)**
- `internal/holds/holds.go` — breaker interno: após `defaultFailureThreshold` (5) falhas consecutivas abre por `defaultCooldown` (30s) e toda operação short-circuita **sem round trip**; a primeira operação pós-cooldown é a sonda half-open (`allow`/`success`/`failure`) — sucesso fecha, falha rearma a janela. Logs apenas nas transições (`degraded` por falha enquanto fechado, limitado ao threshold; um anúncio de abertura; um recovery) — fim do spam por operação. `WithBreaker(threshold, cooldown)` como option variádica de `New`; call sites existentes (`cmd/pulsar-core`, `cmd/pulsar-chrono`, e2e) seguem nos defaults. Postgres permanece autoridade: breaker aberto é miss de acelerador, nunca falha do fluxo.
- Follow-ups registrados: #22 (métricas de degradação — Ciclo 3) e #23 (identidade real no gateway; o fallback `guest` contorna a posse — hardening).

### Garantias implementadas
| Garantia | Onde vive |
|---|---|
| Só o dono paga a reserva | `processor.go` (`ErrNotOwner` antes de qualquer write) + `TestSagaPaymentByNonOwnerRejected` |
| Comando anônimo não assume o dono | `user_id` obrigatório em `Handle` (`TestHandleMissingUserRejected`) |
| Race detectada no loop local | `Makefile` `test` com `-race` (paridade com o CI) |
| Outage de Redis custa ~zero | breaker em `holds.go` (short-circuit + sonda half-open única) |
| Degradation visível sem spam | logs só nas transições do breaker (`degraded`/`recovered`) |

### Testes
- `internal/payment/processor_test.go` — `TestHandleNonOwnerRejected` (sem charge/row/outbox), `TestHandleMissingUserRejected`; testes existentes atualizados ao contrato novo do comando.
- `internal/e2e/saga_test.go` — `TestSagaPaymentByNonOwnerRejected` (impostor sem efeito colateral; dono confirma em seguida).
- `internal/holds/holds_test.go` — 5 testes novos: short-circuit sem spam (`TestStoreBreakerShortCircuitsAfterConsecutiveFailures`), rearme da sonda (`TestStoreBreakerProbeFailureRearmsCooldown`), recovery (`TestStoreBreakerRecoversOnSuccessfulProbe`), concorrência 16 goroutines sob `-race` (`TestStoreBreakerUnderConcurrency`), store desabilitado (`TestStoreDisabledIgnoresBreakerOptions`).

---

## Ciclo 3 — Observabilidade

**Objetivo:** entregar o §9 do blueprint — métricas Prometheus com os cinco sinais nomeados (p99 do gateway, esgotamento, lag do relay, idade de PENDING, DLQ), tracing OTLP com propagação ponta a ponta e dashboards provisionados — fechando a #22.

**Commits:** `5f49afe` → `0a11c73`

### Entregas

**Fundação (ADR-8 — OTel SDK + exporter Prometheus)**
- `pkg/metrics/metrics.go` — `Init(ctx, service)`: provider global de métricas (registry Prometheus própria, resource com service name/version, runtime Go) + provider de traces OTLP quando `OTEL_EXPORTER_OTLP_ENDPOINT` existe (gRPC lazy, `ParentBased(AlwaysSample)`, W3C tracecontext). Rollback do init parcial em qualquer falha; duplo `Init` rejeitado.
- `pkg/health/health.go` — `Mount(pattern, handler)`: cada serviço expõe `/metrics` na porta de health que já roda (9091–95).
- Padrão de binding: instrumentos por pacote criados **lazily** contra o provider vigente no primeiro uso — binários emitem após `Init`; e2e/testes ligam no no-op sem custo e sem mudar assinaturas. Tracers resolvem **por chamada** (o `sync.Once` viciava o processo com o provider no-op de um teste anterior).

**Métricas — os cinco sinais + acelerador**
- `internal/gateway/middleware.go` — `Metrics`: requests por rota templateada (cardinalidade limitada), método e status + histograma de duração (p99).
- `internal/core/metrics.go` + `subscribers.go` — `pulsar_core_reservations_total{op,outcome}`: `sold_out` com label próprio (sinal de esgotamento do blueprint), `sale_not_open`/`invalid`/`error` separados.
- `internal/payment/metrics.go` — `pulsar_payment_charges_total{outcome}`: `approved`/`declined` (match por prefixo — o simulador retorna "card declined (forced by token)")/`window_elapsed`/`acquirer_error`.
- `internal/chrono/metrics.go` + `adapter/postgres/source.go` — sweeps, expirações publicadas (contagem preservada em falha mid-batch) e `pulsar_chrono_pending_max_age_seconds` via novo `PendingAgeSource` opcional; gauge regravado a todo tick (last-value store congelava no máximo antigo).
- `internal/horizon/metrics.go` + `adapter/postgres/store.go` — eventos relayed, sweeps com falha e `pulsar_horizon_outbox_backlog` via novo `BacklogCounter` opcional.
- `pkg/eventbus/metrics.go` — `pulsar_eventbus_dlq_advisories_total{stream,consumer}` no listener de advisories.
- `internal/holds/observer_otel.go` — `OTelObserver` ligado ao seam da #26 via `WithObserver` nos mains do core/chrono: ops por outcome (`degraded`/`short_circuited`), latência (só tentativas que chegaram ao Redis) e gauge `pulsar_holds_breaker_open`.

**Tracing**
- `pkg/eventbus/trace.go` — producer/consumer spans no bus: `Publish` injeta o contexto ativo nos headers (W3C `traceparent`; nil map inicializado — contrato fixado por teste) e `handle` extrai antes do handler, juntando toda a saga num trace só; falhas gravadas na span antes do NAK; `Correlation-Id` vira atributo.
- `internal/gateway/middleware.go` — `Tracing`: server span raiz por request (rota, request id, status), dentro de `RequestID`; extração de upstream intencionalmente desativada (raiz do trace — decisão registrada em código).
- `deployments/docker-compose.yml` — Jaeger all-in-one (UI `:16686`, porta OTLP `4317` publicada porque os serviços rodam no host), Prometheus e Grafana provisionados.

**Dashboards e docs**
- `deployments/docker/prometheus/prometheus.yml` — scrape dos 5 serviços via `host.docker.internal` (`host-gateway` no Linux).
- `deployments/docker/grafana/` — datasource + dashboard "PulsarPass — Saga overview" (10 painéis: os cinco sinais do blueprint, degradação do acelerador e estado do breaker).
- `docs/ARCHITECTURE.md` — ADR-8, §9 atualizado ao estado implementado; README com a seção de observabilidade.

### Garantias implementadas
| Garantia | Onde vive |
|---|---|
| Zero custo quando desligado | instruments no-op até `metrics.Init` (testes/tools pagam nada) |
| Trace único da saga | propagação `traceparent` no bus (`trace.go`) + teste ponta a ponta com JetStream real |
| Cardinalidade limitada | rotas templateadas no gateway; classes de outcome fixadas por teste |
| Last-value não congela | gauges regravados a todo tick (pending age, backlog, breaker) |
| Falha de init não deixa estado parcial | rollback do provider global (`pkg/metrics`) |

### Testes
- `pkg/metrics` — scrape com valores corretos + runtime; duplo `Init`; branch OTLP com trace context válido.
- `pkg/eventbus/trace_test.go` — span consumer carrega o trace id do producer através de headers reais do JetStream; `TestPublishWithActiveSpanAndNilHeaders` fixa o contrato de publish com span ativa.
- `internal/gateway` — scrape de métricas sem vazar id concreto; server span com request id.
- `internal/payment` — as quatro classes de charge no scrape via fluxo real do `Handle`.
- Gate completo com `-race` e e2e testcontainers verdes em todos os PRs (#27, #28, este).

---

## Ciclo 4 — Prova de carga

**Objetivo:** validar p99 e zero overbooking sob pico (roadmap 4) com suíte k6 executável e reproduzível — e endurecer o broker onde a própria carga revelou furos.

**Commits:** `ae311c5` → `2bbcd19`

### Entregas

**Sampler de traces configurável (#29, PR #31)**
- `pkg/metrics` — `OTEL_TRACE_SAMPLE_RATIO` (0..1) troca a raiz para `TraceIDRatioBased` em `ParentBased`; unset mantém always sampling; valor inválido (incluindo `NaN`, que escapava do range check e era plataforma-dependente: amd64 amostrava tudo, arm64 nada) falha o init.

**Tooling k6 + hardening do broker (PR #33)**
- `deployments/k6/flash-sale.js` — perfil travado: 300 VUs de pico, ramp 2min + platô 3min + ramp-down 1min, threshold `p(99)<250` e `http_req_failed rate==0`, conversão (30%) paga no ato; knobs por env (`PEAK_VUS`, `PACE`, `CONVERSION_RATIO`, `CAPACITY`).
- `deployments/k6/seed.sql` + `verify.sql` — evento semeado e **invariantes de inventário** pós-run: `reserved+sold ≤ capacity`, `sold_count == CONFIRMED`, `reserved_count == PENDING` (`make load-seed` / `load-run` / `load-verify`).
- **Pacing de redelivery** (`pkg/eventbus/jetstream.go`) — NAKs agora usam `NakWithDelay` escalando com a tentativa (100ms→2s) e `MaxDeliver` default 5→10 (~14s de janela): um comando que corre contra sua projeção — `payment.process` chegando antes do `reservation_context`, i.e. usuário pagando logo após reservar — sobrevive; antes queimava o budget em ms e ia pra DLQ. Descoberta empírica: `BackOff` de consumer é inerte para NAK.
- **Rejeições terminais ACKadas** (`internal/core/subscribers.go`) — `ErrSoldOut`/`ErrSaleNotOpen` são outcomes de negócio terminais: ACK com log+métrica em vez de 10 redeliveries e DLQ com alarme falso (3.160 NAKs no smoke). DLQ volta a significar "investigar".
- **Métrica de DLQ por dono** (`pkg/eventbus`) — o advisory é broadcast: os 5 serviços incrementavam a mesma métrica (inflação 5× vista no run: 358.800 ≈ 5 × 71.760). Só o dono do consumer conta agora.

**Run completo (300 VUs, evento de 1.000 ingressos)**
- Ingress: **105.030 requests, 291,8 rps sustentados por 6min, p99 4,95ms** (alvo 250ms), 0% falhas, checks 100%.
- Zero overbooking: **0 violações**; `292 CONFIRMED + 708 PENDING = 1000 = capacity` exato; outbox drenada a 0; breaker nunca abriu; sweep e compensações íntegros sob pico.
- DLQ pós-correção: ~23k advisories no run final, todas `payment-process` — submissões de conversão contra reservas esgotadas (artefato do perfil cego do k6; usuário real só pagaria reserva existente) + corrida da #32. Follow-ups: **#32** (espera inline em `ErrContextNotFound`), **#34** (tolerância T-0 de `sale_not_open`).

### Garantias implementadas
| Garantia | Onde vive |
|---|---|
| Zero overbooking sob pico | UPDATE condicional no Postgres sob 291 rps; `load-verify` = 0 violações |
| Comando retryable sobrevive à corrida de estado | `NakWithDelay` escalando + `MaxDeliver` 10 (`jetstream.go`) |
| Rejeição terminal não envenena a DLQ | ACK em `sold_out`/`sale_not_open` (`subscribers.go`) + teste de classificação |
| Métrica de DLQ não multiplica por serviço | filtro de ownership no `listenDLQ` + teste com advisory alheio |
| Volume de traces controlável sob carga | `OTEL_TRACE_SAMPLE_RATIO` com fail-fast (#29) |

### Testes
- `pkg/metrics` — sampler: ratio 0/1/unset/inválido (`NaN` incluso).
- `pkg/eventbus` — `redeliveryDelay` (tabela); `TestDLQAdvisoryCountedByOwnerOnly` (advisory próprio conta, alheio ignorado).
- `internal/core` — subscriber ACKa terminal e mantém retryable não-terminal (ports in-memory).
- Gate completo com `-race` e e2e testcontainers verdes em todos os PRs (#31, #33, este).

---

## Template para novos ciclos

```markdown
## Ciclo N — <Tema>

**Objetivo:** <1–2 frases>

**Commits:** `<hash-inicial>` → `<hash-final>`

### Entregas
**<Área/serviço>**
- `caminho/arquivo.go` — o que foi feito, símbolos relevantes, decisão tomada.

### Garantias implementadas (quando aplicável)
| Garantia | Onde vive |

### Testes
- <testes novos/de integração e o que provam>
```
