#!/usr/bin/env bash
# End-to-end saga smoke against the kind cluster (cycle 6).
#
# Preconditions: make cluster-up, deploy-infra and deploy-services ran,
# and the kind port mappings expose the gateway on :8080, Prometheus on
# :9090 and Jaeger on :16686.
#
# The script seeds an event, runs the saga through the real gateway
# (reserve -> projection -> charge -> confirm) and asserts:
#   - the reservation ends CONFIRMED and the payment SUCCEEDED with the
#     projected amount (quantity x price),
#   - the Redis hold is released after the confirmation,
#   - Prometheus scrapes every pulsar-* target,
#   - a trace for the saga reached Jaeger.
#
# The simulated acquirer declines ~5% of the charges by default; a
# declined payment is a legitimate business outcome (the reservation
# goes FAILED), so the whole saga retries with a fresh reservation —
# CI overrides the rate to 0 for determinism.
#
# Identity: the gateway resolves user ids from bearer tokens (cycle 8).
# The token must exist in the cluster's AUTH_TOKENS table (Helm Secret;
# the chart default mirrors the dev default below).
set -euo pipefail

NAMESPACE=pulsarpass
GATEWAY_URL=${GATEWAY_URL:-http://localhost:8080}
PROM_URL=${PROM_URL:-http://localhost:9090}
JAEGER_URL=${JAEGER_URL:-http://localhost:16686}
CAPACITY=${CAPACITY:-50}
QUANTITY=${QUANTITY:-2}
SMOKE_TOKEN=${SMOKE_TOKEN:-pp-token-user-1}
MAX_SAGA_ATTEMPTS=3

log() { printf '[smoke] %s\n' "$*"; }
fail() { printf '[smoke] FAIL: %s\n' "$*" >&2; exit 1; }

pg_pod() { kubectl get pod -n "$NAMESPACE" -l app=postgres -o jsonpath='{.items[0].metadata.name}'; }
redis_pod() { kubectl get pod -n "$NAMESPACE" -l app=redis -o jsonpath='{.items[0].metadata.name}'; }

psql() { kubectl exec -i "$(pg_pod)" -n "$NAMESPACE" -- psql -U pulsar "$@"; }

json_field() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }

# --- preflight -------------------------------------------------------------
command -v kubectl >/dev/null || fail "kubectl is required"
command -v python3 >/dev/null || fail "python3 is required"
kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || fail "namespace $NAMESPACE not found; run make deploy-infra + deploy-services"
# Any HTTP answer proves the gateway is reachable (the HTTP port has no
# GET route; /healthz lives on the health port, not exposed to the host).
# curl prints 000 for connection failures — only 2xx-5xx count.
code=$(curl -s -o /dev/null -w '%{http_code}' -m 5 "$GATEWAY_URL/" || true)
[[ "$code" =~ ^[2-5][0-9]{2}$ ]] || fail "gateway unreachable at $GATEWAY_URL (last status: $code)"

# --- run one saga; echoes reservation id on success ------------------------
run_saga() {
  local attempt=$1
  local idem_seed="smoke-$(date +%s)-$attempt-$$"

  log "seeding event (capacity=$CAPACITY)"
  local event_id
  event_id=$(psql -d pulsar_core -q -tA -v capacity="$CAPACITY" < deployments/k6/seed.sql)
  [[ -n "$event_id" ]] || fail "event seed returned no id"

  log "creating reservation"
  local res_body res_id
  res_body=$(curl -fsS -X POST "$GATEWAY_URL/v1/reservations" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $idem_seed-reserve" \
    -H "Authorization: Bearer $SMOKE_TOKEN" \
    -d "{\"event_id\":\"$event_id\",\"quantity\":$QUANTITY}")
  res_id=$(printf '%s' "$res_body" | json_field "['reservation_id']")
  [[ -n "$res_id" ]] || fail "no reservation_id in response: $res_body"

  # The payment command races the reservation_context projection; the
  # processor waits inline, but give the relay a beat before submitting.
  sleep 2

  log "submitting payment"
  # errexit is suppressed inside run_saga (the caller retries on
  # declines via `|| rc=$?`), so every failure path here is explicit.
  curl -fsS -X POST "$GATEWAY_URL/v1/reservations/$res_id/payment" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $idem_seed-pay" \
    -H "Authorization: Bearer $SMOKE_TOKEN" \
    -d '{"payment_method_token":"tok-smoke"}' >/dev/null \
    || fail "payment submit for $res_id failed"

  # Wait for the saga to settle.
  local status="" reason="" amount=""
  for _ in $(seq 1 30); do
    status=$(psql -d pulsar_core -tAc "SELECT status FROM reservations WHERE id='$res_id'")
    if [[ "$status" == "CONFIRMED" || "$status" == "FAILED" ]]; then
      break
    fi
    sleep 1
  done
  case "$status" in
    CONFIRMED) ;;
    FAILED)
      reason=$(kubectl exec -i "$(pg_pod)" -n "$NAMESPACE" -- psql -U pulsar -d pulsar_payment -tAc \
        "SELECT failure_reason FROM payments WHERE reservation_id='$res_id' LIMIT 1")
      if [[ "$reason" == *"card declined"* ]]; then
        log "charge declined (simulator); saga attempt $attempt is inconclusive, retrying"
        return 2
      fi
      fail "reservation $res_id FAILED unexpectedly: $reason"
      ;;
    *) fail "reservation $res_id did not settle (last status: '$status')" ;;
  esac

  amount=$(kubectl exec -i "$(pg_pod)" -n "$NAMESPACE" -- psql -U pulsar -d pulsar_payment -tAc \
    "SELECT amount_cents FROM payments WHERE reservation_id='$res_id' LIMIT 1")
  [[ "$amount" == "$((QUANTITY * 1000))" ]] || fail "amount_cents=$amount, want $((QUANTITY * 1000)) (quantity x projected price)"

  # Hold hygiene: released on confirmation. `Confirm` commits the row
  # before releasing the hold, so poll briefly for the release instead
  # of asserting the instant the DB shows CONFIRMED. A failed exec must
  # fail the check (an empty --scan result alone would pass vacuously).
  local holds
  for _ in $(seq 1 3); do
    holds=$(kubectl exec "$(redis_pod)" -n "$NAMESPACE" -- redis-cli --scan --pattern "hold:$res_id") \
      || fail "hold check failed (kubectl exec against the redis pod)"
    [[ -z "$holds" ]] && break
    sleep 1
  done
  [[ -z "$holds" ]] || fail "hold key still present after confirmation: $holds"

  log "reservation $res_id CONFIRMED with amount $amount"
}

# --- run the saga, retrying simulator declines ------------------------------
attempt=1
while true; do
  rc=0
  run_saga "$attempt" || rc=$?
  [[ $rc -eq 0 ]] && break
  if [[ $rc -eq 2 && $attempt -lt $MAX_SAGA_ATTEMPTS ]]; then
    attempt=$((attempt + 1))
  else
    fail "saga did not complete after $attempt attempt(s)"
  fi
done

# --- observability ----------------------------------------------------------
log "checking Prometheus targets"
# Two scalar queries, polled together (fresh clusters register and scrape
# targets within the scrape interval):
#   - min(up) == 1  -> every resolved target is up;
#   - count(count by (job) (up)) == 5 -> all five jobs resolved at least
#     one target. min() alone passes vacuously when a whole job has no
#     series (service renamed, scaled to zero) — the paired count closes
#     that hole, and both must hold before the smoke proceeds.
PROM_JOBS=5
# -m keeps a hung collector from stalling the poll past its budget;
# stderr is silenced so a healthy wait doesn't look alarming in CI logs.
prom_query() { curl -fsS -m 10 -G "$PROM_URL/api/v1/query" --data-urlencode "query=$1" 2>/dev/null | json_field "['data']['result'][0]['value'][1]" 2>/dev/null || true; }
minup=""; jobs_seen=""
for _ in $(seq 1 18); do
  minup=$(prom_query 'min(up{job=~"pulsar-.*"})')
  jobs_seen=$(prom_query 'count(count by (job) (up{job=~"pulsar-.*"}))')
  [[ "$minup" == "1" && "$jobs_seen" == "$PROM_JOBS" ]] && break
  sleep 5
done
[[ "$minup" == "1" && "$jobs_seen" == "$PROM_JOBS" ]] \
  || fail "Prometheus pulsar-* targets unhealthy (min up=${minup:-none}, jobs with targets=${jobs_seen:-none}/$PROM_JOBS)"

log "checking Jaeger trace"
traces="[]"
for _ in $(seq 1 6); do
  sleep 5
  traces=$(curl -fsS -m 10 "$JAEGER_URL/api/traces?service=pulsar-gateway&limit=1&lookback=1h" 2>/dev/null | json_field "['data']" 2>/dev/null || echo "[]")
  [[ "$traces" != "[]" ]] && break
done
[[ "$traces" != "[]" ]] || fail "no pulsar-gateway trace found in Jaeger"

log "ALL CHECKS PASSED"
