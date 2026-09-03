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
set -euo pipefail

NAMESPACE=pulsarpass
GATEWAY_URL=${GATEWAY_URL:-http://localhost:8080}
PROM_URL=${PROM_URL:-http://localhost:9090}
JAEGER_URL=${JAEGER_URL:-http://localhost:16686}
CAPACITY=${CAPACITY:-50}
QUANTITY=${QUANTITY:-2}
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
code=$(curl -s -o /dev/null -w '%{http_code}' -m 5 "$GATEWAY_URL/" || true)
[[ "$code" =~ ^[0-9]{3}$ ]] || fail "gateway unreachable at $GATEWAY_URL"

# --- run one saga; echoes reservation id on success ------------------------
run_saga() {
  local attempt=$1
  local idem_seed="smoke-$(date +%s)-$attempt-$$"
  local user="smoke-user-$attempt"

  log "seeding event (capacity=$CAPACITY)"
  local event_id
  event_id=$(psql -d pulsar_core -q -tA -v capacity="$CAPACITY" < deployments/k6/seed.sql)
  [[ -n "$event_id" ]] || fail "event seed returned no id"

  log "creating reservation"
  local res_body res_id
  res_body=$(curl -fsS -X POST "$GATEWAY_URL/v1/reservations" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $idem_seed-reserve" \
    -H "X-User-Id: $user" \
    -d "{\"event_id\":\"$event_id\",\"quantity\":$QUANTITY}")
  res_id=$(printf '%s' "$res_body" | json_field "['reservation_id']")
  [[ -n "$res_id" ]] || fail "no reservation_id in response: $res_body"

  # The payment command races the reservation_context projection; the
  # processor waits inline, but give the relay a beat before submitting.
  sleep 2

  log "submitting payment"
  curl -fsS -X POST "$GATEWAY_URL/v1/reservations/$res_id/payment" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $idem_seed-pay" \
    -H "X-User-Id: $user" \
    -d '{"payment_method_token":"tok-smoke"}' >/dev/null

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

  # Hold hygiene: released on confirmation.
  local holds
  holds=$(kubectl exec "$(redis_pod)" -n "$NAMESPACE" -- redis-cli --scan --pattern "hold:$res_id")
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
# min over all pulsar-* targets: 1 = everything up, 0 = something down,
# no series = targets registered but not scraped yet (fresh clusters:
# first scrape happens within the scrape interval). Poll until the
# series materializes instead of failing on the race.
minup=""
for _ in $(seq 1 18); do
  minup=$(curl -fsS -G "$PROM_URL/api/v1/query" \
    --data-urlencode 'query=min(up{job=~"pulsar-.*"})' \
    | json_field "['data']['result'][0]['value'][1]" 2>/dev/null || true)
  [[ "$minup" == "1" ]] && break
  sleep 5
done
[[ "$minup" == "1" ]] || fail "Prometheus pulsar-* targets not all up (min up=${minup:-none})"

log "checking Jaeger trace"
traces="[]"
for _ in $(seq 1 6); do
  sleep 5
  traces=$(curl -fsS "$JAEGER_URL/api/traces?service=pulsar-gateway&limit=1&lookback=1h" | json_field "['data']" 2>/dev/null || echo "[]")
  [[ "$traces" != "[]" ]] && break
done
[[ "$traces" != "[]" ]] || fail "no pulsar-gateway trace found in Jaeger"

log "ALL CHECKS PASSED"
