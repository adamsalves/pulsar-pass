// Flash-sale load profile for the reservation saga.
//
// Profile (locked with the team for cycle 4): 300 peak VUs, 2m ramp-up,
// 3m plateau, 1m ramp-down, p99 < 250ms at the ingress. Every iteration
// reserves tickets; a fraction (CONVERSION_RATIO) immediately pays,
// exercising reserve → payment → confirm end to end. Sold-out
// rejections happen downstream of the async gateway, so they show up
// in pulsar_core_reservations_total{outcome="sold_out"} (Prometheus),
// never as HTTP errors — the ingress keeps answering 202.
//
// Run with: make load-run EVENT_ID=<id from make load-seed>
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const EVENT_ID = __ENV.EVENT_ID || '';
const PEAK_VUS = Number(__ENV.PEAK_VUS || 300);
const RAMP_UP = __ENV.RAMP_UP || '2m';
const PLATEAU = __ENV.PLATEAU || '3m';
const RAMP_DOWN = __ENV.RAMP_DOWN || '1m';
const CONVERSION_RATIO = Number(__ENV.CONVERSION_RATIO || 0.3);
const QUANTITY = Number(__ENV.QUANTITY || 1);
// Seconds a VU waits between iterations. 300 VUs at pace 1 keep the
// arrival rate near 300 rps; lower it only with the local Postgres in
// view, not to chase k6 numbers.
const PACE = Number(__ENV.PACE || 1);

const reservationsAccepted = new Counter('pulsar_reservations_accepted');
const paymentsSubmitted = new Counter('pulsar_payments_submitted');

export const options = {
  scenarios: {
    flash_sale: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: RAMP_UP, target: PEAK_VUS },
        { duration: PLATEAU, target: PEAK_VUS },
        { duration: RAMP_DOWN, target: 0 },
      ],
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<250'],
    http_req_failed: ['rate==0'],
  },
};

function randHex(n) {
  let s = '';
  for (let i = 0; i < n; i++) {
    s += Math.floor(Math.random() * 16).toString(16);
  }
  return s;
}

// Identity: the gateway resolves user ids from bearer tokens (cycle 8).
// AUTH_TOKENS is the same CSV the gateway runs with — generate a load
// table with `make load-auth-tokens LOAD_USERS=2000` and pass it to
// both the gateway process and this script. A user whose reservation
// is still pending (unique index: one active reservation per user per
// event) cannot reserve again, so the table must be larger than the
// event capacity (LOAD_USERS >= 2x CAPACITY is a safe default).
const credentials = (__ENV.AUTH_TOKENS || '')
  .split(',')
  .map((pair) => pair.trim().split('='))
  .filter((parts) => parts.length === 2 && parts[0] && parts[1])
  .map(([token, user]) => ({ token, user }));
if (credentials.length === 0) {
  throw new Error('AUTH_TOKENS is required: generate a load table with `make load-auth-tokens` and pass it to the gateway and to this script');
}

export default function () {
  // A random identity per iteration: identities are fungible for the
  // load profile — each one owns whatever reservation it just created
  // and pays with the same token (same identity, so no impostor noise).
  const cred = credentials[Math.floor(Math.random() * credentials.length)];
  const res = http.post(
    `${BASE_URL}/v1/reservations`,
    JSON.stringify({ event_id: EVENT_ID, quantity: QUANTITY }),
    {
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': 'load-res-' + randHex(16),
        Authorization: 'Bearer ' + cred.token,
      },
    },
  );
  if (!check(res, { 'reserve accepted 202': (r) => r.status === 202 })) {
    sleep(PACE);
    return;
  }
  reservationsAccepted.add(1);
  const reservationID = res.json('reservation_id');

  if (reservationID && Math.random() < CONVERSION_RATIO) {
    const pay = http.post(
      `${BASE_URL}/v1/reservations/${reservationID}/payment`,
      JSON.stringify({ payment_method_token: 'tok-ok' }),
      {
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': 'load-pay-' + randHex(16),
          Authorization: 'Bearer ' + cred.token,
        },
      },
    );
    if (check(pay, { 'payment accepted 202': (r) => r.status === 202 })) {
      paymentsSubmitted.add(1);
    }
  }
  sleep(PACE);
}
