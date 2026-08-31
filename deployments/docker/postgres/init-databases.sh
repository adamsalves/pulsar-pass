#!/usr/bin/env bash
set -euo pipefail

# Database-per-service: pulsar_core comes from POSTGRES_DB, pulsar_payment
# is created here.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE pulsar_payment;
    GRANT ALL PRIVILEGES ON DATABASE pulsar_payment TO $POSTGRES_USER;
EOSQL
