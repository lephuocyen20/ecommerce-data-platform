#!/usr/bin/env bash
# Tạo database metadata riêng cho Airflow khi PostgreSQL khởi tạo volume mới.
set -e

if [ -z "${AIRFLOW_DB_NAME:-}" ]; then
  echo "AIRFLOW_DB_NAME is not configured" >&2
  exit 1
fi

database_exists=$(psql \
  --username "$POSTGRES_USER" \
  --dbname postgres \
  --tuples-only \
  --no-align \
  --command "SELECT 1 FROM pg_database WHERE datname = '$AIRFLOW_DB_NAME'")

if [ "$database_exists" != "1" ]; then
  psql \
    --username "$POSTGRES_USER" \
    --dbname postgres \
    --command "CREATE DATABASE \"$AIRFLOW_DB_NAME\""
fi
