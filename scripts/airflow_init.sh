#!/usr/bin/env bash
set -euo pipefail

echo "Initializing Airflow DB..."
airflow db migrate

echo "Creating Airflow admin user (if not exists)..."
airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME}" \
  --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
  --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME}" \
  --lastname "${_AIRFLOW_WWW_USER_LASTNAME}" \
  --role Admin \
  --email "${_AIRFLOW_WWW_USER_EMAIL}" || true

echo "Creating/Updating Airflow connection: openweathermap_api"
airflow connections delete openweathermap_api || true
airflow connections add openweathermap_api \
  --conn-type "http" \
  --conn-host "api.openweathermap.org" \
  --conn-password "${OPENWEATHER_API_KEY}"

echo "Creating/Updating Airflow connection: postgres_warehouse"
airflow connections delete postgres_warehouse || true
airflow connections add postgres_warehouse \
  --conn-type "postgres" \
  --conn-host "${WAREHOUSE_HOST}" \
  --conn-port "${WAREHOUSE_PORT}" \
  --conn-schema "${WAREHOUSE_DB}" \
  --conn-login "${WAREHOUSE_USER}" \
  --conn-password "${WAREHOUSE_PASSWORD}"

echo "Airflow init complete."