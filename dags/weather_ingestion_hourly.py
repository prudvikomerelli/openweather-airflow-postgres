# dags/weather_ingestion_hourly.py
from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago

from weather_pipeline.config import (
    get_api_key_from_env_or_airflow,
    get_postgres_conninfo_from_airflow,
    DEFAULT_ENDPOINT,
    DEFAULT_UNITS,
)
from weather_pipeline.extract import fetch_current_weather
from weather_pipeline.transform import normalize_current_weather_payload
from weather_pipeline.load import (
    pg_fetch_active_locations,
    pg_insert_raw_response,
    pg_upsert_weather_observation,
    pg_refresh_latest,
    pg_dq_freshness_and_rowcount,
)


@dag(
    dag_id="weather_ingestion_hourly",
    start_date=days_ago(2),
    schedule="0 * * * *",  # hourly
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    tags=["weather", "openweathermap", "postgres"],
)
def weather_ingestion_hourly():
    @task
    def get_locations() -> List[Dict[str, Any]]:
        """
        Pull active locations from dim.location. Returns list of dicts for mapping.
        """
        pg_conninfo = get_postgres_conninfo_from_airflow()
        return pg_fetch_active_locations(pg_conninfo)

    @task
    def extract_and_load_raw(loc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch API data for a single location, store raw JSON to Postgres, return ingestion metadata.
        """
        api_key = get_api_key_from_env_or_airflow()
        pg_conninfo = get_postgres_conninfo_from_airflow()

        endpoint = Variable.get("OPENWEATHER_ENDPOINT", default_var=DEFAULT_ENDPOINT)
        units = Variable.get("OPENWEATHER_UNITS", default_var=DEFAULT_UNITS)

        # 1) Extract
        resp = fetch_current_weather(
            api_key=api_key,
            lat=loc["lat"],
            lon=loc["lon"],
            units=units,
            endpoint=endpoint,
            timeout_seconds=15,
        )

        # 2) Load raw (append)
        ingestion_id = pg_insert_raw_response(
            pg_conninfo=pg_conninfo,
            endpoint=endpoint,
            location_id=loc["location_id"],
            location_key=loc["location_key"],
            request_params=resp["request_params"],
            http_status=resp["http_status"],
            data_timestamp=resp.get("data_timestamp"),
            payload=resp["payload"],
        )

        return {
            "location_id": loc["location_id"],
            "location_key": loc["location_key"],
            "ingestion_id": ingestion_id,
        }

    @task
    def transform_and_upsert_curated(meta: Dict[str, Any]) -> Dict[str, Any]:
        """
        Read raw payload from the extractor result, normalize, and upsert into mart.weather_observation.
        """
        pg_conninfo = get_postgres_conninfo_from_airflow()

        # We already have raw payload in extractor output? We stored it in DB.
        # To keep XCom small, we normalize by reusing payload from DB in load.py upsert function,
        # but here we'll keep it simple: fetch payload back by ingestion_id inside pg_upsert_weather_observation.
        rowcount = pg_upsert_weather_observation(
            pg_conninfo=pg_conninfo,
            ingestion_id=meta["ingestion_id"],
        )
        return {"location_id": meta["location_id"], "rowcount": rowcount}

    @task
    def dq_checks(curated_results: List[Dict[str, Any]]) -> None:
        pg_conninfo = get_postgres_conninfo_from_airflow()
        expected_locations = len(curated_results)

        # Simple check: rowcount totals and freshness
        pg_dq_freshness_and_rowcount(
            pg_conninfo=pg_conninfo,
            expected_locations=expected_locations,
            max_lag_minutes=180,
        )

    @task
    def refresh_latest() -> None:
        pg_conninfo = get_postgres_conninfo_from_airflow()
        pg_refresh_latest(pg_conninfo)

    locations = get_locations()
    raw_metas = extract_and_load_raw.expand(loc=locations)
    curated = transform_and_upsert_curated.expand(meta=raw_metas)
    dq_checks(curated)
    refresh_latest()


dag = weather_ingestion_hourly()