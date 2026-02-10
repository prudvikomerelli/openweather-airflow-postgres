# weather_pipeline/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict

from airflow.hooks.base import BaseHook

DEFAULT_ENDPOINT = "weather"  # OpenWeatherMap "Current Weather Data"
DEFAULT_UNITS = "metric"


def get_api_key_from_env_or_airflow() -> str:
    """
    Priority:
    1) OPENWEATHER_API_KEY env var
    2) Airflow Connection: openweathermap_api (password field)
    """
    env_key = os.getenv("OPENWEATHER_API_KEY")
    if env_key:
        return env_key

    conn = BaseHook.get_connection("openweathermap_api")
    if conn.password:
        return conn.password

    raise RuntimeError(
        "OpenWeather API key not found. Set env OPENWEATHER_API_KEY or Airflow connection openweathermap_api.password"
    )


def get_postgres_conninfo_from_airflow() -> Dict[str, str]:
    """
    Reads Airflow connection 'postgres_warehouse' and returns a dict suitable for psycopg2.connect(**dict).
    """
    conn = BaseHook.get_connection("postgres_warehouse")
    if not conn.host:
        raise RuntimeError("Airflow connection postgres_warehouse is missing host")

    return {
        "host": conn.host,
        "port": str(conn.port or 5432),
        "dbname": conn.schema or "postgres",
        "user": conn.login or "postgres",
        "password": conn.password or "",
        "sslmode": os.getenv("PGSSLMODE", "prefer"),
    }