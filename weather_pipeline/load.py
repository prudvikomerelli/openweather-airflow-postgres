# weather_pipeline/load.py
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

from weather_pipeline.transform import normalize_current_weather_payload


def _connect(pg_conninfo: Dict[str, str]):
    return psycopg2.connect(
        host=pg_conninfo["host"],
        port=int(pg_conninfo["port"]),
        dbname=pg_conninfo["dbname"],
        user=pg_conninfo["user"],
        password=pg_conninfo["password"],
        sslmode=pg_conninfo.get("sslmode", "prefer"),
    )


def pg_fetch_active_locations(pg_conninfo: Dict[str, str]) -> List[Dict[str, Any]]:
    sql = """
      SELECT location_id, location_key, lat, lon
      FROM dim.location
      WHERE is_active = TRUE
      ORDER BY location_id;
    """
    with _connect(pg_conninfo) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            return [dict(r) for r in rows]


def pg_insert_raw_response(
    pg_conninfo: Dict[str, str],
    endpoint: str,
    location_id: int,
    location_key: str,
    request_params: Dict[str, Any],
    http_status: int,
    data_timestamp: Optional[str],
    payload: Dict[str, Any],
) -> str:
    sql = """
      INSERT INTO raw.weather_api_responses
        (endpoint, location_id, location_key, request_params, http_status, data_timestamp, payload)
      VALUES
        (%s, %s, %s, %s::jsonb, %s, %s::timestamptz, %s::jsonb)
      RETURNING ingestion_id;
    """
    with _connect(pg_conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    endpoint,
                    location_id,
                    location_key,
                    psycopg2.extras.Json(request_params),
                    http_status,
                    data_timestamp,
                    psycopg2.extras.Json(payload),
                ),
            )
            ingestion_id = cur.fetchone()[0]
            return str(ingestion_id)


def pg_upsert_weather_observation(pg_conninfo: Dict[str, str], ingestion_id: str) -> int:
    """
    Loads curated observation from raw payload referenced by ingestion_id.
    Upsert is keyed by (location_id, observed_at).
    Returns affected rowcount (1 typically).
    """
    fetch_sql = """
      SELECT location_id, payload, ingested_at
      FROM raw.weather_api_responses
      WHERE ingestion_id = %s;
    """

    upsert_sql = """
      INSERT INTO mart.weather_observation (
        location_id, observed_at,
        temp_c, feels_like_c, humidity_pct, pressure_hpa,
        wind_speed_mps, wind_deg, clouds_pct, visibility_m,
        rain_1h_mm, snow_1h_mm,
        weather_main, weather_description,
        ingested_at, source_ingestion_id
      )
      VALUES (
        %(location_id)s, %(observed_at)s,
        %(temp_c)s, %(feels_like_c)s, %(humidity_pct)s, %(pressure_hpa)s,
        %(wind_speed_mps)s, %(wind_deg)s, %(clouds_pct)s, %(visibility_m)s,
        %(rain_1h_mm)s, %(snow_1h_mm)s,
        %(weather_main)s, %(weather_description)s,
        %(ingested_at)s, %(source_ingestion_id)s
      )
      ON CONFLICT (location_id, observed_at) DO UPDATE SET
        temp_c              = EXCLUDED.temp_c,
        feels_like_c        = EXCLUDED.feels_like_c,
        humidity_pct        = EXCLUDED.humidity_pct,
        pressure_hpa        = EXCLUDED.pressure_hpa,
        wind_speed_mps      = EXCLUDED.wind_speed_mps,
        wind_deg            = EXCLUDED.wind_deg,
        clouds_pct          = EXCLUDED.clouds_pct,
        visibility_m        = EXCLUDED.visibility_m,
        rain_1h_mm          = EXCLUDED.rain_1h_mm,
        snow_1h_mm          = EXCLUDED.snow_1h_mm,
        weather_main        = EXCLUDED.weather_main,
        weather_description = EXCLUDED.weather_description,
        ingested_at         = EXCLUDED.ingested_at,
        source_ingestion_id = EXCLUDED.source_ingestion_id;
    """

    with _connect(pg_conninfo) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(fetch_sql, (ingestion_id,))
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"No raw row found for ingestion_id={ingestion_id}")

            location_id = row["location_id"]
            payload = row["payload"]
            ingested_at = row["ingested_at"]

            normalized = normalize_current_weather_payload(payload)
            params = {
                "location_id": location_id,
                "observed_at": normalized["observed_at"],
                "temp_c": normalized["temp_c"],
                "feels_like_c": normalized["feels_like_c"],
                "humidity_pct": normalized["humidity_pct"],
                "pressure_hpa": normalized["pressure_hpa"],
                "wind_speed_mps": normalized["wind_speed_mps"],
                "wind_deg": normalized["wind_deg"],
                "clouds_pct": normalized["clouds_pct"],
                "visibility_m": normalized["visibility_m"],
                "rain_1h_mm": normalized["rain_1h_mm"],
                "snow_1h_mm": normalized["snow_1h_mm"],
                "weather_main": normalized["weather_main"],
                "weather_description": normalized["weather_description"],
                "ingested_at": ingested_at,
                "source_ingestion_id": ingestion_id,
            }

            cur.execute(upsert_sql, params)
            # psycopg2 rowcount for INSERT..ON CONFLICT can be 1 (insert/update)
            return cur.rowcount


def pg_refresh_latest(pg_conninfo: Dict[str, str]) -> None:
    """
    Rebuild/refresh mart.weather_latest using the newest observation per location.
    For moderate data sizes, this is fine; at scale, do incremental updates instead.
    """
    sql = """
      INSERT INTO mart.weather_latest (
        location_id, observed_at,
        temp_c, feels_like_c, humidity_pct, pressure_hpa,
        wind_speed_mps, wind_deg, clouds_pct, visibility_m,
        rain_1h_mm, snow_1h_mm,
        weather_main, weather_description,
        updated_at
      )
      SELECT DISTINCT ON (location_id)
        location_id, observed_at,
        temp_c, feels_like_c, humidity_pct, pressure_hpa,
        wind_speed_mps, wind_deg, clouds_pct, visibility_m,
        rain_1h_mm, snow_1h_mm,
        weather_main, weather_description,
        NOW() AS updated_at
      FROM mart.weather_observation
      ORDER BY location_id, observed_at DESC
      ON CONFLICT (location_id) DO UPDATE SET
        observed_at         = EXCLUDED.observed_at,
        temp_c              = EXCLUDED.temp_c,
        feels_like_c        = EXCLUDED.feels_like_c,
        humidity_pct        = EXCLUDED.humidity_pct,
        pressure_hpa        = EXCLUDED.pressure_hpa,
        wind_speed_mps      = EXCLUDED.wind_speed_mps,
        wind_deg            = EXCLUDED.wind_deg,
        clouds_pct          = EXCLUDED.clouds_pct,
        visibility_m        = EXCLUDED.visibility_m,
        rain_1h_mm          = EXCLUDED.rain_1h_mm,
        snow_1h_mm          = EXCLUDED.snow_1h_mm,
        weather_main        = EXCLUDED.weather_main,
        weather_description = EXCLUDED.weather_description,
        updated_at          = EXCLUDED.updated_at;
    """
    with _connect(pg_conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def pg_dq_freshness_and_rowcount(
    pg_conninfo: Dict[str, str],
    expected_locations: int,
    max_lag_minutes: int = 180,
) -> None:
    """
    Basic checks:
      - latest table has at least expected_locations rows
      - max(observed_at) within max_lag_minutes of now
    """
    count_sql = "SELECT COUNT(*) FROM mart.weather_latest;"
    max_ts_sql = "SELECT MAX(observed_at) FROM mart.weather_observation;"

    with _connect(pg_conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql)
            latest_count = cur.fetchone()[0]

            cur.execute(max_ts_sql)
            max_observed = cur.fetchone()[0]

    if latest_count < max(1, expected_locations):
        raise ValueError(f"DQ failed: mart.weather_latest has {latest_count} rows, expected >= {expected_locations}")

    if max_observed is None:
        raise ValueError("DQ failed: no observations in mart.weather_observation")

    now_utc = datetime.now(timezone.utc)
    lag = now_utc - max_observed
    if lag > timedelta(minutes=max_lag_minutes):
        raise ValueError(f"DQ failed: data is stale. max_observed={max_observed}, lag={lag}")