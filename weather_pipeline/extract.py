# weather_pipeline/extract.py
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests


def _utc_from_unix(ts: Optional[int]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def fetch_current_weather(
    api_key: str,
    lat: float,
    lon: float,
    units: str = "metric",
    endpoint: str = "weather",
    timeout_seconds: int = 15,
    max_retries: int = 3,
    backoff_seconds: float = 1.0,
) -> Dict[str, Any]:
    """
    Fetch current weather from OpenWeatherMap.
    Returns:
      {
        "http_status": int,
        "payload": dict,
        "request_params": dict,
        "data_timestamp": iso str | None
      }
    Retries on transient errors (429/5xx/network).
    """
    base_url = "https://api.openweathermap.org/data/2.5"
    url = f"{base_url}/{endpoint}"

    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": units,
    }

    last_err: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout_seconds)
            http_status = r.status_code

            # Retry on rate limit / transient server errors
            if http_status in (429, 500, 502, 503, 504) and attempt < max_retries:
                sleep_s = backoff_seconds * (2**attempt)
                time.sleep(sleep_s)
                continue

            # Raise for non-OK that we won't retry (e.g., 401)
            if http_status >= 400 and http_status not in (429, 500, 502, 503, 504):
                # still capture body for debugging
                try:
                    payload = r.json()
                except Exception:
                    payload = {"raw_text": r.text}
                return {
                    "http_status": http_status,
                    "payload": payload,
                    "request_params": params,
                    "data_timestamp": None,
                }

            payload = r.json()
            data_ts = None
            # For current endpoint, "dt" is present
            if isinstance(payload, dict) and "dt" in payload:
                data_ts = _utc_from_unix(payload.get("dt"))

            return {
                "http_status": http_status,
                "payload": payload,
                "request_params": params,
                "data_timestamp": data_ts,
            }

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            last_err = e
            if attempt < max_retries:
                sleep_s = backoff_seconds * (2**attempt)
                time.sleep(sleep_s)
                continue
            raise

    # Should never hit
    raise RuntimeError(f"Failed to fetch weather after retries: {last_err}")