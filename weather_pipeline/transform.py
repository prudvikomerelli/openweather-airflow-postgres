# weather_pipeline/transform.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _utc_dt_from_unix(ts: Optional[int]) -> Optional[datetime]:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def _safe_get(d: Dict[str, Any], *keys, default=None):
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def normalize_current_weather_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize OpenWeatherMap 'current weather' payload to curated schema fields.
    Returns dict matching mart.weather_observation columns (except location_id and ingestion id).
    """
    observed_dt = _utc_dt_from_unix(payload.get("dt"))
    if observed_dt is None:
        raise ValueError("Payload missing 'dt' (observation time)")

    main = payload.get("main", {}) if isinstance(payload.get("main"), dict) else {}
    wind = payload.get("wind", {}) if isinstance(payload.get("wind"), dict) else {}
    clouds = payload.get("clouds", {}) if isinstance(payload.get("clouds"), dict) else {}
    weather0 = None
    if isinstance(payload.get("weather"), list) and payload["weather"]:
        weather0 = payload["weather"][0] if isinstance(payload["weather"][0], dict) else None

    # rain/snow sometimes nested like {"1h": mm}
    rain_1h = _safe_get(payload, "rain", "1h", default=None)
    snow_1h = _safe_get(payload, "snow", "1h", default=None)

    normalized = {
        "observed_at": observed_dt,  # datetime
        "temp_c": main.get("temp"),
        "feels_like_c": main.get("feels_like"),
        "humidity_pct": main.get("humidity"),
        "pressure_hpa": main.get("pressure"),
        "wind_speed_mps": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "clouds_pct": clouds.get("all"),
        "visibility_m": payload.get("visibility"),
        "rain_1h_mm": rain_1h,
        "snow_1h_mm": snow_1h,
        "weather_main": weather0.get("main") if weather0 else None,
        "weather_description": weather0.get("description") if weather0 else None,
    }

    return normalized