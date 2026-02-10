-- sql/ddl.sql
-- PostgreSQL DDL for OpenWeatherMap ingestion (raw + curated + latest)

BEGIN;

-- Optional: keep things tidy
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS mart;

-- Extensions (uuid generation)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- --------------------------
-- Dimension: Locations
-- --------------------------
CREATE TABLE IF NOT EXISTS dim.location (
  location_id        BIGSERIAL PRIMARY KEY,
  location_key       TEXT NOT NULL UNIQUE,         -- e.g. "latlon:47.6062,-122.3321" or "city:Seattle,US"
  name               TEXT NULL,
  country            TEXT NULL,
  lat                DOUBLE PRECISION NOT NULL,
  lon                DOUBLE PRECISION NOT NULL,
  timezone           TEXT NULL,                    -- optional
  is_active          BOOLEAN NOT NULL DEFAULT TRUE,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_location_active ON dim.location(is_active);

-- --------------------------
-- Raw: API responses
-- --------------------------
CREATE TABLE IF NOT EXISTS raw.weather_api_responses (
  ingestion_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ingested_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source             TEXT NOT NULL DEFAULT 'openweathermap',
  endpoint           TEXT NOT NULL,                -- e.g. "weather" (current), "onecall"
  location_id        BIGINT NOT NULL REFERENCES dim.location(location_id),
  location_key       TEXT NOT NULL,
  request_params     JSONB NOT NULL,
  http_status        INTEGER NOT NULL,
  data_timestamp     TIMESTAMPTZ NULL,             -- derived from payload dt (if present)
  payload            JSONB NOT NULL
);

-- Helpful indexes for replay/debug
CREATE INDEX IF NOT EXISTS idx_raw_responses_loc_ingested
  ON raw.weather_api_responses(location_id, ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_responses_data_ts
  ON raw.weather_api_responses(location_id, data_timestamp DESC);

-- Optional: speed jsonb search
CREATE INDEX IF NOT EXISTS idx_raw_responses_payload_gin
  ON raw.weather_api_responses USING GIN (payload);

-- --------------------------
-- Curated Fact: Observations
-- --------------------------
CREATE TABLE IF NOT EXISTS mart.weather_observation (
  location_id            BIGINT NOT NULL REFERENCES dim.location(location_id),
  observed_at            TIMESTAMPTZ NOT NULL,     -- from payload "dt"
  temp_c                 REAL NULL,
  feels_like_c           REAL NULL,
  humidity_pct           REAL NULL,
  pressure_hpa           REAL NULL,
  wind_speed_mps         REAL NULL,
  wind_deg               REAL NULL,
  clouds_pct             REAL NULL,
  visibility_m           REAL NULL,
  rain_1h_mm             REAL NULL,
  snow_1h_mm             REAL NULL,
  weather_main           TEXT NULL,
  weather_description    TEXT NULL,

  ingested_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source_ingestion_id    UUID NULL REFERENCES raw.weather_api_responses(ingestion_id),

  -- Idempotency key:
  CONSTRAINT pk_weather_observation PRIMARY KEY (location_id, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_obs_loc_time_desc
  ON mart.weather_observation(location_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_obs_observed_at
  ON mart.weather_observation(observed_at DESC);

-- --------------------------
-- Latest table (one row per location)
-- --------------------------
CREATE TABLE IF NOT EXISTS mart.weather_latest (
  location_id            BIGINT PRIMARY KEY REFERENCES dim.location(location_id),
  observed_at            TIMESTAMPTZ NOT NULL,
  temp_c                 REAL NULL,
  feels_like_c           REAL NULL,
  humidity_pct           REAL NULL,
  pressure_hpa           REAL NULL,
  wind_speed_mps         REAL NULL,
  wind_deg               REAL NULL,
  clouds_pct             REAL NULL,
  visibility_m           REAL NULL,
  rain_1h_mm             REAL NULL,
  snow_1h_mm             REAL NULL,
  weather_main           TEXT NULL,
  weather_description    TEXT NULL,

  updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_latest_observed_at
  ON mart.weather_latest(observed_at DESC);

COMMIT;