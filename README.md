# 🌦️ OpenWeather Airflow Data Pipeline

An end-to-end **data engineering pipeline** that ingests weather data from the **OpenWeatherMap API**, transforms it using **Python**, and loads it into **PostgreSQL**, orchestrated by **Apache Airflow** and deployed locally using **Docker Compose**.

This project demonstrates **production-style data engineering practices** including orchestration, idempotent loading, data modeling, observability, and infrastructure-as-code.

---

## ✨ Key Features

- OpenWeatherMap API ingestion (current weather)
- Apache Airflow (TaskFlow API + CeleryExecutor)
- Dynamic task mapping (parallel ingestion per location)
- Raw → Curated → Serving data layers
- Idempotent upserts with retries
- PostgreSQL warehouse
- Docker Compose–based local deployment
- pgAdmin web UI for database inspection

---

## 🏗️ Architecture Overview

### High-Level System Architecture

```mermaid
flowchart LR
    API[OpenWeather API<br/>Current Weather]
    AF[Apache Airflow<br/>TaskFlow API]
    RAW[(Postgres<br/>raw.weather_api_responses)]
    CUR[(Postgres<br/>mart.weather_observation)]
    LATEST[(Postgres<br/>mart.weather_latest)]

    API -->|HTTPS JSON| AF
    AF -->|Insert JSONB| RAW
    RAW -->|Transform & Upsert| CUR
    CUR -->|Refresh| LATEST
