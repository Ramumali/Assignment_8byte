# Dagster Stock Pipeline (Dockerized)

## Overview
This project fetches stock time-series JSON from Alpha Vantage, parses it, and upserts rows into a PostgreSQL table automatically via Dagster.

Components:
- Postgres (stores final stock data and Dagster run metadata)
- Dagit web UI (Dagster UI) and daemon (scheduler)
- Dagster job that fetches & stores data

## Prerequisites
- Docker & Docker Compose v2
- Alpha Vantage API key (free): https://www.alphavantage.co/support/#api-key

## Files of interest
- `docker-compose.yml` — brings up postgres, dagit, dagster-daemon
- `workspace/libs/stock_loader.py` — API + DB logic
- `workspace/jobs/fetch_stock_job.py` — Dagster job definition
- `workspace/sql/create_table.sql` — table schema

## Setup & Run

1. Clone / copy this repository locally.

2. Put your Alpha Vantage API key and optional DB settings into environment variables.
   You can create a `.env` file next to `docker-compose.yml`: