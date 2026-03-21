# Oikos Scrapper

Python scraper for housing listings in Greater Florianopolis. It collects sale and rental listings for houses and apartments, normalizes them, and stores them in Postgres.

The pipeline is split into three phases:

1. `ingest`: discovers listings with the current `httpx -> playwright -> selenium` fallback chain and stores raw HTML, screenshots, metadata, and downloaded listing images in MinIO under `s3://datalake/bronze/ingestion/listings/...`.
2. `parse`: reads bronze artifacts back from MinIO, parses them into the `bronze_listings` table, and can optionally run the DBT silver layer.
3. `publish`: runs the DBT gold/API models so the frontend or API can read pre-grouped tables.

## Commands

```bash
python -m oikos_scraper.cli migrate
python -m oikos_scraper.cli scrape --config config/sources.yaml
python -m oikos_scraper.cli ingest --config config/sources.yaml
python -m oikos_scraper.cli parse --config config/sources.yaml --run-dbt
python -m oikos_scraper.cli publish --select tag:gold
python -m oikos_scraper.cli scrape-source --source olx --config config/sources.yaml
python -m oikos_scraper.cli benchmark-source --source vivareal --config config/sources.yaml
```

## Local setup

```bash
./scripts/bootstrap_local.sh
cp .env.example .env
source .venv/bin/activate
python -m pytest
```

`dbt-core` is expected to run on Python `3.12` or `3.13` for this project. The checked-in bootstrap script recreates `.venv` with one of those runtimes. If the machine only has Python `3.14`, use the container image or the Argo workflow path instead of local DBT.

Fill `.env` with the database settings. Example:

```bash
DATABASE_URL=postgresql+psycopg://app:change-me-oikos@localhost:5432/app
POSTGRES_USER=app
POSTGRES_PASSWORD=change-me-oikos
POSTGRES_DB=app
OIKOS_LOG_LEVEL=INFO
OIKOS_SOURCE_CONFIG=config/sources.yaml
OIKOS_SELENIUM_REMOTE_URL=http://selenium-grid-router.selenium.svc.cluster.local:4444/wd/hub
OIKOS_BRONZE_S3_ENDPOINT=minio.minio.svc.cluster.local:9000
OIKOS_BRONZE_S3_ACCESS_KEY=minio
OIKOS_BRONZE_S3_SECRET_KEY=change-me
OIKOS_BRONZE_S3_BUCKET=datalake
OIKOS_BRONZE_S3_SECURE=false
OIKOS_ENABLE_SCREENSHOTS=true
DBT_POSTGRES_HOST=localhost
DBT_POSTGRES_PORT=5432
DBT_POSTGRES_DB=app
DBT_POSTGRES_USER=app
DBT_POSTGRES_PASSWORD=change-me-oikos
DBT_POSTGRES_SCHEMA=public
```
