# Oikos Scrapper

Python scraper for housing listings in Greater Florianopolis. It collects sale and rental listings for houses and apartments, normalizes them, and stores them in Postgres.

The repo is organized by bot:

- `bots/realestate-listings`: listing discovery config and docs
- `bots/neighborhood-signal`: public-signal config and docs

The listings pipeline is split into phases:

1. `ingest`: discovers listings with the current `httpx -> playwright -> selenium` fallback chain, recursively follows same-host links up to depth `5`, and stores raw HTML, screenshots, and metadata in MinIO under `s3://datalake/bronze/ingestion/listings/...`.
2. `enriching-assets`: expands `asset_links`, downloads assets into MinIO, and writes the `raw_listing_assets` table.
3. `parse`: reads bronze artifacts back from MinIO, parses them into the `raw_listings` table, and can optionally run the DBT `raw_`/`int_` layer.
4. `enrich-geocodes`: forward-geocodes parsed listing addresses into `raw_listings.latitude` and `raw_listings.longitude`.
5. `publish`: runs the DBT gold/API models so the frontend or API can read pre-grouped tables.

The neighborhood-signal bot follows the same raw-first pattern:

1. `neighborhood-ingest`: downloads official HTML, JSON, screenshots, and other documents to `s3://datalake/bronze/ingestion/neighborhood_signal/...`.
2. `neighborhood-parse`: attempts generic JSON scalar extraction into `neighborhood_signals`.
3. `neighborhood-publish`: builds `raw_`, `int_`, and `mart_` dbt models for files and parsed signals.

## Commands

```bash
python -m oikos_scraper.cli migrate
python -m oikos_scraper.cli scrape --config config/sources.yaml
python -m oikos_scraper.cli ingest --config config/sources.yaml
python -m oikos_scraper.cli enriching-assets --config config/sources.yaml
python -m oikos_scraper.cli parse --config config/sources.yaml --run-dbt
python -m oikos_scraper.cli enrich-geocodes --config config/sources.yaml --limit 200
python -m oikos_scraper.cli publish --select tag:gold
python -m oikos_scraper.cli scrape-source --source olx --config config/sources.yaml
python -m oikos_scraper.cli benchmark-source --source vivareal --config config/sources.yaml
python -m oikos_scraper.cli neighborhood-ingest --config bots/neighborhood-signal/sources.yaml
python -m oikos_scraper.cli neighborhood-parse --config bots/neighborhood-signal/sources.yaml
python -m oikos_scraper.cli neighborhood-publish
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
OIKOS_GEOCODE_ENDPOINT=https://nominatim.openstreetmap.org
OIKOS_GEOCODE_PROVIDER=nominatim
OIKOS_GEOCODE_COUNTRY=Brazil
OIKOS_GEOCODE_ACCEPT_LANGUAGE=pt-BR,pt;q=0.9,en;q=0.8
OIKOS_GEOCODE_USER_AGENT=oikos-scrapper/1.0
OIKOS_GEOCODE_RATE_LIMIT_SECONDS=1.1
DBT_POSTGRES_HOST=localhost
DBT_POSTGRES_PORT=5432
DBT_POSTGRES_DB=app
DBT_POSTGRES_USER=app
DBT_POSTGRES_PASSWORD=change-me-oikos
DBT_POSTGRES_SCHEMA=public
```
