# Oikos Scrapper

Python scraper for housing listings in Greater Florianopolis. It collects sale and rental listings for houses and apartments, normalizes them, and stores them in Postgres.

## Commands

```bash
python -m oikos_scraper.cli migrate
python -m oikos_scraper.cli scrape --config config/sources.yaml
python -m oikos_scraper.cli scrape-source --source olx --config config/sources.yaml
python -m oikos_scraper.cli benchmark-source --source vivareal --config config/sources.yaml
```

## Local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
playwright install chromium
cp .env.example .env
pytest
```

Fill `.env` with the database settings. Example:

```bash
DATABASE_URL=postgresql+psycopg://app:change-me-oikos@localhost:5432/app
POSTGRES_USER=app
POSTGRES_PASSWORD=change-me-oikos
POSTGRES_DB=app
OIKOS_LOG_LEVEL=INFO
OIKOS_SOURCE_CONFIG=config/sources.yaml
OIKOS_SELENIUM_REMOTE_URL=http://selenium-grid-router.selenium.svc.cluster.local:4444/wd/hub
```
