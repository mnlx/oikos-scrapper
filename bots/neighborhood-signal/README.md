# Neighborhood Signal Bot

Bot for collecting public neighborhood and city signals that may affect housing prices.

- Config: `bots/neighborhood-signal/sources.yaml`
- Runtime package: `src/oikos_scraper/bots/neighborhood_signal`
- Storage layout: `s3://datalake/bronze/ingestion/neighborhood_signal/...`

This bot stores raw HTML, JSON, screenshots, and binary documents in MinIO first. Parsed rows go to `neighborhood_signals`; unparsed captures stay queryable via `neighborhood_files`.
