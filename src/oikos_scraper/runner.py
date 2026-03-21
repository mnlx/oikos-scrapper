from __future__ import annotations

from dataclasses import dataclass

import httpx
import structlog

from oikos_scraper.config import AppConfig, SourceDefinition
from oikos_scraper.db.repository import complete_scrape_run, create_scrape_run, ensure_sources, upsert_listings
from oikos_scraper.db.session import create_session_factory
from oikos_scraper.strategies.browser import BrowserStrategy
from oikos_scraper.strategies.embedded_data import EmbeddedDataStrategy
from oikos_scraper.strategies.static_html import StaticHTMLStrategy
from oikos_scraper.types import StrategyResult

LOGGER = structlog.get_logger(__name__)


@dataclass(slots=True)
class SourceRunSummary:
    source_code: str
    strategy: str
    items_seen: int
    items_inserted: int
    items_updated: int
    error_count: int


class ScrapeRunner:
    def __init__(self, config: AppConfig, database_url: str | None = None) -> None:
        self.config = config
        self.database_url = database_url
        self.session_factory = None
        self.strategies = {
            "static_html": StaticHTMLStrategy(),
            "embedded_data": EmbeddedDataStrategy(),
            "browser": BrowserStrategy(),
        }

    def _session_factory(self):
        if self.session_factory is None:
            self.session_factory = create_session_factory(self.database_url)
        return self.session_factory

    def _http_client(self) -> httpx.Client:
        return httpx.Client(
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                )
            },
            timeout=30.0,
        )

    def scrape_sources(self, source_codes: list[str] | None = None, trigger_type: str = "scheduled") -> list[SourceRunSummary]:
        selected = (
            [self.config.find_source(code) for code in source_codes]
            if source_codes
            else self.config.active_sources()
        )
        with self._session_factory()() as session:
            source_records = ensure_sources(session, selected)

        summaries: list[SourceRunSummary] = []
        for source in selected:
            summaries.append(self.scrape_source(source, source_records[source.code], trigger_type=trigger_type))
        return summaries

    def scrape_source(self, source: SourceDefinition, source_record, trigger_type: str) -> SourceRunSummary:
        strategy_name = source.preferred_strategy
        with self._session_factory()() as session:
            run = create_scrape_run(session, source.code, trigger_type, strategy_name)

        try:
            aggregated = StrategyResult(strategy=strategy_name, listings=[])
            with self._http_client() as client:
                strategy = self.strategies[strategy_name]
                for seed_url in source.urls:
                    result = strategy.scrape_seed(client, source, seed_url)
                    aggregated.listings.extend(result.listings)
            with self._session_factory()() as session:
                inserted, updated = upsert_listings(session, source_record, aggregated.listings)
                complete_scrape_run(
                    session,
                    run,
                    status="success",
                    items_seen=len(aggregated.listings),
                    items_inserted=inserted,
                    items_updated=updated,
                    error_count=0,
                )
            LOGGER.info(
                "scrape_complete",
                source=source.code,
                strategy=strategy_name,
                items_seen=len(aggregated.listings),
                inserted=inserted,
                updated=updated,
            )
            return SourceRunSummary(
                source_code=source.code,
                strategy=strategy_name,
                items_seen=len(aggregated.listings),
                items_inserted=inserted,
                items_updated=updated,
                error_count=0,
            )
        except Exception as exc:
            with self._session_factory()() as session:
                complete_scrape_run(
                    session,
                    run,
                    status="failed",
                    items_seen=0,
                    items_inserted=0,
                    items_updated=0,
                    error_count=1,
                    last_error=str(exc),
                )
            LOGGER.exception("scrape_failed", source=source.code, strategy=strategy_name)
            return SourceRunSummary(
                source_code=source.code,
                strategy=strategy_name,
                items_seen=0,
                items_inserted=0,
                items_updated=0,
                error_count=1,
            )

    def benchmark_source(self, source_code: str) -> dict[str, dict[str, int | str]]:
        source = self.config.find_source(source_code)
        results: dict[str, dict[str, int | str]] = {}
        with self._http_client() as client:
            for strategy_name, strategy in self.strategies.items():
                try:
                    listings = []
                    for seed_url in source.urls:
                        result = strategy.scrape_seed(client, source, seed_url)
                        listings.extend(result.listings)
                    results[strategy_name] = {
                        "status": "success",
                        "items_seen": len(listings),
                    }
                except Exception as exc:
                    results[strategy_name] = {
                        "status": "failed",
                        "error": str(exc),
                    }
        return results
