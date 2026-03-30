from __future__ import annotations

import json
import os
import re
import subprocess
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
import structlog
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from oikos_scraper.config import AppConfig, SourceDefinition
from oikos_scraper.db.repository import (
    complete_scrape_run,
    create_scrape_run,
    ensure_sources,
    list_ingestions,
    list_listings_for_geocode_enrichment,
    list_listings_for_llm_enrichment,
    list_listings_for_price_enrichment,
    update_listing_geocode,
    update_listing_price,
    upsert_bronze_listing,
    upsert_listing_ingestion,
    upsert_listings,
    upsert_llm_enrichment,
)
from oikos_scraper.geocoding import NominatimGeocoder, build_listing_geocode_query
from oikos_scraper.ingest_cache import RedisError, build_ingest_cache
from oikos_scraper.db.session import create_session_factory
from oikos_scraper.heuristics import extract_asset_links, extract_follow_links, extract_image_urls, extract_text_blocks, find_price_candidates
from oikos_scraper.normalizer import normalize_listing
from oikos_scraper.object_store import offering_hash
from oikos_scraper.settings import get_setting
from oikos_scraper.strategies.browser import BrowserStrategy
from oikos_scraper.strategies.embedded_data import EmbeddedDataStrategy
from oikos_scraper.strategies.selenium_grid import SeleniumGridStrategy
from oikos_scraper.strategies.static_html import StaticHTMLStrategy, enrich_listing_from_detail_html, extract_listing_from_detail
from oikos_scraper.types import CrawledPage, ListingDraft, ParsedListingRecord, StrategyResult

LOGGER = structlog.get_logger(__name__)

_STRIP_HTML_TAGS = re.compile(r"<[^>]+>")
_COLLAPSE_WHITESPACE = re.compile(r"\s+")


def _extract_text_from_html(html: str) -> str | None:
    """Strip HTML tags and return concatenated plain text, collapsing whitespace."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "meta", "link", "noscript", "head"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    text = _COLLAPSE_WHITESPACE.sub(" ", text).strip()
    return text or None


LLM_EXTRACTION_PROMPT = """Fill in this JSON with values extracted from the real estate listing below. Use the structured fields as hints and the description to fill in any gaps. Return numeric values without currency symbols. For latitude/longitude, only provide values if coordinates appear explicitly in the address or description — otherwise use null.

Title: {title}
Transaction type: {transaction_type}
Property type: {property_type}
City: {city}
State: {state}
Neighborhood: {neighborhood}
Address: {address}
Price (sale): {price_sale}
Price (rent): {price_rent}
Condo fee: {condo_fee}
IPTU: {iptu}
Bedrooms: {bedrooms}
Bathrooms: {bathrooms}
Parking spaces: {parking_spaces}
Area m²: {area_m2}
Description: {description}

{{"price_sale":null,"price_rent":null,"condo_fee":null,"iptu":null,"address":null,"neighborhood":null,"city":null,"bedrooms":null,"bathrooms":null,"parking_spaces":null,"area_m2":null,"property_type":null,"transaction_type":null,"latitude":null,"longitude":null}}"""


@dataclass(slots=True)
class SourceRunSummary:
    source_code: str
    strategy: str
    items_seen: int
    items_inserted: int
    items_updated: int
    error_count: int


@dataclass(slots=True)
class IngestionSummary:
    source_code: str
    strategy: str
    items_seen: int
    ingestions_upserted: int
    cached_skips: int
    error_count: int


@dataclass(slots=True)
class ParseSummary:
    source_code: str
    parsed_count: int
    error_count: int



@dataclass(slots=True)
class PriceEnrichmentSummary:
    source_code: str
    processed: int
    enriched: int
    error_count: int


@dataclass(slots=True)
class GeocodeEnrichmentSummary:
    source_code: str
    processed: int
    enriched: int
    no_match: int
    error_count: int


@dataclass(slots=True)
class LlmEnrichmentSummary:
    source_code: str
    processed: int
    enriched: int
    error_count: int


class ScrapeRunner:
    def __init__(self, config: AppConfig, database_url: str | None = None) -> None:
        self.config = config
        self.database_url = database_url
        self.session_factory = None
        self.selenium_remote_url = get_setting("OIKOS_SELENIUM_REMOTE_URL")
        self.ingest_cache = build_ingest_cache()
        self.max_crawl_depth = int(get_setting("OIKOS_MAX_CRAWL_DEPTH", "5") or "5")
        self.max_links_per_page = int(get_setting("OIKOS_MAX_LINKS_PER_PAGE", "25") or "25")
        self.max_pages_per_listing = int(get_setting("OIKOS_MAX_PAGES_PER_LISTING", "100") or "100")
        self.strategies = {
            "static_html": StaticHTMLStrategy(),
            "embedded_data": EmbeddedDataStrategy(),
            "browser": BrowserStrategy(),
        }
        if self.selenium_remote_url:
            self.strategies["selenium"] = SeleniumGridStrategy(self.selenium_remote_url)
        self.ollama_url = get_setting("OIKOS_OLLAMA_URL", "http://ollama.llm.svc.cluster.local:11434") or "http://ollama.llm.svc.cluster.local:11434"
        self.ollama_model = get_setting("OIKOS_OLLAMA_MODEL", "qwen3:4b") or "qwen3:4b"
        self.ollama_token = get_setting("OIKOS_OLLAMA_TOKEN")
        self.geocode_endpoint = (
            get_setting("OIKOS_GEOCODE_ENDPOINT", "https://nominatim.openstreetmap.org")
            or "https://nominatim.openstreetmap.org"
        )
        self.geocode_provider = get_setting("OIKOS_GEOCODE_PROVIDER", "nominatim") or "nominatim"
        self.geocode_country = get_setting("OIKOS_GEOCODE_COUNTRY", "Brazil") or "Brazil"
        self.geocode_accept_language = (
            get_setting("OIKOS_GEOCODE_ACCEPT_LANGUAGE", "pt-BR,pt;q=0.9,en;q=0.8")
            or "pt-BR,pt;q=0.9,en;q=0.8"
        )
        self.geocode_user_agent = (
            get_setting("OIKOS_GEOCODE_USER_AGENT", "oikos-scrapper/1.0")
            or "oikos-scrapper/1.0"
        )
        self.geocode_rate_limit_seconds = float(
            get_setting("OIKOS_GEOCODE_RATE_LIMIT_SECONDS", "1.1") or "1.1"
        )

    def _project_root(self) -> Path:
        env_root = os.environ.get("OIKOS_PROJECT_ROOT")
        if env_root:
            return Path(env_root)
        # When running from source, walk up from runner.py → oikos_scraper → src → project root
        candidate = Path(__file__).resolve().parents[2]
        if (candidate / "dbt_project.yml").exists():
            return candidate
        # Installed as a package: fall back to /app (Docker WORKDIR)
        return Path("/app")

    def _strategy_sequence(self, preferred_strategy: str) -> list[str]:
        httpx_primary = preferred_strategy if preferred_strategy in {"embedded_data", "static_html"} else "embedded_data"
        httpx_secondary = "static_html" if httpx_primary == "embedded_data" else "embedded_data"
        ordered = [httpx_primary, httpx_secondary, "browser"]
        if "selenium" in self.strategies:
            ordered.append("selenium")
        return list(dict.fromkeys(ordered))

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

    def _discover_with_fallbacks(self, source: SourceDefinition) -> tuple[str, list[ListingDraft], dict]:
        strategy_sequence = self._strategy_sequence(source.preferred_strategy)
        last_error: Exception | None = None
        for strategy_name in strategy_sequence:
            try:
                aggregated = StrategyResult(strategy=strategy_name, listings=[])
                diagnostics: dict = {}
                with self._http_client() as client:
                    strategy = self.strategies[strategy_name]
                    for seed_url in source.urls:
                        result = strategy.scrape_seed(client, source, seed_url)
                        aggregated.listings.extend(result.listings)
                        diagnostics[seed_url] = result.diagnostics
                if aggregated.listings:
                    return strategy_name, aggregated.listings, diagnostics
                LOGGER.info("scrape_strategy_empty", source=source.code, strategy=strategy_name)
            except Exception as exc:  # pragma: no cover - real network failures
                last_error = exc
                LOGGER.warning("scrape_strategy_failed", source=source.code, strategy=strategy_name, error=str(exc))
        raise RuntimeError(str(last_error or "no listings extracted"))

    def scrape_sources(
        self,
        source_codes: list[str] | None = None,
        trigger_type: str = "scheduled",
        group: str | None = None,
    ) -> list[SourceRunSummary]:
        selected = (
            [self.config.find_source(code) for code in source_codes]
            if source_codes
            else self.config.active_sources(group=group)
        )
        with self._session_factory()() as session:
            source_records = ensure_sources(session, selected)

        summaries: list[SourceRunSummary] = []
        for source in selected:
            summaries.append(self.scrape_source(source, source_records[source.code], trigger_type=trigger_type))
        return summaries

    def scrape_source(self, source: SourceDefinition, source_record, trigger_type: str) -> SourceRunSummary:
        strategy_sequence = self._strategy_sequence(source.preferred_strategy)
        with self._session_factory()() as session:
            run = create_scrape_run(session, source.code, trigger_type, strategy_sequence[0], pipeline_stage="scrape")

        try:
            strategy_name, listings, _ = self._discover_with_fallbacks(source)
            with self._session_factory()() as session:
                inserted, updated = upsert_listings(session, source_record, listings)
                complete_scrape_run(
                    session,
                    run,
                    status="success",
                    items_seen=len(listings),
                    items_inserted=inserted,
                    items_updated=updated,
                    error_count=0,
                )
            return SourceRunSummary(
                source_code=source.code,
                strategy=strategy_name,
                items_seen=len(listings),
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
            LOGGER.exception("scrape_failed", source=source.code)
            return SourceRunSummary(
                source_code=source.code,
                strategy=strategy_sequence[0],
                items_seen=0,
                items_inserted=0,
                items_updated=0,
                error_count=1,
            )

    def _playwright_page_html(self, url: str) -> str | None:
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                try:
                    page = browser.new_page(viewport={"width": 1440, "height": 1200})
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=5000)
                    except PlaywrightTimeoutError:
                        pass
                    return page.content()
                finally:
                    browser.close()
        except Exception:
            return None

    def _selenium_page_html(self, url: str) -> str | None:
        if not self.selenium_remote_url:
            return None
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        driver = webdriver.Remote(command_executor=self.selenium_remote_url, options=options)
        try:
            driver.set_window_size(1440, 1400)
            driver.get(url)
            return driver.page_source
        except Exception:
            return None
        finally:
            driver.quit()

    def _fetch_page_html(self, client: httpx.Client, url: str, *, raw_html: str | None = None) -> str:
        if raw_html and raw_html.strip():
            return raw_html
        last_error: Exception | None = None
        try:
            response = client.get(url)
            response.raise_for_status()
            return response.text
        except Exception as exc:
            last_error = exc
        html = self._playwright_page_html(url)
        if html:
            return html
        html = self._selenium_page_html(url)
        if html:
            return html
        raise RuntimeError(f"unable to fetch page html for {url}: {last_error}")

    def _allowed_hosts(self, source: SourceDefinition, listing: ListingDraft) -> set[str]:
        hosts = {
            urlparse(source.base_url).netloc.lower(),
            urlparse(listing.canonical_url).netloc.lower(),
        }
        return {host for host in hosts if host}

    def _dedupe_links(self, values: list[str]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            deduped.append(value)
        return deduped

    def _crawl_listing_pages(
        self,
        *,
        client: httpx.Client,
        source: SourceDefinition,
        listing: ListingDraft,
    ) -> list[CrawledPage]:
        allowed_hosts = self._allowed_hosts(source, listing)
        queue: deque[tuple[str, int, str | None, str | None]] = deque(
            [(listing.canonical_url, 0, None, listing.raw_payload.get("raw_html"))]
        )
        seen = {listing.canonical_url}
        pages: list[CrawledPage] = []

        while queue and len(pages) < self.max_pages_per_listing:
            page_url, depth, parent_page_url, raw_html = queue.popleft()
            html = self._fetch_page_html(client, page_url, raw_html=raw_html)
            image_urls = extract_image_urls(html, page_url)
            asset_links = self._dedupe_links(image_urls + extract_asset_links(html, page_url))
            link_urls = extract_follow_links(html, page_url, allowed_hosts=allowed_hosts)
            link_urls = link_urls[: self.max_links_per_page]
            pages.append(
                CrawledPage(
                    page_url=page_url,
                    depth=depth,
                    parent_page_url=parent_page_url,
                    html=html,
                    image_urls=image_urls,
                    link_urls=link_urls,
                    asset_links=asset_links,
                )
            )
            if depth >= self.max_crawl_depth:
                continue
            for link_url in link_urls:
                if link_url in seen:
                    continue
                seen.add(link_url)
                queue.append((link_url, depth + 1, page_url, None))

        return pages

    def ingest_sources(
        self,
        source_codes: list[str] | None = None,
        trigger_type: str = "scheduled",
        group: str | None = None,
    ) -> list[IngestionSummary]:
        selected = (
            [self.config.find_source(code) for code in source_codes]
            if source_codes
            else self.config.active_sources(group=group)
        )
        with self._session_factory()() as session:
            source_records = ensure_sources(session, selected)

        summaries: list[IngestionSummary] = []
        for source in selected:
            summaries.append(self.ingest_source(source, source_records[source.code], trigger_type=trigger_type))
        return summaries

    def ingest_source(self, source: SourceDefinition, source_record, trigger_type: str) -> IngestionSummary:
        strategy_sequence = self._strategy_sequence(source.preferred_strategy)
        strategy_name = strategy_sequence[0]
        ingestions_upserted = 0
        cached_skips = 0
        with self._session_factory()() as session:
            run = create_scrape_run(session, source.code, trigger_type, strategy_sequence[0], pipeline_stage="ingest")

        try:
            strategy_name, listings, _ = self._discover_with_fallbacks(source)
            with self._http_client() as client:
                for listing in listings:
                    listing_reserved = False
                    listing_ingested = False
                    try:
                        listing_reserved = self.ingest_cache.reserve_listing(source.code, listing.external_id)
                    except RedisError as exc:
                        LOGGER.warning(
                            "ingest_cache_failed_open",
                            source=source.code,
                            external_id=listing.external_id,
                            error=str(exc),
                        )
                        listing_reserved = True
                    if not listing_reserved:
                        cached_skips += 1
                        LOGGER.info("ingest_cache_listing_hit", source=source.code, external_id=listing.external_id)
                        continue
                    LOGGER.info("ingest_cache_listing_miss", source=source.code, external_id=listing.external_id)
                    try:
                        pages = self._crawl_listing_pages(client=client, source=source, listing=listing)
                        seed_url = str(listing.raw_payload.get("seed_url") or source.urls[0])
                        aggregated_asset_links = self._dedupe_links(
                            [asset_link for page in pages for asset_link in page.asset_links]
                        )
                        for page in pages:
                            if page.depth > 0:
                                LOGGER.info(
                                    "ingest_follow_page_skipped",
                                    source=source.code,
                                    external_id=listing.external_id,
                                    page_url=page.page_url,
                                    canonical_url=listing.canonical_url,
                                    depth=page.depth,
                                )
                                continue
                            with self._session_factory()() as session:
                                ingestion = upsert_listing_ingestion(
                                    session,
                                    scrape_run=run,
                                    source=source_record,
                                    listing=listing,
                                    page_url=page.page_url,
                                    seed_url=seed_url,
                                    parent_page_url=page.parent_page_url,
                                    depth=page.depth,
                                    strategy=strategy_name,
                                    image_urls=page.image_urls,
                                    asset_links=aggregated_asset_links if page.depth == 0 else page.asset_links,
                                    screenshot_uri=None,
                                    ingestion_payload={
                                        **listing.raw_payload,
                                        "title": listing.title,
                                        "canonical_url": listing.canonical_url,
                                        "page_url": page.page_url,
                                        "parent_page_url": page.parent_page_url,
                                        "depth": page.depth,
                                        "discovered_links": page.link_urls,
                                        "asset_links": aggregated_asset_links if page.depth == 0 else page.asset_links,
                                        "raw_html": page.html,
                                    },
                                )
                            listing_ingested = True
                            ingestions_upserted += 1
                    except Exception:
                        if not listing_ingested:
                            try:
                                self.ingest_cache.release_listing(source.code, listing.external_id)
                            except RedisError as exc:
                                LOGGER.warning(
                                    "ingest_cache_release_failed",
                                    source=source.code,
                                    external_id=listing.external_id,
                                    error=str(exc),
                                )
                        raise

            with self._session_factory()() as session:
                complete_scrape_run(
                    session,
                    run,
                    status="success",
                    items_seen=len(listings),
                    items_inserted=ingestions_upserted,
                    items_updated=0,
                    error_count=0,
                )
            return IngestionSummary(
                source_code=source.code,
                strategy=strategy_name,
                items_seen=len(listings),
                ingestions_upserted=ingestions_upserted,
                cached_skips=cached_skips,
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
            LOGGER.exception("ingest_failed", source=source.code)
            return IngestionSummary(
                source_code=source.code,
                strategy=strategy_name,
                items_seen=0,
                ingestions_upserted=ingestions_upserted,
                cached_skips=cached_skips,
                error_count=1,
            )

    def parse_sources(self, source_codes: list[str] | None = None) -> list[ParseSummary]:
        summaries: list[ParseSummary] = []
        with self._session_factory()() as session:
            ingestions = list_ingestions(session, source_codes=source_codes)
            source_records = ensure_sources(
                session,
                [self.config.find_source(code) for code in {row.source_code for row in ingestions}],
            )

        grouped: dict[str, list] = {}
        for ingestion in ingestions:
            grouped.setdefault(ingestion.source_code, []).append(ingestion)

        for source_code, rows in grouped.items():
            parsed_count = 0
            error_count = 0
            source_definition = self.config.find_source(source_code)
            for ingestion in rows:
                try:
                    html = str(ingestion.ingestion_payload.get("raw_html") or "")
                    text_html = _extract_text_from_html(html) if html else None
                    listing = extract_listing_from_detail(
                        source_definition,
                        html,
                        ingestion.canonical_url,
                        ingestion.seed_url,
                    )
                    if listing is None:
                        listing = normalize_listing(
                            source_definition,
                            dict(ingestion.ingestion_payload),
                            ingestion.seed_url,
                        )
                    if listing is None:
                        raise RuntimeError(f"unable to parse ingestion {ingestion.id}")
                    enrich_listing_from_detail_html(listing, html)
                    record = ParsedListingRecord(
                        source_code=listing.source_code,
                        external_id=listing.external_id,
                        offering_hash=offering_hash(listing.source_code, listing.external_id),
                        canonical_url=listing.canonical_url,
                        title=listing.title,
                        transaction_type=listing.transaction_type,
                        property_type=listing.property_type,
                        city=listing.city,
                        state=listing.state,
                        neighborhood=listing.neighborhood,
                        address=listing.address,
                        latitude=listing.latitude,
                        longitude=listing.longitude,
                        price_sale=listing.price_sale,
                        price_rent=listing.price_rent,
                        condo_fee=listing.condo_fee,
                        iptu=listing.iptu,
                        bedrooms=listing.bedrooms,
                        bathrooms=listing.bathrooms,
                        parking_spaces=listing.parking_spaces,
                        area_m2=listing.area_m2,
                        description=listing.description,
                        broker_name=listing.broker_name,
                        published_at=listing.published_at,
                        listing_created_at=listing.listing_created_at,
                        listing_updated_at=listing.listing_updated_at,
                        image_uris=[],
                        asset_links=list(ingestion.asset_links or []),
                        screenshot_uri=ingestion.screenshot_uri,
                        html_uri=None,
                        metadata_uri=None,
                        text_html=text_html,
                        raw_payload={},
                        parsed_at=datetime.now(UTC),
                    )
                    with self._session_factory()() as session:
                        upsert_bronze_listing(
                            session,
                            source=source_records[source_code],
                            ingestion=ingestion,
                            record=record,
                        )
                    parsed_count += 1
                except Exception:
                    error_count += 1
                    LOGGER.exception("parse_ingestion_failed", ingestion_id=ingestion.id, source_code=source_code)
            summaries.append(ParseSummary(source_code=source_code, parsed_count=parsed_count, error_count=error_count))
        return summaries

    def run_dbt_build(self, select: str | None = None) -> subprocess.CompletedProcess[str]:
        env = None
        profiles_dir = self._project_root() / "dbt"
        command = [
            "dbt",
            "build",
            "--project-dir",
            str(self._project_root()),
            "--profiles-dir",
            str(profiles_dir),
        ]
        if select:
            command.extend(["--select", select])
        return subprocess.run(command, check=True, text=True, capture_output=True, env=env)

    def _fetch_page_html_with_source(self, client: httpx.Client, url: str) -> tuple[str, str]:
        """Fetch page HTML with httpx → playwright → selenium fallback. Returns (html, source_name)."""
        try:
            response = client.get(url)
            response.raise_for_status()
            return response.text, "httpx"
        except Exception:
            pass
        html = self._playwright_page_html(url)
        if html:
            return html, "playwright"
        html = self._selenium_page_html(url)
        if html:
            return html, "selenium"
        raise RuntimeError(f"unable to fetch {url} with any strategy")

    def enrich_prices(
        self,
        source_codes: list[str] | None = None,
        limit: int = 100,
    ) -> list[PriceEnrichmentSummary]:
        with self._session_factory()() as session:
            listings = list_listings_for_price_enrichment(session, source_codes=source_codes, limit=limit)

        grouped: dict[str, list] = {}
        for listing in listings:
            grouped.setdefault(listing.source_code, []).append(listing)

        summaries: list[PriceEnrichmentSummary] = []
        for source_code, rows in grouped.items():
            processed = 0
            enriched = 0
            error_count = 0
            with self._http_client() as client:
                for listing in rows:
                    processed += 1
                    try:
                        html, fetch_source = self._fetch_page_html_with_source(client, listing.canonical_url)
                        prices = find_price_candidates(extract_text_blocks(html)[:40])
                        if not prices:
                            LOGGER.info(
                                "price_enrichment_no_price",
                                listing_id=listing.id,
                                source_code=source_code,
                                url=listing.canonical_url,
                            )
                            continue
                        with self._session_factory()() as session:
                            update_listing_price(
                                session,
                                listing_id=listing.id,
                                transaction_type=listing.transaction_type,
                                price=prices[0],
                                enrichment_source=fetch_source,
                            )
                        enriched += 1
                        LOGGER.info(
                            "price_enriched",
                            listing_id=listing.id,
                            source_code=source_code,
                            price=str(prices[0]),
                            fetch_source=fetch_source,
                        )
                    except Exception:
                        error_count += 1
                        LOGGER.exception(
                            "price_enrichment_failed",
                            listing_id=listing.id,
                            source_code=source_code,
                            url=listing.canonical_url,
                        )
            summaries.append(
                PriceEnrichmentSummary(
                    source_code=source_code,
                    processed=processed,
                    enriched=enriched,
                    error_count=error_count,
                )
            )
        return summaries

    def enrich_geocodes(
        self,
        source_codes: list[str] | None = None,
        limit: int = 200,
    ) -> list[GeocodeEnrichmentSummary]:
        with self._session_factory()() as session:
            listings = list_listings_for_geocode_enrichment(
                session,
                source_codes=source_codes,
                limit=limit,
            )

        grouped: dict[str, list] = {}
        for listing in listings:
            grouped.setdefault(listing.source_code, []).append(listing)

        summaries: list[GeocodeEnrichmentSummary] = []
        geocoder = NominatimGeocoder(
            endpoint=self.geocode_endpoint,
            user_agent=self.geocode_user_agent,
            accept_language=self.geocode_accept_language,
            rate_limit_seconds=self.geocode_rate_limit_seconds,
            provider=self.geocode_provider,
            country=self.geocode_country,
        )

        for source_code, rows in grouped.items():
            processed = 0
            enriched = 0
            no_match = 0
            error_count = 0
            with self._session_factory()() as session:
                run = create_scrape_run(
                    session,
                    source_code,
                    "scheduled",
                    "geocode_enrichment",
                    pipeline_stage="enriching_geocodes",
                )
            with self._http_client() as client:
                for listing in rows:
                    processed += 1
                    query = build_listing_geocode_query(
                        address=listing.address,
                        neighborhood=listing.neighborhood,
                        city=listing.city,
                        state=listing.state,
                        country=self.geocode_country,
                    )
                    if query is None:
                        continue
                    try:
                        result = geocoder.geocode_listing(
                            client,
                            address=listing.address,
                            neighborhood=listing.neighborhood,
                            city=listing.city,
                            state=listing.state,
                        )
                        with self._session_factory()() as session:
                            if result is None:
                                update_listing_geocode(
                                    session,
                                    listing_id=listing.id,
                                    latitude=None,
                                    longitude=None,
                                    provider=self.geocode_provider,
                                    query=query,
                                    status="no_match",
                                    confidence=None,
                                    payload={},
                                )
                                no_match += 1
                            else:
                                update_listing_geocode(
                                    session,
                                    listing_id=listing.id,
                                    latitude=result.latitude,
                                    longitude=result.longitude,
                                    provider=result.provider,
                                    query=result.query,
                                    status="matched",
                                    confidence=result.confidence,
                                    payload=result.payload,
                                )
                                enriched += 1
                    except Exception:
                        error_count += 1
                        LOGGER.exception(
                            "geocode_enrichment_failed",
                            listing_id=listing.id,
                            source_code=source_code,
                            query=query,
                        )
            with self._session_factory()() as session:
                complete_scrape_run(
                    session,
                    run,
                    status="success" if error_count == 0 else "partial_success",
                    items_seen=processed,
                    items_inserted=enriched + no_match,
                    items_updated=0,
                    error_count=error_count,
                )
            summaries.append(
                GeocodeEnrichmentSummary(
                    source_code=source_code,
                    processed=processed,
                    enriched=enriched,
                    no_match=no_match,
                    error_count=error_count,
                )
            )
        return summaries

    def _call_ollama(self, prompt: str) -> dict:
        """Call Ollama API with think:false + JSON format. Returns parsed dict."""
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.ollama_token:
            headers["Authorization"] = f"Bearer {self.ollama_token}"
        with httpx.Client(timeout=120.0) as client:
            response = client.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.ollama_model,
                    "prompt": prompt,
                    "format": "json",
                    "think": False,
                    "stream": False,
                    "options": {"temperature": 0},
                },
                headers=headers,
            )
            response.raise_for_status()
        raw_response = response.json().get("response", "{}")
        return json.loads(raw_response)

    def enrich_with_llm(
        self,
        source_codes: list[str] | None = None,
        limit: int = 50,
    ) -> list[LlmEnrichmentSummary]:
        with self._session_factory()() as session:
            listings = list_listings_for_llm_enrichment(session, source_codes=source_codes, limit=limit)

        grouped: dict[str, list] = {}
        for listing in listings:
            grouped.setdefault(listing["source_code"], []).append(listing)

        summaries: list[LlmEnrichmentSummary] = []
        for source_code, rows in grouped.items():
            processed = 0
            enriched = 0
            error_count = 0
            for listing in rows:
                processed += 1
                llm_input = {
                    "title": listing.get("title") or "",
                    "transaction_type": listing.get("transaction_type") or "",
                    "property_type": listing.get("property_type") or "",
                    "city": listing.get("city") or "",
                    "state": listing.get("state") or "",
                    "neighborhood": listing.get("neighborhood") or "",
                    "address": listing.get("address") or "",
                    "price_sale": listing.get("price_sale") or "",
                    "price_rent": listing.get("price_rent") or "",
                    "condo_fee": listing.get("condo_fee") or "",
                    "iptu": listing.get("iptu") or "",
                    "bedrooms": listing.get("bedrooms") or "",
                    "bathrooms": listing.get("bathrooms") or "",
                    "parking_spaces": listing.get("parking_spaces") or "",
                    "area_m2": listing.get("area_m2") or "",
                    "description": (listing.get("description") or "")[:2000],
                }
                prompt = LLM_EXTRACTION_PROMPT.format(**llm_input)
                try:
                    extracted = self._call_ollama(prompt)
                    with self._session_factory()() as session:
                        upsert_llm_enrichment(
                            session,
                            offering_hash=listing["offering_hash"],
                            source_code=listing["source_code"],
                            external_id=listing["external_id"],
                            llm_model=self.ollama_model,
                            extracted=extracted,
                            llm_input=llm_input,
                        )
                    enriched += 1
                    LOGGER.info(
                        "llm_enrichment_done",
                        offering_hash=listing["offering_hash"],
                        source_code=source_code,
                        price_sale=extracted.get("price_sale"),
                        price_rent=extracted.get("price_rent"),
                        latitude=extracted.get("latitude"),
                        longitude=extracted.get("longitude"),
                    )
                except Exception:
                    error_count += 1
                    LOGGER.exception(
                        "llm_enrichment_failed",
                        offering_hash=listing["offering_hash"],
                        source_code=source_code,
                    )
            summaries.append(
                LlmEnrichmentSummary(
                    source_code=source_code,
                    processed=processed,
                    enriched=enriched,
                    error_count=error_count,
                )
            )
        return summaries

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
