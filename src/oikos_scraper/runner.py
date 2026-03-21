from __future__ import annotations

import json
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
    list_artifacts_for_ingestion,
    list_ingestions,
    replace_listing_artifacts,
    upsert_listing_asset,
    upsert_bronze_listing,
    upsert_listing_ingestion,
    upsert_listings,
)
from oikos_scraper.db.session import create_session_factory
from oikos_scraper.heuristics import ASSET_SUFFIXES, extract_asset_links, extract_follow_links, extract_image_urls
from oikos_scraper.normalizer import normalize_listing
from oikos_scraper.object_store import BronzePathSpec, build_bronze_object_store, offering_hash
from oikos_scraper.settings import get_setting
from oikos_scraper.strategies.browser import BrowserStrategy
from oikos_scraper.strategies.embedded_data import EmbeddedDataStrategy
from oikos_scraper.strategies.selenium_grid import SeleniumGridStrategy
from oikos_scraper.strategies.static_html import StaticHTMLStrategy, enrich_listing_from_detail_html, extract_listing_from_detail
from oikos_scraper.types import CrawledPage, ListingArtifactBundle, ListingDraft, ParsedListingRecord, StoredObject, StrategyResult

LOGGER = structlog.get_logger(__name__)


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
    artifacts_created: int
    error_count: int


@dataclass(slots=True)
class ParseSummary:
    source_code: str
    parsed_count: int
    error_count: int


@dataclass(slots=True)
class AssetEnrichmentSummary:
    source_code: str
    assets_seen: int
    assets_scrapped: int
    assets_reused: int
    error_count: int


class ScrapeRunner:
    def __init__(self, config: AppConfig, database_url: str | None = None) -> None:
        self.config = config
        self.database_url = database_url
        self.session_factory = None
        self.selenium_remote_url = get_setting("OIKOS_SELENIUM_REMOTE_URL")
        self.object_store = build_bronze_object_store()
        self.max_images_per_listing = int(get_setting("OIKOS_MAX_IMAGES_PER_LISTING", "10") or "10")
        self.max_crawl_depth = int(get_setting("OIKOS_MAX_CRAWL_DEPTH", "5") or "5")
        self.max_links_per_page = int(get_setting("OIKOS_MAX_LINKS_PER_PAGE", "25") or "25")
        self.max_pages_per_listing = int(get_setting("OIKOS_MAX_PAGES_PER_LISTING", "100") or "100")
        self.enable_screenshots = (get_setting("OIKOS_ENABLE_SCREENSHOTS", "true") or "true").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.strategies = {
            "static_html": StaticHTMLStrategy(),
            "embedded_data": EmbeddedDataStrategy(),
            "browser": BrowserStrategy(),
        }
        if self.selenium_remote_url:
            self.strategies["selenium"] = SeleniumGridStrategy(self.selenium_remote_url)

    def _project_root(self) -> Path:
        return Path(__file__).resolve().parents[2]

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

    def _playwright_screenshot(self, url: str) -> bytes | None:
        if not self.enable_screenshots:
            return None
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
                    return page.screenshot(full_page=True, type="png")
                finally:
                    browser.close()
        except Exception:
            return None

    def _selenium_screenshot(self, url: str) -> bytes | None:
        if not self.enable_screenshots or not self.selenium_remote_url:
            return None
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        driver = webdriver.Remote(command_executor=self.selenium_remote_url, options=options)
        try:
            driver.set_window_size(1440, 1400)
            driver.get(url)
            return driver.get_screenshot_as_png()
        except Exception:
            return None
        finally:
            driver.quit()

    def _capture_screenshot(self, url: str) -> bytes | None:
        return self._selenium_screenshot(url)

    def _artifact_key(self, *, category: str, base_hash: str, extension: str, index: int | None = None) -> str:
        return BronzePathSpec(
            layer="bronze",
            category=category,
            run_at=datetime.now(UTC),
            base_hash=base_hash,
            extension=extension,
            index=index,
        ).object_key()

    def _page_artifact_hash(self, listing: ListingDraft, page_url: str, depth: int) -> str:
        base_hash = offering_hash(listing.source_code, listing.external_id)
        if depth == 0 and page_url == listing.canonical_url:
            return base_hash
        page_hash = offering_hash(base_hash, page_url)[:12]
        return f"{base_hash}-d{depth:02d}-{page_hash}"

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

    def _asset_type(self, asset_url: str, content_type: str | None = None) -> str:
        lowered_type = (content_type or "").split(";", 1)[0].strip().lower()
        if lowered_type.startswith("image/"):
            return "image"
        if lowered_type == "application/pdf":
            return "pdf"
        path = urlparse(asset_url).path.lower()
        if any(path.endswith(ext) for ext in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".tif", ".tiff", ".avif", ".heic"}):
            return "image"
        if path.endswith(".pdf"):
            return "pdf"
        if any(path.endswith(ext) for ext in ASSET_SUFFIXES):
            return "asset"
        return "asset"

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

    def _store_bundle(
        self,
        *,
        client: httpx.Client,
        source: SourceDefinition,
        listing: ListingDraft,
        page: CrawledPage,
        strategy_name: str,
    ) -> ListingArtifactBundle:
        if self.object_store is None:
            raise RuntimeError("Bronze object store is not configured")
        base_hash = self._page_artifact_hash(listing, page.page_url, page.depth)
        html_object = self.object_store.put_text(
            payload=page.html,
            key=self._artifact_key(category="html", base_hash=base_hash, extension=".html"),
            content_type="text/html; charset=utf-8",
        )
        metadata_payload = json.dumps(
            {
                "source_code": source.code,
                "source_name": source.name,
                "strategy": strategy_name,
                "external_id": listing.external_id,
                "canonical_url": listing.canonical_url,
                "page_url": page.page_url,
                "parent_page_url": page.parent_page_url,
                "depth": page.depth,
                "seed_urls": source.urls,
                "discovered_links": page.link_urls,
                "raw_payload": listing.raw_payload,
            },
            ensure_ascii=True,
            default=str,
            indent=2,
        )
        metadata_object = self.object_store.put_text(
            payload=metadata_payload,
            key=self._artifact_key(category="json", base_hash=base_hash, extension=".json"),
            content_type="application/json",
        )
        screenshot_bytes = self._capture_screenshot(page.page_url)
        screenshot_object: StoredObject | None = None
        if screenshot_bytes is not None:
            screenshot_object = self.object_store.put_bytes(
                payload=screenshot_bytes,
                key=self._artifact_key(category="screenshots", base_hash=base_hash, extension=".png"),
                content_type="image/png",
            )
        return ListingArtifactBundle(
            html=html_object,
            screenshot=screenshot_object,
            metadata=metadata_object,
        )

    def ingest_sources(
        self,
        source_codes: list[str] | None = None,
        trigger_type: str = "scheduled",
        group: str | None = None,
    ) -> list[IngestionSummary]:
        if self.object_store is None:
            raise RuntimeError("Bronze object store is not configured")
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
        with self._session_factory()() as session:
            run = create_scrape_run(session, source.code, trigger_type, strategy_sequence[0], pipeline_stage="ingest")

        try:
            strategy_name, listings, _ = self._discover_with_fallbacks(source)
            artifacts_created = 0
            ingestions_upserted = 0
            with self._http_client() as client:
                for listing in listings:
                    pages = self._crawl_listing_pages(client=client, source=source, listing=listing)
                    seed_url = str(listing.raw_payload.get("seed_url") or source.urls[0])
                    aggregated_asset_links = self._dedupe_links(
                        [asset_link for page in pages for asset_link in page.asset_links]
                    )
                    for page in pages:
                        bundle = self._store_bundle(
                            client=client,
                            source=source,
                            listing=listing,
                            page=page,
                            strategy_name=strategy_name,
                        )
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
                                screenshot_uri=bundle.screenshot.object_uri if bundle.screenshot is not None else None,
                                ingestion_payload={
                                    **listing.raw_payload,
                                    "title": listing.title,
                                    "canonical_url": listing.canonical_url,
                                    "page_url": page.page_url,
                                    "parent_page_url": page.parent_page_url,
                                    "depth": page.depth,
                                    "discovered_links": page.link_urls,
                                    "asset_links": aggregated_asset_links if page.depth == 0 else page.asset_links,
                                },
                            )
                            replace_listing_artifacts(
                                session,
                                ingestion=ingestion,
                                bundle=bundle,
                            )
                        ingestions_upserted += 1
                        artifacts_created += sum(
                            1 for item in (bundle.html, bundle.screenshot, bundle.metadata) if item is not None
                        )

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
                artifacts_created=artifacts_created,
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
                strategy=strategy_sequence[0],
                items_seen=0,
                ingestions_upserted=0,
                artifacts_created=0,
                error_count=1,
            )

    def enrich_assets_sources(self, source_codes: list[str] | None = None) -> list[AssetEnrichmentSummary]:
        if self.object_store is None:
            raise RuntimeError("Bronze object store is not configured")
        selected = (
            [self.config.find_source(code) for code in source_codes]
            if source_codes
            else self.config.active_sources()
        )
        with self._session_factory()() as session:
            source_records = ensure_sources(session, selected)

        summaries: list[AssetEnrichmentSummary] = []
        for source in selected:
            summaries.append(self.enrich_assets_source(source, source_records[source.code]))
        return summaries

    def enrich_assets_source(self, source: SourceDefinition, source_record) -> AssetEnrichmentSummary:
        strategy_name = "asset_enrichment"
        with self._session_factory()() as session:
            run = create_scrape_run(session, source.code, "scheduled", strategy_name, pipeline_stage="enriching_assets")

        assets_seen = 0
        assets_scrapped = 0
        assets_reused = 0
        error_count = 0
        try:
            with self._session_factory()() as session:
                ingestions = list_ingestions(session, source_codes=[source.code])
            with self._http_client() as client:
                for ingestion in ingestions:
                    asset_links = self._dedupe_links(list(ingestion.asset_links or []))
                    for asset_id, asset_url in enumerate(asset_links, start=1):
                        assets_seen += 1
                        default_extension = self.object_store.infer_extension(asset_url, "application/octet-stream")
                        asset_hash = f"{ingestion.offering_hash}-asset-{asset_id:02d}"
                        key = self._artifact_key(category="assets", base_hash=asset_hash, extension=default_extension)
                        asset_uri = self.object_store.uri_for_key(key)
                        is_scrapped = self.object_store.object_exists(key)
                        stored: StoredObject | None = None
                        if is_scrapped:
                            assets_reused += 1
                        else:
                            try:
                                stored = self.object_store.fetch_and_store(
                                    client=client,
                                    source_url=asset_url,
                                    key=key,
                                )
                                asset_uri = stored.uri
                                is_scrapped = True
                                assets_scrapped += 1
                            except Exception:
                                error_count += 1
                                LOGGER.exception(
                                    "asset_enrichment_failed",
                                    source_code=source.code,
                                    external_id=ingestion.external_id,
                                    asset_url=asset_url,
                                )
                        with self._session_factory()() as session:
                            upsert_listing_asset(
                                session,
                                source=source_record,
                                ingestion=ingestion,
                                asset_id=asset_id,
                                asset_type=self._asset_type(asset_url, stored.content_type if stored else None),
                                asset_url=asset_url,
                                asset_uri=asset_uri,
                                is_scrapped=is_scrapped,
                                content_type=stored.content_type if stored else None,
                                checksum_sha256=stored.checksum_sha256 if stored else None,
                                size_bytes=stored.size if stored else None,
                            )
            with self._session_factory()() as session:
                complete_scrape_run(
                    session,
                    run,
                    status="success",
                    items_seen=assets_seen,
                    items_inserted=assets_scrapped + assets_reused,
                    items_updated=0,
                    error_count=error_count,
                )
            return AssetEnrichmentSummary(
                source_code=source.code,
                assets_seen=assets_seen,
                assets_scrapped=assets_scrapped,
                assets_reused=assets_reused,
                error_count=error_count,
            )
        except Exception as exc:
            with self._session_factory()() as session:
                complete_scrape_run(
                    session,
                    run,
                    status="failed",
                    items_seen=assets_seen,
                    items_inserted=assets_scrapped + assets_reused,
                    items_updated=0,
                    error_count=error_count + 1,
                    last_error=str(exc),
                )
            LOGGER.exception("enrich_assets_failed", source=source.code)
            return AssetEnrichmentSummary(
                source_code=source.code,
                assets_seen=assets_seen,
                assets_scrapped=assets_scrapped,
                assets_reused=assets_reused,
                error_count=error_count + 1,
            )

    def parse_sources(self, source_codes: list[str] | None = None) -> list[ParseSummary]:
        if self.object_store is None:
            raise RuntimeError("Bronze object store is not configured")
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
                    with self._session_factory()() as session:
                        artifacts = list_artifacts_for_ingestion(session, ingestion.id)
                    by_type: dict[str, list] = {}
                    for artifact in artifacts:
                        by_type.setdefault(artifact.artifact_type, []).append(artifact)
                    html_artifact = by_type.get("html", [None])[0]
                    html = self.object_store.get_text(html_artifact.object_key) if html_artifact is not None else ""
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
                        image_uris=[],
                        asset_links=list(ingestion.asset_links or []),
                        screenshot_uri=ingestion.screenshot_uri
                        or (by_type.get("screenshot", [None])[0].object_uri if by_type.get("screenshot") else None),
                        html_uri=html_artifact.object_uri if html_artifact is not None else None,
                        metadata_uri=by_type.get("json", [None])[0].object_uri if by_type.get("json") else None,
                        raw_payload={
                            "ingestion_payload": ingestion.ingestion_payload,
                            "listing_raw_payload": listing.raw_payload,
                            "artifact_uris": {
                                key: [item.object_uri for item in value]
                                for key, value in by_type.items()
                            },
                        },
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
