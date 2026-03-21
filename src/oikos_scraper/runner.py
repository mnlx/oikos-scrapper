from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

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
    upsert_bronze_listing,
    upsert_listing_ingestion,
    upsert_listings,
)
from oikos_scraper.db.session import create_session_factory
from oikos_scraper.heuristics import extract_image_urls
from oikos_scraper.normalizer import normalize_listing
from oikos_scraper.object_store import BronzePathSpec, build_bronze_object_store, offering_hash
from oikos_scraper.settings import get_setting
from oikos_scraper.strategies.browser import BrowserStrategy
from oikos_scraper.strategies.embedded_data import EmbeddedDataStrategy
from oikos_scraper.strategies.selenium_grid import SeleniumGridStrategy
from oikos_scraper.strategies.static_html import StaticHTMLStrategy, enrich_listing_from_detail_html, extract_listing_from_detail
from oikos_scraper.types import ListingArtifactBundle, ListingDraft, ParsedListingRecord, StoredObject, StrategyResult

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


class ScrapeRunner:
    def __init__(self, config: AppConfig, database_url: str | None = None) -> None:
        self.config = config
        self.database_url = database_url
        self.session_factory = None
        self.selenium_remote_url = get_setting("OIKOS_SELENIUM_REMOTE_URL")
        self.object_store = build_bronze_object_store()
        self.max_images_per_listing = int(get_setting("OIKOS_MAX_IMAGES_PER_LISTING", "10") or "10")
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

    def _fetch_listing_html(self, client: httpx.Client, listing: ListingDraft) -> str:
        raw_html = listing.raw_payload.get("raw_html")
        if isinstance(raw_html, str) and raw_html.strip():
            return raw_html
        response = client.get(listing.canonical_url)
        response.raise_for_status()
        return response.text

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
        return self._playwright_screenshot(url) or self._selenium_screenshot(url)

    def _artifact_key(self, *, category: str, base_hash: str, extension: str, index: int | None = None) -> str:
        return BronzePathSpec(
            layer="bronze",
            category=category,
            run_at=datetime.now(UTC),
            base_hash=base_hash,
            extension=extension,
            index=index,
        ).object_key()

    def _store_bundle(
        self,
        *,
        client: httpx.Client,
        source: SourceDefinition,
        listing: ListingDraft,
        strategy_name: str,
        html: str,
        image_urls: list[str],
    ) -> ListingArtifactBundle:
        if self.object_store is None:
            raise RuntimeError("Bronze object store is not configured")
        base_hash = offering_hash(source.code, listing.external_id)
        html_object = self.object_store.put_text(
            payload=html,
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
                "seed_urls": source.urls,
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
        screenshot_bytes = self._capture_screenshot(listing.canonical_url)
        screenshot_object: StoredObject | None = None
        if screenshot_bytes is not None:
            screenshot_object = self.object_store.put_bytes(
                payload=screenshot_bytes,
                key=self._artifact_key(category="screenshots", base_hash=base_hash, extension=".png"),
                content_type="image/png",
            )
        image_objects: list[StoredObject] = []
        for index, image_url in enumerate(image_urls[: self.max_images_per_listing], start=1):
            try:
                head = client.get(image_url)
                head.raise_for_status()
            except Exception:
                continue
            content_type = head.headers.get("Content-Type", "application/octet-stream")
            extension = self.object_store.infer_extension(image_url, content_type)
            image_objects.append(
                self.object_store.put_bytes(
                    payload=head.content,
                    key=self._artifact_key(category="images", base_hash=base_hash, extension=extension, index=index),
                    content_type=content_type,
                )
            )
        return ListingArtifactBundle(
            html=html_object,
            screenshot=screenshot_object,
            metadata=metadata_object,
            images=image_objects,
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
            with self._http_client() as client:
                for listing in listings:
                    html = self._fetch_listing_html(client, listing)
                    image_urls = extract_image_urls(html, listing.canonical_url)
                    bundle = self._store_bundle(
                        client=client,
                        source=source,
                        listing=listing,
                        strategy_name=strategy_name,
                        html=html,
                        image_urls=image_urls,
                    )
                    with self._session_factory()() as session:
                        ingestion = upsert_listing_ingestion(
                            session,
                            scrape_run=run,
                            source=source_record,
                            listing=listing,
                            seed_url=str(listing.raw_payload.get("seed_url") or source.urls[0]),
                            strategy=strategy_name,
                            image_urls=image_urls,
                            ingestion_payload={
                                **listing.raw_payload,
                                "title": listing.title,
                                "canonical_url": listing.canonical_url,
                            },
                        )
                        replace_listing_artifacts(session, ingestion=ingestion, bundle=bundle, image_source_urls=image_urls)
                    artifacts_created += len(bundle.images) + sum(
                        1 for item in (bundle.html, bundle.screenshot, bundle.metadata) if item is not None
                    )

            with self._session_factory()() as session:
                complete_scrape_run(
                    session,
                    run,
                    status="success",
                    items_seen=len(listings),
                    items_inserted=len(listings),
                    items_updated=0,
                    error_count=0,
                )
            return IngestionSummary(
                source_code=source.code,
                strategy=strategy_name,
                items_seen=len(listings),
                ingestions_upserted=len(listings),
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
                    image_uris = [artifact.object_uri for artifact in by_type.get("image", [])]
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
                        image_uris=image_uris,
                        screenshot_uri=by_type.get("screenshot", [None])[0].object_uri if by_type.get("screenshot") else None,
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
