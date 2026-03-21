from __future__ import annotations

import httpx
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from oikos_scraper.config import SourceDefinition
from oikos_scraper.heuristics import collect_json_blobs
from oikos_scraper.strategies.base import ScrapeStrategy
from oikos_scraper.strategies.embedded_data import extract_from_json_blobs
from oikos_scraper.strategies.static_html import enrich_listing_from_detail_html, extract_listing_from_detail
from oikos_scraper.types import ListingDraft, StrategyResult


class BrowserStrategy(ScrapeStrategy):
    name = "browser"

    def _load_page_html(self, page, url: str) -> str:  # noqa: ANN001
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except PlaywrightTimeoutError:
            pass
        self._reveal_location(page)
        return page.content()

    def _reveal_location(self, page) -> None:  # noqa: ANN001
        candidates = (
            "mapa",
            "localização",
            "localizacao",
            "endereço",
            "endereco",
            "ver mapa",
            "mostrar localização",
            "mostrar localizacao",
        )
        for label in candidates:
            for locator in (
                page.get_by_role("button", name=label, exact=False),
                page.get_by_role("link", name=label, exact=False),
                page.get_by_text(label, exact=False),
            ):
                try:
                    if locator.count() > 0:
                        locator.first.click(timeout=1500)
                except Exception:
                    continue
        page.wait_for_timeout(500)

    def _enrich_listings(self, page, listings: list[ListingDraft]) -> list[ListingDraft]:  # noqa: ANN001
        for listing in listings:
            if listing.address and listing.latitude is not None and listing.longitude is not None:
                continue
            try:
                detail_html = self._load_page_html(page, listing.canonical_url)
            except Exception:
                continue
            enrich_listing_from_detail_html(listing, detail_html)
        return listings

    def scrape_seed(self, client: httpx.Client, source: SourceDefinition, seed_url: str) -> StrategyResult:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            html = ""
            try:
                page = browser.new_page()
                html = self._load_page_html(page, seed_url)
                diagnostics = {"rendered": True, "seed_url": seed_url}
                listings = extract_from_json_blobs(source, collect_json_blobs(html), seed_url)
                if listings:
                    enriched = self._enrich_listings(page, listings)
                    diagnostics["detail_enriched"] = sum(
                        1
                        for listing in enriched
                        if listing.address or listing.latitude is not None or listing.longitude is not None
                    )
                    return StrategyResult(
                        strategy=self.name,
                        listings=enriched,
                        diagnostics=diagnostics,
                    )
            finally:
                browser.close()

        listing = extract_listing_from_detail(source, html, seed_url, seed_url)
        return StrategyResult(
            strategy=self.name,
            listings=[listing] if listing else [],
            diagnostics={"rendered": True, "seed_url": seed_url, "detail_enriched": 1 if listing else 0},
        )
