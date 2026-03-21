from __future__ import annotations

import httpx
from playwright.sync_api import sync_playwright

from oikos_scraper.config import SourceDefinition
from oikos_scraper.heuristics import collect_json_blobs
from oikos_scraper.strategies.base import ScrapeStrategy
from oikos_scraper.strategies.embedded_data import extract_from_json_blobs
from oikos_scraper.strategies.static_html import extract_listing_from_detail
from oikos_scraper.types import ListingDraft, StrategyResult


class BrowserStrategy(ScrapeStrategy):
    name = "browser"

    def scrape_seed(self, client: httpx.Client, source: SourceDefinition, seed_url: str) -> StrategyResult:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(seed_url, wait_until="networkidle", timeout=30000)
            html = page.content()
            browser.close()

        listings = extract_from_json_blobs(source, collect_json_blobs(html), seed_url)
        if listings:
            return StrategyResult(
                strategy=self.name,
                listings=listings,
                diagnostics={"rendered": True, "seed_url": seed_url},
            )

        listing = extract_listing_from_detail(source, html, seed_url, seed_url)
        return StrategyResult(
            strategy=self.name,
            listings=[listing] if listing else [],
            diagnostics={"rendered": True, "seed_url": seed_url},
        )
