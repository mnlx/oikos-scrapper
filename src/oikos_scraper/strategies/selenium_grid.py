from __future__ import annotations

import httpx
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from oikos_scraper.config import SourceDefinition
from oikos_scraper.heuristics import collect_json_blobs
from oikos_scraper.strategies.base import ScrapeStrategy
from oikos_scraper.strategies.embedded_data import extract_from_json_blobs
from oikos_scraper.strategies.static_html import extract_listing_from_detail
from oikos_scraper.types import StrategyResult


class SeleniumGridStrategy(ScrapeStrategy):
    name = "selenium"

    def __init__(self, remote_url: str) -> None:
        self.remote_url = remote_url

    def scrape_seed(self, client: httpx.Client, source: SourceDefinition, seed_url: str) -> StrategyResult:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")

        driver = webdriver.Remote(command_executor=self.remote_url, options=options)
        try:
            driver.set_page_load_timeout(30)
            driver.get(seed_url)
            html = driver.page_source
        finally:
            driver.quit()

        listings = extract_from_json_blobs(source, collect_json_blobs(html), seed_url)
        if listings:
            return StrategyResult(
                strategy=self.name,
                listings=listings,
                diagnostics={"remote": self.remote_url, "seed_url": seed_url},
            )

        listing = extract_listing_from_detail(source, html, seed_url, seed_url)
        return StrategyResult(
            strategy=self.name,
            listings=[listing] if listing else [],
            diagnostics={"remote": self.remote_url, "seed_url": seed_url},
        )
