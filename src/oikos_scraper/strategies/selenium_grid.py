from __future__ import annotations

import httpx
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from oikos_scraper.config import SourceDefinition
from oikos_scraper.heuristics import collect_json_blobs
from oikos_scraper.strategies.base import ScrapeStrategy
from oikos_scraper.strategies.embedded_data import extract_from_json_blobs
from oikos_scraper.strategies.static_html import enrich_listing_from_detail_html, extract_listing_from_detail
from oikos_scraper.types import ListingDraft, StrategyResult


class SeleniumGridStrategy(ScrapeStrategy):
    name = "selenium"

    def __init__(self, remote_url: str) -> None:
        self.remote_url = remote_url

    def _reveal_location(self, driver) -> None:  # noqa: ANN001
        selectors = (
            "//*[self::a or self::button or @role='button'][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÀÃÂÉÊÍÓÔÕÚÇ', 'abcdefghijklmnopqrstuvwxyzáàãâéêíóôõúç'), 'mapa')]",
            "//*[self::a or self::button or @role='button'][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÀÃÂÉÊÍÓÔÕÚÇ', 'abcdefghijklmnopqrstuvwxyzáàãâéêíóôõúç'), 'localização') or contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÀÃÂÉÊÍÓÔÕÚÇ', 'abcdefghijklmnopqrstuvwxyzáàãâéêíóôõúç'), 'localizacao')]",
            "//*[self::a or self::button or @role='button'][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÀÃÂÉÊÍÓÔÕÚÇ', 'abcdefghijklmnopqrstuvwxyzáàãâéêíóôõúç'), 'endereço') or contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÀÃÂÉÊÍÓÔÕÚÇ', 'abcdefghijklmnopqrstuvwxyzáàãâéêíóôõúç'), 'endereco')]",
        )
        for selector in selectors:
            try:
                elements = driver.find_elements(By.XPATH, selector)[:2]
            except Exception:
                continue
            for element in elements:
                try:
                    driver.execute_script("arguments[0].click();", element)
                except Exception:
                    continue

    def _load_page_html(self, driver, url: str) -> str:  # noqa: ANN001
        driver.get(url)
        self._reveal_location(driver)
        return driver.page_source

    def _enrich_listings(self, driver, listings: list[ListingDraft]) -> list[ListingDraft]:  # noqa: ANN001
        for listing in listings:
            if listing.address and listing.latitude is not None and listing.longitude is not None:
                continue
            try:
                detail_html = self._load_page_html(driver, listing.canonical_url)
            except Exception:
                continue
            enrich_listing_from_detail_html(listing, detail_html)
        return listings

    def scrape_seed(self, client: httpx.Client, source: SourceDefinition, seed_url: str) -> StrategyResult:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")

        driver = webdriver.Remote(command_executor=self.remote_url, options=options)
        html = ""
        try:
            driver.set_page_load_timeout(30)
            html = self._load_page_html(driver, seed_url)
            diagnostics = {"remote": self.remote_url, "seed_url": seed_url}
            listings = extract_from_json_blobs(source, collect_json_blobs(html), seed_url)
            if listings:
                enriched = self._enrich_listings(driver, listings)
                diagnostics["detail_enriched"] = sum(
                    1 for listing in enriched if listing.address or listing.latitude is not None or listing.longitude is not None
                )
                return StrategyResult(
                    strategy=self.name,
                    listings=enriched,
                    diagnostics=diagnostics,
                )
        finally:
            driver.quit()

        listing = extract_listing_from_detail(source, html, seed_url, seed_url)
        return StrategyResult(
            strategy=self.name,
            listings=[listing] if listing else [],
            diagnostics={"remote": self.remote_url, "seed_url": seed_url, "detail_enriched": 1 if listing else 0},
        )
