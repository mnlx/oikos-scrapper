from __future__ import annotations

from contextlib import contextmanager

from oikos_scraper.config import AppConfig, SourceDefinition
from oikos_scraper.runner import ScrapeRunner
from oikos_scraper.types import ListingDraft, StrategyResult


class DummyStrategy:
    def __init__(self, result: StrategyResult | None = None, exc: Exception | None = None) -> None:
        self.result = result
        self.exc = exc

    def scrape_seed(self, client, source, seed_url):  # noqa: ANN001
        if self.exc is not None:
            raise self.exc
        return self.result


@contextmanager
def dummy_session():  # noqa: ANN201
    yield object()


def build_runner() -> ScrapeRunner:
    source = SourceDefinition(
        code="test",
        name="Test",
        base_url="https://example.com",
        preferred_strategy="embedded_data",
        urls=["https://example.com"],
    )
    config = AppConfig(cities=[], property_types=[], transaction_types=[], sources=[source])
    runner = ScrapeRunner(config, database_url="sqlite://")
    runner.session_factory = dummy_session
    return runner


def test_runner_falls_back_to_browser(monkeypatch) -> None:
    runner = build_runner()
    source = runner.config.find_source("test")
    draft = ListingDraft(
        source_code="test",
        external_id="test:1",
        canonical_url="https://example.com/1",
        title="House",
        description=None,
        transaction_type="sale",
        property_type="house",
        city="Florianopolis",
        state="SC",
        neighborhood=None,
        address=None,
        latitude=None,
        longitude=None,
        price_sale=None,
        price_rent=None,
        condo_fee=None,
        iptu=None,
        bedrooms=None,
        bathrooms=None,
        parking_spaces=None,
        area_m2=None,
        broker_name=None,
        published_at=None,
        raw_payload={},
    )
    runner.strategies = {
        "embedded_data": DummyStrategy(exc=RuntimeError("403 Forbidden")),
        "browser": DummyStrategy(result=StrategyResult(strategy="browser", listings=[draft])),
        "static_html": DummyStrategy(result=StrategyResult(strategy="static_html", listings=[])),
    }

    monkeypatch.setattr("oikos_scraper.runner.create_scrape_run", lambda *args, **kwargs: object())
    monkeypatch.setattr("oikos_scraper.runner.complete_scrape_run", lambda *args, **kwargs: None)
    monkeypatch.setattr("oikos_scraper.runner.upsert_listings", lambda *args, **kwargs: (1, 0))

    summary = runner.scrape_source(source, object(), trigger_type="manual")

    assert summary.strategy == "browser"
    assert summary.items_inserted == 1
    assert summary.error_count == 0


def test_runner_falls_back_to_selenium_after_browser(monkeypatch) -> None:
    runner = build_runner()
    source = runner.config.find_source("test")
    draft = ListingDraft(
        source_code="test",
        external_id="test:2",
        canonical_url="https://example.com/2",
        title="Apartment",
        description=None,
        transaction_type="rent",
        property_type="apartment",
        city="Florianopolis",
        state="SC",
        neighborhood=None,
        address=None,
        latitude=None,
        longitude=None,
        price_sale=None,
        price_rent=None,
        condo_fee=None,
        iptu=None,
        bedrooms=None,
        bathrooms=None,
        parking_spaces=None,
        area_m2=None,
        broker_name=None,
        published_at=None,
        raw_payload={},
    )
    runner.strategies = {
        "embedded_data": DummyStrategy(result=StrategyResult(strategy="embedded_data", listings=[])),
        "static_html": DummyStrategy(result=StrategyResult(strategy="static_html", listings=[])),
        "browser": DummyStrategy(result=StrategyResult(strategy="browser", listings=[])),
        "selenium": DummyStrategy(result=StrategyResult(strategy="selenium", listings=[draft])),
    }

    monkeypatch.setattr("oikos_scraper.runner.create_scrape_run", lambda *args, **kwargs: object())
    monkeypatch.setattr("oikos_scraper.runner.complete_scrape_run", lambda *args, **kwargs: None)
    monkeypatch.setattr("oikos_scraper.runner.upsert_listings", lambda *args, **kwargs: (1, 0))

    summary = runner.scrape_source(source, object(), trigger_type="manual")

    assert summary.strategy == "selenium"
    assert summary.items_inserted == 1
    assert summary.error_count == 0


def test_crawl_listing_pages_respects_depth_limit(monkeypatch) -> None:
    runner = build_runner()
    runner.max_crawl_depth = 2
    runner.max_links_per_page = 10
    runner.max_pages_per_listing = 20
    source = runner.config.find_source("test")
    draft = ListingDraft(
        source_code="test",
        external_id="test:depth",
        canonical_url="https://example.com/root",
        title="Recursive",
        description=None,
        transaction_type="sale",
        property_type="house",
        city="Florianopolis",
        state="SC",
        raw_payload={"raw_html": '<a href="/a">A</a>'},
    )
    html_map = {
        "https://example.com/root": '<a href="/a">A</a>',
        "https://example.com/a": '<a href="/b">B</a>',
        "https://example.com/b": '<a href="/c">C</a>',
        "https://example.com/c": '<a href="/d">D</a>',
    }

    monkeypatch.setattr(
        runner,
        "_fetch_page_html",
        lambda client, url, raw_html=None: raw_html or html_map[url],
    )

    pages = runner._crawl_listing_pages(client=None, source=source, listing=draft)

    assert [page.page_url for page in pages] == [
        "https://example.com/root",
        "https://example.com/a",
        "https://example.com/b",
    ]
    assert [page.depth for page in pages] == [0, 1, 2]


def test_crawl_listing_pages_collects_asset_links(monkeypatch) -> None:
    runner = build_runner()
    source = runner.config.find_source("test")
    draft = ListingDraft(
        source_code="test",
        external_id="test:assets",
        canonical_url="https://example.com/root",
        title="Assets",
        description=None,
        transaction_type="sale",
        property_type="house",
        city="Florianopolis",
        state="SC",
        raw_payload={
            "raw_html": (
                '<a href="/folder/brochure.pdf">PDF</a>'
                '<a href="/next">Next</a>'
                '<img src="/images/home.jpg" />'
            )
        },
    )

    monkeypatch.setattr(
        runner,
        "_fetch_page_html",
        lambda client, url, raw_html=None: raw_html
        or '<a href="/folder/floorplan.pdf">Floorplan</a><img src="/images/room.png" />',
    )

    pages = runner._crawl_listing_pages(client=None, source=source, listing=draft)

    assert pages[0].asset_links == [
        "https://example.com/images/home.jpg",
        "https://example.com/folder/brochure.pdf",
    ]
    assert pages[0].link_urls == ["https://example.com/next"]
