from __future__ import annotations

from contextlib import contextmanager

from oikos_scraper.config import AppConfig, SourceDefinition
from oikos_scraper.ingest_cache import NullIngestCache, RedisError, normalize_page_url
from oikos_scraper.normalizer import normalize_listing
from oikos_scraper.runner import ScrapeRunner
from oikos_scraper.strategies.static_html import extract_listing_from_detail
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
    runner.ingest_cache = NullIngestCache()
    return runner


class FakeIngestCache:
    def __init__(
        self,
        listing_reserve_outcomes: list[bool] | None = None,
        page_reserve_outcomes: list[bool] | None = None,
    ) -> None:
        self.listing_reserve_outcomes = list(listing_reserve_outcomes or [])
        self.page_reserve_outcomes = list(page_reserve_outcomes or [])
        self.listing_reserve_calls: list[tuple[str, str]] = []
        self.page_reserve_calls: list[tuple[str, str]] = []
        self.listing_release_calls: list[tuple[str, str]] = []
        self.page_release_calls: list[tuple[str, str]] = []

    def reserve_listing(self, source_code: str, external_id: str) -> bool:
        self.listing_reserve_calls.append((source_code, external_id))
        if self.listing_reserve_outcomes:
            return self.listing_reserve_outcomes.pop(0)
        return True

    def reserve_page(self, source_code: str, page_url: str) -> bool:
        self.page_reserve_calls.append((source_code, page_url))
        if self.page_reserve_outcomes:
            return self.page_reserve_outcomes.pop(0)
        return True

    def release_listing(self, source_code: str, external_id: str) -> None:
        self.listing_release_calls.append((source_code, external_id))

    def release_page(self, source_code: str, page_url: str) -> None:
        self.page_release_calls.append((source_code, page_url))


class FailingIngestCache(FakeIngestCache):
    def reserve_listing(self, source_code: str, external_id: str) -> bool:
        raise RedisError("cache unavailable")


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


def test_normalize_listing_extracts_created_and_updated_dates() -> None:
    source = build_runner().config.find_source("test")
    listing = normalize_listing(
        source,
        {
            "id": "abc",
            "url": "https://example.com/abc",
            "title": "Casa com patio",
            "city": "Florianopolis",
            "createdAt": "2026-03-20T10:15:00Z",
            "updated_at": "2026-03-21T13:45:00+00:00",
            "published_at": "2026-03-19T08:00:00Z",
        },
        "https://example.com",
    )

    assert listing is not None
    assert listing.published_at == "2026-03-19T08:00:00+00:00"
    assert listing.listing_created_at == "2026-03-20T10:15:00+00:00"
    assert listing.listing_updated_at == "2026-03-21T13:45:00+00:00"


def test_extract_listing_from_detail_reads_html_dates() -> None:
    source = build_runner().config.find_source("test")
    listing = extract_listing_from_detail(
        source,
        """
        <html>
          <head>
            <meta property="article:published_time" content="2026-03-18T09:00:00Z" />
            <meta property="article:modified_time" content="2026-03-20T11:30:00Z" />
          </head>
          <body>
            <h1>Casa perto da praia</h1>
            <div class="address">Rua das Palmeiras, 10</div>
          </body>
        </html>
        """,
        "https://example.com/imovel/1",
        "https://example.com",
    )

    assert listing is not None
    assert listing.published_at == "2026-03-18T09:00:00+00:00"
    assert listing.listing_created_at == "2026-03-18T09:00:00+00:00"
    assert listing.listing_updated_at == "2026-03-20T11:30:00+00:00"


def test_ingest_source_skips_cached_pages(monkeypatch) -> None:
    runner = build_runner()
    source = runner.config.find_source("test")
    listing = ListingDraft(
        source_code="test",
        external_id="test:cache",
        canonical_url="https://example.com/1",
        title="Cached",
        description=None,
        transaction_type="sale",
        property_type="house",
        city="Florianopolis",
        state="SC",
        raw_payload={},
    )
    runner.ingest_cache = FakeIngestCache(listing_reserve_outcomes=[False])

    monkeypatch.setattr(runner, "_discover_with_fallbacks", lambda source: ("static_html", [listing], {}))
    monkeypatch.setattr(
        runner,
        "_crawl_listing_pages",
        lambda client, source, listing: [type("Page", (), {
            "page_url": "https://example.com/1",
            "parent_page_url": None,
            "depth": 0,
            "image_urls": [],
            "asset_links": [],
            "link_urls": [],
            "html": "<html></html>",
        })()],
    )
    monkeypatch.setattr("oikos_scraper.runner.create_scrape_run", lambda *args, **kwargs: object())
    monkeypatch.setattr("oikos_scraper.runner.complete_scrape_run", lambda *args, **kwargs: None)

    def fail_store_bundle(**kwargs):  # noqa: ANN003
        raise AssertionError("bundle should not be stored on cache hit")

    monkeypatch.setattr(runner, "_store_bundle", fail_store_bundle)
    monkeypatch.setattr("oikos_scraper.runner.upsert_listing_ingestion", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not upsert")))
    monkeypatch.setattr("oikos_scraper.runner.replace_listing_artifacts", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not replace artifacts")))

    summary = runner.ingest_source(source, object(), trigger_type="manual")

    assert summary.ingestions_upserted == 0
    assert summary.cached_skips == 1
    assert runner.ingest_cache.listing_reserve_calls == [("test", "test:cache")]


def test_ingest_source_releases_cache_key_on_store_failure(monkeypatch) -> None:
    runner = build_runner()
    source = runner.config.find_source("test")
    listing = ListingDraft(
        source_code="test",
        external_id="test:fail",
        canonical_url="https://example.com/2",
        title="Failure",
        description=None,
        transaction_type="sale",
        property_type="house",
        city="Florianopolis",
        state="SC",
        raw_payload={},
    )
    cache = FakeIngestCache(listing_reserve_outcomes=[True])
    runner.ingest_cache = cache

    monkeypatch.setattr(runner, "_discover_with_fallbacks", lambda source: ("static_html", [listing], {}))
    monkeypatch.setattr(
        runner,
        "_crawl_listing_pages",
        lambda client, source, listing: [type("Page", (), {
            "page_url": "https://example.com/2",
            "parent_page_url": None,
            "depth": 0,
            "image_urls": [],
            "asset_links": [],
            "link_urls": [],
            "html": "<html></html>",
        })()],
    )
    monkeypatch.setattr("oikos_scraper.runner.create_scrape_run", lambda *args, **kwargs: object())
    monkeypatch.setattr("oikos_scraper.runner.complete_scrape_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(runner, "_store_bundle", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    summary = runner.ingest_source(source, object(), trigger_type="manual")

    assert summary.error_count == 1
    assert cache.listing_release_calls == [("test", "test:fail")]


def test_ingest_source_fails_open_when_cache_errors(monkeypatch) -> None:
    runner = build_runner()
    source = runner.config.find_source("test")
    listing = ListingDraft(
        source_code="test",
        external_id="test:open",
        canonical_url="https://example.com/3",
        title="Open",
        description=None,
        transaction_type="sale",
        property_type="house",
        city="Florianopolis",
        state="SC",
        raw_payload={},
    )
    runner.ingest_cache = FailingIngestCache()

    page = type("Page", (), {
        "page_url": "https://example.com/3",
        "parent_page_url": None,
        "depth": 0,
        "image_urls": [],
        "asset_links": [],
        "link_urls": [],
        "html": "<html></html>",
    })()
    bundle = type("Bundle", (), {"html": object(), "screenshot": None, "metadata": object()})()

    monkeypatch.setattr(runner, "_discover_with_fallbacks", lambda source: ("static_html", [listing], {}))
    monkeypatch.setattr(runner, "_crawl_listing_pages", lambda client, source, listing: [page])
    monkeypatch.setattr(runner, "_store_bundle", lambda **kwargs: bundle)
    monkeypatch.setattr("oikos_scraper.runner.create_scrape_run", lambda *args, **kwargs: object())
    monkeypatch.setattr("oikos_scraper.runner.complete_scrape_run", lambda *args, **kwargs: None)
    monkeypatch.setattr("oikos_scraper.runner.upsert_listing_ingestion", lambda *args, **kwargs: object())
    monkeypatch.setattr("oikos_scraper.runner.replace_listing_artifacts", lambda *args, **kwargs: None)

    summary = runner.ingest_source(source, object(), trigger_type="manual")

    assert summary.ingestions_upserted == 1
    assert summary.cached_skips == 0


def test_ingest_source_writes_without_page_cache_gate(monkeypatch) -> None:
    runner = build_runner()
    source = runner.config.find_source("test")
    listing = ListingDraft(
        source_code="test",
        external_id="test:no-page-cache",
        canonical_url="https://example.com/4",
        title="No page cache",
        description=None,
        transaction_type="sale",
        property_type="house",
        city="Florianopolis",
        state="SC",
        raw_payload={},
    )
    runner.ingest_cache = FakeIngestCache(listing_reserve_outcomes=[True])
    bundle = type("Bundle", (), {"html": object(), "screenshot": None, "metadata": object()})()

    monkeypatch.setattr(runner, "_discover_with_fallbacks", lambda source: ("static_html", [listing], {}))
    monkeypatch.setattr(
        runner,
        "_crawl_listing_pages",
        lambda client, source, listing: [type("Page", (), {
            "page_url": "https://example.com/4",
            "parent_page_url": None,
            "depth": 0,
            "image_urls": [],
            "asset_links": [],
            "link_urls": [],
            "html": "<html></html>",
        })()],
    )
    monkeypatch.setattr("oikos_scraper.runner.create_scrape_run", lambda *args, **kwargs: object())
    monkeypatch.setattr("oikos_scraper.runner.complete_scrape_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(runner, "_store_bundle", lambda **kwargs: bundle)
    monkeypatch.setattr("oikos_scraper.runner.upsert_listing_ingestion", lambda *args, **kwargs: object())
    monkeypatch.setattr("oikos_scraper.runner.replace_listing_artifacts", lambda *args, **kwargs: None)

    summary = runner.ingest_source(source, object(), trigger_type="manual")

    assert summary.ingestions_upserted == 1
    assert summary.cached_skips == 0
    assert runner.ingest_cache.page_reserve_calls == []


def test_ingest_source_skips_follow_pages(monkeypatch) -> None:
    runner = build_runner()
    source = runner.config.find_source("test")
    listing = ListingDraft(
        source_code="test",
        external_id="test:follow-pages",
        canonical_url="https://example.com/5",
        title="Follow pages",
        description=None,
        transaction_type="sale",
        property_type="house",
        city="Florianopolis",
        state="SC",
        raw_payload={},
    )
    cache = FakeIngestCache(listing_reserve_outcomes=[True])
    runner.ingest_cache = cache

    root_page = type("Page", (), {
        "page_url": "https://example.com/5",
        "parent_page_url": None,
        "depth": 0,
        "image_urls": [],
        "asset_links": ["https://example.com/assets/root.pdf"],
        "link_urls": ["https://example.com/extra"],
        "html": "<html></html>",
    })()
    child_page = type("Page", (), {
        "page_url": "https://example.com/extra",
        "parent_page_url": "https://example.com/5",
        "depth": 1,
        "image_urls": [],
        "asset_links": ["https://example.com/assets/extra.pdf"],
        "link_urls": [],
        "html": "<html></html>",
    })()
    bundle = type("Bundle", (), {"html": object(), "screenshot": None, "metadata": object()})()
    upsert_calls: list[dict] = []

    monkeypatch.setattr(runner, "_discover_with_fallbacks", lambda source: ("static_html", [listing], {}))
    monkeypatch.setattr(runner, "_crawl_listing_pages", lambda client, source, listing: [root_page, child_page])
    monkeypatch.setattr(runner, "_store_bundle", lambda **kwargs: bundle)
    monkeypatch.setattr("oikos_scraper.runner.create_scrape_run", lambda *args, **kwargs: object())
    monkeypatch.setattr("oikos_scraper.runner.complete_scrape_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "oikos_scraper.runner.upsert_listing_ingestion",
        lambda *args, **kwargs: upsert_calls.append(kwargs) or object(),
    )
    monkeypatch.setattr("oikos_scraper.runner.replace_listing_artifacts", lambda *args, **kwargs: None)

    summary = runner.ingest_source(source, object(), trigger_type="manual")

    assert summary.ingestions_upserted == 1
    assert len(upsert_calls) == 1
    assert upsert_calls[0]["page_url"] == "https://example.com/5"
    assert upsert_calls[0]["asset_links"] == [
        "https://example.com/assets/root.pdf",
        "https://example.com/assets/extra.pdf",
    ]
    assert cache.page_reserve_calls == []


def test_normalize_page_url_canonicalizes_fragment_and_slash() -> None:
    assert normalize_page_url("HTTPS://Example.com:443/apto/?a=1&b=2#top") == "https://example.com/apto?a=1&b=2"
