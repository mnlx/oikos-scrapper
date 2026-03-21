from __future__ import annotations

from decimal import Decimal

import httpx

from oikos_scraper.db.repository import persist_raw_html_payload, sanitize_json_value
from oikos_scraper.types import ListingDraft


def test_sanitize_json_value_handles_nested_non_json_types() -> None:
    payload = {
        "url": httpx.URL("https://example.com/property/1"),
        "price": Decimal("123.45"),
        "nested": [{"area": Decimal("70.5")}],
    }

    sanitized = sanitize_json_value(payload)

    assert sanitized == {
        "url": "https://example.com/property/1",
        "price": "123.45",
        "nested": [{"area": "70.5"}],
    }


def test_persist_raw_html_payload_uploads_to_object_store(monkeypatch) -> None:
    class DummyStore:
        def upload_listing_html(self, listing, html):  # noqa: ANN001
            assert listing.external_id == "test:1"
            assert html == "<html>hello</html>"
            return type(
                "Uploaded",
                (),
                {
                    "bucket": "oikos-raw-html",
                    "key": "test/florianopolis/test-1.html",
                    "endpoint": "minio.minio.svc.cluster.local:9000",
                    "secure": False,
                    "size": len(html.encode("utf-8")),
                },
            )()

    monkeypatch.setattr("oikos_scraper.db.repository.get_raw_html_store", lambda: DummyStore())
    listing = ListingDraft(
        source_code="test",
        external_id="test:1",
        canonical_url="https://example.com/imovel/1",
        title="House",
        transaction_type="sale",
        property_type="house",
        city="Florianopolis",
        raw_payload={"raw_html": "<html>hello</html>", "foo": "bar"},
    )

    payload = persist_raw_html_payload(listing)

    assert "raw_html" not in payload
    assert payload["foo"] == "bar"
    assert payload["raw_html_object"]["bucket"] == "oikos-raw-html"
