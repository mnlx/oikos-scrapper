from __future__ import annotations

from decimal import Decimal

import httpx

from oikos_scraper.db.repository import sanitize_json_value


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
