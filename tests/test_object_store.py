from __future__ import annotations

from datetime import UTC, datetime

from oikos_scraper.object_store import BronzePathSpec, offering_hash


def test_bronze_path_spec_uses_medallion_layout() -> None:
    key = BronzePathSpec(
        layer="bronze",
        category="html",
        run_at=datetime(2026, 3, 21, tzinfo=UTC),
        base_hash="abc123",
        extension=".html",
    ).object_key()

    assert key == "bronze/ingestion/listings/html/2026/03/21/abc123.html"


def test_offering_hash_is_stable() -> None:
    assert offering_hash("brognoli", "listing-123") == offering_hash("brognoli", "listing-123")
    assert offering_hash("brognoli", "listing-123") != offering_hash("brognoli", "listing-456")
