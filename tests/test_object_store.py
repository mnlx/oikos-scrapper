from __future__ import annotations

from datetime import UTC, datetime

from oikos_scraper.object_store import BronzePathSpec, offering_hash
from oikos_scraper.types import StoredObject


def test_bronze_path_spec_uses_medallion_layout() -> None:
    key = BronzePathSpec(
        layer="bronze",
        category="html",
        run_at=datetime(2026, 3, 21, tzinfo=UTC),
        base_hash="abc123",
        extension=".html",
    ).object_key()

    assert key == "bronze/ingestion/listings/html/2026/03/21/abc123.html"


def test_bronze_path_spec_supports_datasets() -> None:
    key = BronzePathSpec(
        layer="bronze",
        dataset="neighborhood_signal",
        category="json",
        run_at=datetime(2026, 3, 21, tzinfo=UTC),
        base_hash="abc123",
        extension=".json",
    ).object_key()

    assert key == "bronze/ingestion/neighborhood_signal/json/2026/03/21/abc123.json"


def test_offering_hash_is_stable() -> None:
    assert offering_hash("brognoli", "listing-123") == offering_hash("brognoli", "listing-123")
    assert offering_hash("brognoli", "listing-123") != offering_hash("brognoli", "listing-456")


def test_stored_object_exposes_object_uri_alias() -> None:
    stored = StoredObject(
        bucket="datalake",
        key="bronze/ingestion/listings/html/2026/03/21/abc123.html",
        endpoint="127.0.0.1:9000",
        secure=False,
        size=10,
        content_type="text/html",
        checksum_sha256="abc",
    )

    assert stored.object_uri == stored.uri
