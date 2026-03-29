from __future__ import annotations

from oikos_scraper.geocoding import build_listing_geocode_query


def test_build_listing_geocode_query_joins_non_empty_parts() -> None:
    query = build_listing_geocode_query(
        address="Rua João Pio Duarte Silva, 123",
        neighborhood="Córrego Grande",
        city="Florianópolis",
        state="SC",
    )

    assert query == "Rua João Pio Duarte Silva, 123, Córrego Grande, Florianópolis, SC, Brazil"


def test_build_listing_geocode_query_returns_none_when_no_location_data() -> None:
    query = build_listing_geocode_query(
        address=None,
        neighborhood="",
        city=None,
        state="",
    )

    assert query is None
