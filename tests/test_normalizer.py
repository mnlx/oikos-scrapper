from decimal import Decimal

from oikos_scraper.config import SourceDefinition
from oikos_scraper.normalizer import build_external_id, normalize_listing


def test_build_external_id_from_url() -> None:
    assert build_external_id("olx", "https://example.com/imovel/casa-123") == "olx:casa-123"


def test_normalize_listing_defaults() -> None:
    source = SourceDefinition(
        code="olx",
        name="OLX",
        base_url="https://www.olx.com.br",
        cities=["Florianopolis"],
        urls=["https://www.olx.com.br"],
    )
    listing = normalize_listing(
        source,
        {
            "canonical_url": "https://www.olx.com.br/d/imovel/casa-123",
            "title": "Casa com piscina em Florianopolis",
            "price_sale": Decimal("123.45"),
        },
        "https://www.olx.com.br",
    )
    assert listing is not None
    assert listing.city == "Florianopolis"
    assert listing.property_type == "house"
    assert listing.transaction_type == "sale"
