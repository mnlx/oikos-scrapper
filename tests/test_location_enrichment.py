from decimal import Decimal

from oikos_scraper.heuristics import extract_location_fields_from_html
from oikos_scraper.strategies.static_html import enrich_listing_from_detail_html
from oikos_scraper.types import ListingDraft


HTML = """
<html>
  <head>
    <meta property="place:location:latitude" content="-27.5953777" />
    <meta property="place:location:longitude" content="-48.5480499" />
  </head>
  <body>
    <h1>Apartamento no Centro</h1>
    <div class="property-address">Rua Felipe Schmidt, 100 - Centro, Florianopolis - SC</div>
    <div class="bairro">Centro</div>
  </body>
</html>
"""


def test_extract_location_fields_from_html() -> None:
    location = extract_location_fields_from_html(HTML, fallback_city="Florianopolis")

    assert location["address"] == "Rua Felipe Schmidt, 100 - Centro, Florianopolis - SC"
    assert location["neighborhood"] == "Centro"
    assert location["city"] == "Florianopolis"
    assert location["latitude"] == Decimal("-27.5953777")
    assert location["longitude"] == Decimal("-48.5480499")


def test_enrich_listing_from_detail_html_populates_missing_location() -> None:
    listing = ListingDraft(
        source_code="test",
        external_id="test:1",
        canonical_url="https://example.com/imovel/1",
        title="Apartamento no Centro",
        transaction_type="sale",
        property_type="apartment",
        city="Florianopolis",
        raw_payload={},
    )

    enrich_listing_from_detail_html(listing, HTML)

    assert listing.address == "Rua Felipe Schmidt, 100 - Centro, Florianopolis - SC"
    assert listing.neighborhood == "Centro"
    assert listing.latitude == Decimal("-27.5953777")
    assert listing.longitude == Decimal("-48.5480499")
    assert listing.raw_payload["detail_enrichment"]["address"] == listing.address
    assert listing.raw_payload["raw_html"] == HTML
