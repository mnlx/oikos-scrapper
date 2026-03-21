from pathlib import Path

import httpx

from oikos_scraper.config import SourceDefinition
from oikos_scraper.strategies.embedded_data import EmbeddedDataStrategy


def test_embedded_data_strategy_extracts_listings() -> None:
    html = Path("tests/fixtures/olx_next_data.html").read_text()
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))
    source = SourceDefinition(
        code="olx",
        name="OLX",
        base_url="https://www.olx.com.br",
        preferred_strategy="embedded_data",
        cities=["Florianopolis"],
        urls=["https://www.olx.com.br"],
    )
    with httpx.Client(transport=transport) as client:
        result = EmbeddedDataStrategy().scrape_seed(client, source, "https://www.olx.com.br")
    assert result.listings
    assert result.listings[0].external_id == "olx-1"
