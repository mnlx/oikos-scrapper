from pathlib import Path

import httpx

from oikos_scraper.config import SourceDefinition
from oikos_scraper.strategies.static_html import StaticHTMLStrategy


def test_static_html_strategy_follows_detail_links() -> None:
    grid_html = Path("tests/fixtures/local_listing_grid.html").read_text()
    detail_html = Path("tests/fixtures/local_listing_detail.html").read_text()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/imoveis":
            return httpx.Response(200, text=grid_html)
        return httpx.Response(200, text=detail_html)

    transport = httpx.MockTransport(handler)
    source = SourceDefinition(
        code="pagani_imoveis",
        name="Pagani Imoveis",
        base_url="https://www.paganiimoveis.com.br",
        preferred_strategy="static_html",
        cities=["Palhoca"],
        urls=["https://www.paganiimoveis.com.br/imoveis"],
    )
    with httpx.Client(transport=transport) as client:
        result = StaticHTMLStrategy().scrape_seed(client, source, source.urls[0])
    assert result.listings
    assert result.listings[0].city == "Palhoca"
    assert result.listings[0].bedrooms == 3
