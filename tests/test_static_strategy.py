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


def test_static_html_strategy_ignores_offsite_and_navigation_links() -> None:
    grid_html = """
    <html>
      <body>
        <a href="https://www.google.com/search?q=imoveis">Google</a>
        <a href="/sobre">Sobre</a>
        <a href="/imovel/apartamento-centro-codigo-123">Detalhe</a>
      </body>
    </html>
    """
    detail_html = Path("tests/fixtures/local_listing_detail.html").read_text()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/imoveis":
            return httpx.Response(200, text=grid_html)
        return httpx.Response(200, text=detail_html)

    transport = httpx.MockTransport(handler)
    source = SourceDefinition(
        code="test",
        name="Test",
        base_url="https://example.com",
        preferred_strategy="static_html",
        cities=["Florianopolis"],
        urls=["https://example.com/imoveis"],
    )
    with httpx.Client(transport=transport) as client:
        result = StaticHTMLStrategy().scrape_seed(client, source, source.urls[0])

    assert [listing.canonical_url for listing in result.listings] == [
        "https://example.com/imovel/apartamento-centro-codigo-123"
    ]
