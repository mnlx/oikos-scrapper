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


def test_embedded_data_strategy_prefers_richer_priced_objects() -> None:
    html = """
    <html>
      <head>
        <script type="application/json">
          {
            "schema": {
              "@type": "Residence",
              "name": "Apartamento com 2 quartos",
              "url": "https://www.quintoandar.com.br/imovel/894468802/alugar/apartamento-2-quartos-joao-paulo-florianopolis",
              "address": "Rua Julio Vieira, Joao Paulo, Florianopolis"
            },
            "listing": {
              "id": "894468802",
              "url": "https://www.quintoandar.com.br/imovel/894468802/alugar/apartamento-2-quartos-joao-paulo-florianopolis",
              "title": "Apartamento com 2 quartos, 78m2 em Joao Paulo, Florianopolis",
              "city": "Florianopolis",
              "address": "Rua Julio Vieira, Joao Paulo, Florianopolis",
              "rentPrice": 7000,
              "condoPrice": 1100,
              "bedrooms": 2,
              "bathrooms": 2,
              "area": 78
            }
          }
        </script>
      </head>
    </html>
    """
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))
    source = SourceDefinition(
        code="quintoandar",
        name="QuintoAndar",
        base_url="https://www.quintoandar.com.br",
        preferred_strategy="embedded_data",
        cities=["Florianopolis"],
        urls=["https://www.quintoandar.com.br"],
    )
    with httpx.Client(transport=transport) as client:
        result = EmbeddedDataStrategy().scrape_seed(client, source, "https://www.quintoandar.com.br")

    assert result.listings
    assert result.listings[0].external_id == "894468802"
    assert result.listings[0].price_rent == 7000
    assert result.listings[0].condo_fee == 1100
