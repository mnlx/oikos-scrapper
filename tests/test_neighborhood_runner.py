from __future__ import annotations

from types import SimpleNamespace

from oikos_scraper.bots.neighborhood_signal import NeighborhoodSignalRunner
from oikos_scraper.config import AppConfig


def build_runner() -> NeighborhoodSignalRunner:
    return NeighborhoodSignalRunner(AppConfig(cities=[], property_types=[], transaction_types=[], sources=[]), database_url="sqlite://")


def build_row(source_code: str, *, city: str = "Sao Jose", category: str = "market", source_name: str = "Source"):
    return SimpleNamespace(
        id=1,
        source_code=source_code,
        city=city,
        neighborhood=None,
        geographic_scope="city",
        signal_category=category,
        source_name=source_name,
        source_type="report",
        publisher="Publisher",
        source_url=f"https://example.com/{source_code}",
        reference_date=None,
    )


def test_parse_sao_jose_pmrr_extracts_core_numbers() -> None:
    runner = build_runner()
    row = build_row("sao_jose_pmrr", category="flood_risk", source_name="PMRR")
    html = """
    <html><body>
    O PMRR identificou 33 áreas em situação de risco, totalizando 164 setores.
    Destas, 13 foram classificadas como de alto ou muito alto risco.
    Com base nesse levantamento, a Prefeitura solicitou ao Ministério das Cidades R$ 18 milhões.
    São José está entre as 20 cidades brasileiras selecionadas.
    </body></html>
    """

    parsed = runner._parse_sao_jose_pmrr(row, html)
    codes = {item["signal_code"]: item for item in parsed}

    assert codes["sao_jose_pmrr.areas_risco"]["value_numeric"] == 33
    assert codes["sao_jose_pmrr.setores_risco"]["value_numeric"] == 164
    assert codes["sao_jose_pmrr.alto_risco"]["value_numeric"] == 13
    assert codes["sao_jose_pmrr.obras_contemplacao_solicitada"]["value_numeric"] == 18


def test_parse_biguacu_reurb_extracts_family_count() -> None:
    runner = build_runner()
    row = build_row("biguacu_reurb", city="Biguacu", category="regularization", source_name="REURB")
    html = """
    <html><body>
    O sonho do imóvel com registro público está prestes a se tornar realidade para 96 famílias
    residentes no Núcleo Urbano da Foz do Rio Biguaçu.
    Pela primeira vez a REURB Social é realizada pelo poder público.
    </body></html>
    """

    parsed = runner._parse_biguacu_reurb(row, html)
    codes = {item["signal_code"]: item for item in parsed}

    assert codes["biguacu_reurb.familias_beneficiadas"]["value_numeric"] == 96
    assert codes["biguacu_reurb.registro_publico"]["value_numeric"] == 1


def test_parse_opendatasus_extracts_dataset_counts() -> None:
    runner = build_runner()
    row = build_row("opendatasus_cnes", city="Florianopolis", category="health_access", source_name="OpenDataSUS")
    html = """
    <html><body>
    Arboviroses 4 Conjuntos de dados
    Assistência à saúde 4 Conjuntos de dados
    Atenção Primária 3 Conjuntos de dados
    Diagnósticos e Tratamentos 2 Conjuntos de dados
    Plano de Dados Abertos - Ministério da Saúde - 2024-2026
    </body></html>
    """

    parsed = runner._parse_opendatasus(row, html)
    codes = {item["signal_code"]: item for item in parsed}

    assert codes["opendatasus.assistencia_saude_datasets"]["value_numeric"] == 4
    assert codes["opendatasus.atencao_primaria_datasets"]["value_numeric"] == 3
    assert codes["opendatasus.pda_2024_2026"]["value_numeric"] == 1


def test_extract_neighborhood_asset_links_from_html_and_json() -> None:
    runner = build_runner()
    links = runner._extract_neighborhood_asset_links(
        source_url="https://example.com/report",
        content_type="text/html; charset=utf-8",
        response_text=(
            '<a href="/docs/relatorio.pdf">PDF</a>'
            '<img src="/images/mapa.png" />'
        ),
        payload={
            "attachments": [
                "https://example.com/files/base.csv",
                "https://example.com/page",
            ]
        },
    )

    assert links == [
        "https://example.com/docs/relatorio.pdf",
        "https://example.com/images/mapa.png",
        "https://example.com/files/base.csv",
    ]
