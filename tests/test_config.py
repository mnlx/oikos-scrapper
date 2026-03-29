from oikos_scraper.config import load_config


def test_load_config() -> None:
    config = load_config("bots/realestate-listings/sources.yaml")
    assert len(config.sources) == 46
    assert config.find_source("olx").preferred_strategy == "embedded_data"
    assert config.find_source("olx").block is True
    assert config.find_source("quintoandar").preferred_strategy == "browser"
    assert config.find_source("quintoandar").block is True
    assert config.find_source("brognoli").base_url == "https://www.brognoli.com.br"
    assert config.find_source("help_imoveis").base_url == "https://www.helpimoveis.com.br"
    assert config.find_source("wkoerich_empreendimentos").group == "developer"
    assert config.find_source("gralha_imoveis").base_url == "https://www.gralhaimoveis.com.br"
    assert config.find_source("dimas_construcoes").group == "developer"
    assert len(config.active_sources(group="developer")) == 13


def test_load_neighborhood_config() -> None:
    config = load_config("bots/neighborhood-signal/sources.yaml")
    assert len(config.sources) >= 6
    assert config.find_source("geofloripa").signal_category == "zoning"
    assert config.find_source("ssp_sc_seguranca_numeros").source_type == "report"
