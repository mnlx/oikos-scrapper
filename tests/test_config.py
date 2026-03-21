from oikos_scraper.config import load_config


def test_load_config() -> None:
    config = load_config("config/sources.yaml")
    assert len(config.sources) == 20
    assert config.find_source("olx").preferred_strategy == "embedded_data"
    assert config.find_source("brognoli").base_url == "https://www.brognoli.com.br"
    assert config.find_source("help_imoveis").base_url == "https://www.helpimoveis.com.br"
    assert config.find_source("wkoerich_empreendimentos").group == "developer"
    assert len(config.active_sources(group="developer")) == 3
