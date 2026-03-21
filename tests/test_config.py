from oikos_scraper.config import load_config


def test_load_config() -> None:
    config = load_config("config/sources.yaml")
    assert len(config.sources) == 10
    assert config.find_source("olx").preferred_strategy == "embedded_data"
