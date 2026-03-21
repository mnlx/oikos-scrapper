from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class SourceDefinition(BaseModel):
    code: str
    name: str
    base_url: str
    active: bool = True
    preferred_strategy: str = "embedded_data"
    cities: list[str] = Field(default_factory=list)
    urls: list[str] = Field(default_factory=list)


class AppConfig(BaseModel):
    cities: list[str]
    property_types: list[str]
    transaction_types: list[str]
    sources: list[SourceDefinition]

    def active_sources(self) -> list[SourceDefinition]:
        return [source for source in self.sources if source.active]

    def find_source(self, code: str) -> SourceDefinition:
        for source in self.sources:
            if source.code == code:
                return source
        raise KeyError(f"Unknown source: {code}")


def load_config(path: str | Path) -> AppConfig:
    raw = yaml.safe_load(Path(path).read_text())
    return AppConfig.model_validate(raw)
