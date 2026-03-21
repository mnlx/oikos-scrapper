from __future__ import annotations

from abc import ABC, abstractmethod

import httpx

from oikos_scraper.config import SourceDefinition
from oikos_scraper.types import StrategyResult


class ScrapeStrategy(ABC):
    name: str

    @abstractmethod
    def scrape_seed(self, client: httpx.Client, source: SourceDefinition, seed_url: str) -> StrategyResult:
        raise NotImplementedError
