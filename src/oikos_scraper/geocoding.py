from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import httpx

from oikos_scraper.types import GeocodeResult


def build_listing_geocode_query(
    *,
    address: str | None,
    neighborhood: str | None,
    city: str | None,
    state: str | None,
    country: str = "Brazil",
) -> str | None:
    location_parts = [
        (address or "").strip(),
        (neighborhood or "").strip(),
        (city or "").strip(),
        (state or "").strip(),
    ]
    if not any(location_parts):
        return None

    parts = [
        *location_parts,
        country.strip(),
    ]
    query = ", ".join(part for part in parts if part)
    return query or None


@dataclass(slots=True)
class NominatimGeocoder:
    endpoint: str
    user_agent: str
    accept_language: str = "pt-BR,pt;q=0.9,en;q=0.8"
    rate_limit_seconds: float = 1.1
    provider: str = "nominatim"
    country: str = "Brazil"
    _last_request_started_at: float = field(init=False, default=0.0)

    def __post_init__(self) -> None:
        self.endpoint = self.endpoint.rstrip("/")

    def _respect_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_started_at
        if elapsed < self.rate_limit_seconds:
            time.sleep(self.rate_limit_seconds - elapsed)

    def geocode_listing(
        self,
        client: httpx.Client,
        *,
        address: str | None,
        neighborhood: str | None,
        city: str | None,
        state: str | None,
    ) -> GeocodeResult | None:
        query = build_listing_geocode_query(
            address=address,
            neighborhood=neighborhood,
            city=city,
            state=state,
            country=self.country,
        )
        if query is None:
            return None

        self._respect_rate_limit()
        self._last_request_started_at = time.monotonic()
        response = client.get(
            f"{self.endpoint}/search",
            params={
                "q": query,
                "format": "jsonv2",
                "limit": 1,
                "addressdetails": 1,
            },
            headers={
                "User-Agent": self.user_agent,
                "Accept-Language": self.accept_language,
            },
        )
        response.raise_for_status()
        rows = response.json()
        if not rows:
            return None

        row = rows[0]
        confidence = None
        if row.get("importance") is not None:
            try:
                confidence = Decimal(str(row["importance"]))
            except Exception:
                confidence = None

        payload: dict[str, Any] = {
            "place_id": row.get("place_id"),
            "osm_type": row.get("osm_type"),
            "osm_id": row.get("osm_id"),
            "class": row.get("class"),
            "type": row.get("type"),
            "display_name": row.get("display_name"),
            "address": row.get("address"),
            "importance": row.get("importance"),
        }
        return GeocodeResult(
            query=query,
            provider=self.provider,
            latitude=Decimal(str(row["lat"])),
            longitude=Decimal(str(row["lon"])),
            confidence=confidence,
            display_name=row.get("display_name"),
            payload=payload,
        )
