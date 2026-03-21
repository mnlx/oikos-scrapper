from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass(slots=True)
class ListingDraft:
    source_code: str
    external_id: str
    canonical_url: str
    title: str
    transaction_type: str
    property_type: str
    city: str
    state: str = "SC"
    neighborhood: str | None = None
    address: str | None = None
    latitude: Decimal | None = None
    longitude: Decimal | None = None
    price_sale: Decimal | None = None
    price_rent: Decimal | None = None
    condo_fee: Decimal | None = None
    iptu: Decimal | None = None
    bedrooms: int | None = None
    bathrooms: int | None = None
    parking_spaces: int | None = None
    area_m2: Decimal | None = None
    description: str | None = None
    broker_name: str | None = None
    published_at: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class StrategyResult:
    strategy: str
    listings: list[ListingDraft]
    diagnostics: dict[str, Any] = field(default_factory=dict)
