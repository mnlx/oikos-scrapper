from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
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


@dataclass(slots=True)
class StoredObject:
    bucket: str
    key: str
    endpoint: str
    secure: bool
    size: int
    content_type: str
    checksum_sha256: str

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


@dataclass(slots=True)
class ListingArtifactBundle:
    html: StoredObject | None = None
    screenshot: StoredObject | None = None
    metadata: StoredObject | None = None
    images: list[StoredObject] = field(default_factory=list)


@dataclass(slots=True)
class ParsedListingRecord:
    source_code: str
    external_id: str
    offering_hash: str
    canonical_url: str
    title: str
    transaction_type: str
    property_type: str
    city: str
    state: str
    neighborhood: str | None
    address: str | None
    latitude: Decimal | None
    longitude: Decimal | None
    price_sale: Decimal | None
    price_rent: Decimal | None
    condo_fee: Decimal | None
    iptu: Decimal | None
    bedrooms: int | None
    bathrooms: int | None
    parking_spaces: int | None
    area_m2: Decimal | None
    description: str | None
    broker_name: str | None
    published_at: str | None
    image_uris: list[str]
    screenshot_uri: str | None
    html_uri: str | None
    metadata_uri: str | None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    parsed_at: datetime | None = None
