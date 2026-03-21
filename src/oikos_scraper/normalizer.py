from __future__ import annotations

from urllib.parse import urlparse

from oikos_scraper.config import SourceDefinition
from oikos_scraper.heuristics import (
    compact_text,
    detect_property_type,
    detect_transaction_type,
    normalize_city,
    slugify,
)
from oikos_scraper.types import ListingDraft


def build_external_id(source_code: str, url: str, fallback: str | None = None) -> str:
    if fallback:
        return compact_text(str(fallback))
    parsed = urlparse(url)
    tail = parsed.path.rstrip("/").split("/")[-1] or parsed.netloc
    return f"{source_code}:{slugify(tail)}"


def normalize_listing(
    source: SourceDefinition,
    raw: dict,
    seed_url: str,
) -> ListingDraft | None:
    url = compact_text(
        str(raw.get("canonical_url") or raw.get("url") or raw.get("link") or seed_url)
    )
    if not url.startswith("http"):
        return None

    title = compact_text(str(raw.get("title") or raw.get("name") or ""))
    if not title:
        return None

    transaction_type = detect_transaction_type(
        title,
        str(raw.get("transaction_type") or raw.get("business_type") or ""),
        url,
    )
    property_type = detect_property_type(
        title,
        str(raw.get("property_type") or raw.get("unit_type") or ""),
        url,
    )
    city = normalize_city(
        str(raw.get("city") or raw.get("address") or raw.get("neighborhood") or ""),
        fallback=source.cities[0] if source.cities else "Florianopolis",
    )

    return ListingDraft(
        source_code=source.code,
        external_id=build_external_id(source.code, url, raw.get("external_id") or raw.get("id")),
        canonical_url=url,
        title=title,
        transaction_type=transaction_type,
        property_type=property_type,
        city=city,
        state=compact_text(str(raw.get("state") or "SC"))[:2] or "SC",
        neighborhood=compact_text(str(raw.get("neighborhood") or "")) or None,
        address=compact_text(str(raw.get("address") or "")) or None,
        latitude=raw.get("latitude"),
        longitude=raw.get("longitude"),
        price_sale=raw.get("price_sale"),
        price_rent=raw.get("price_rent"),
        condo_fee=raw.get("condo_fee"),
        iptu=raw.get("iptu"),
        bedrooms=raw.get("bedrooms"),
        bathrooms=raw.get("bathrooms"),
        parking_spaces=raw.get("parking_spaces"),
        area_m2=raw.get("area_m2"),
        description=compact_text(str(raw.get("description") or "")) or None,
        broker_name=compact_text(str(raw.get("broker_name") or "")) or None,
        published_at=raw.get("published_at"),
        raw_payload=raw,
    )
