from __future__ import annotations

from datetime import UTC, datetime
import re
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


CREATED_AT_KEYS = (
    "listing_created_at",
    "created_at",
    "createdAt",
    "creation_date",
    "dateCreated",
    "created",
)
UPDATED_AT_KEYS = (
    "listing_updated_at",
    "updated_at",
    "updatedAt",
    "modified_at",
    "modifiedAt",
    "last_updated_at",
    "lastUpdated",
    "dateModified",
    "updated",
)
PUBLISHED_AT_KEYS = (
    "published_at",
    "publishedAt",
    "datePublished",
    "publication_date",
)
NUMERIC_TIMESTAMP_RE = re.compile(r"^\d{10,13}$")


def build_external_id(source_code: str, url: str, fallback: str | None = None) -> str:
    if fallback:
        return compact_text(str(fallback))
    parsed = urlparse(url)
    tail = parsed.path.rstrip("/").split("/")[-1] or parsed.netloc
    return f"{source_code}:{slugify(tail)}"


def _normalize_datetime_candidate(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.isoformat()
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 1_000_000_000_000:
            seconds /= 1000.0
        try:
            return datetime.fromtimestamp(seconds, tz=UTC).isoformat()
        except Exception:
            return None
    text = compact_text(str(value))
    if not text:
        return None
    if NUMERIC_TIMESTAMP_RE.match(text):
        return _normalize_datetime_candidate(int(text))
    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.isoformat()


def _walk_values(value: object):  # noqa: ANN202
    if isinstance(value, dict):
        for item in value.values():
            yield item
            yield from _walk_values(item)
    elif isinstance(value, list):
        for item in value:
            yield item
            yield from _walk_values(item)


def _find_timestamp(raw: dict, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = raw.get(key)
        normalized = _normalize_datetime_candidate(value)
        if normalized:
            return normalized

    lowercase_keys = {key.lower() for key in keys}
    for nested in _walk_values(raw):
        if not isinstance(nested, dict):
            continue
        for key, value in nested.items():
            if str(key).lower() not in lowercase_keys:
                continue
            normalized = _normalize_datetime_candidate(value)
            if normalized:
                return normalized
    return None


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
    published_at = _find_timestamp(raw, PUBLISHED_AT_KEYS)
    listing_created_at = _find_timestamp(raw, CREATED_AT_KEYS)
    listing_updated_at = _find_timestamp(raw, UPDATED_AT_KEYS)

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
        published_at=published_at,
        listing_created_at=listing_created_at,
        listing_updated_at=listing_updated_at,
        raw_payload=raw,
    )
