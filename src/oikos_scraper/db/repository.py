from __future__ import annotations

from decimal import Decimal
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from oikos_scraper.config import SourceDefinition
from oikos_scraper.db.models import Listing, ScrapeRun, Source
from oikos_scraper.types import ListingDraft


def sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): sanitize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_json_value(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def ensure_sources(session: Session, sources: list[SourceDefinition]) -> dict[str, Source]:
    existing = {
        source.code: source
        for source in session.execute(select(Source)).scalars().all()
    }
    for definition in sources:
        source = existing.get(definition.code)
        if source is None:
            source = Source(
                code=definition.code,
                name=definition.name,
                base_url=definition.base_url,
                active=definition.active,
            )
            session.add(source)
            existing[definition.code] = source
        else:
            source.name = definition.name
            source.base_url = definition.base_url
            source.active = definition.active
    session.commit()
    return existing


def create_scrape_run(session: Session, source_code: str, trigger_type: str, strategy: str) -> ScrapeRun:
    run = ScrapeRun(
        started_at=datetime.now(UTC),
        trigger_type=trigger_type,
        status="running",
        source_code=source_code,
        strategy=strategy,
        items_seen=0,
        items_inserted=0,
        items_updated=0,
        error_count=0,
    )
    session.add(run)
    session.commit()
    return run


def complete_scrape_run(
    session: Session,
    run: ScrapeRun,
    *,
    status: str,
    items_seen: int,
    items_inserted: int,
    items_updated: int,
    error_count: int,
    last_error: str | None = None,
) -> None:
    run.finished_at = datetime.now(UTC)
    run.status = status
    run.items_seen = items_seen
    run.items_inserted = items_inserted
    run.items_updated = items_updated
    run.error_count = error_count
    run.last_error = last_error
    session.add(run)
    session.commit()


def upsert_listings(session: Session, source: Source, listings: list[ListingDraft]) -> tuple[int, int]:
    inserted = 0
    updated = 0

    for listing in listings:
        now = datetime.now(UTC)
        exists = session.execute(
            select(Listing.id).where(
                Listing.source_id == source.id,
                Listing.external_id == listing.external_id,
            )
        ).scalar_one_or_none()
        payload = {
            "source_id": source.id,
            "external_id": listing.external_id,
            "canonical_url": listing.canonical_url,
            "title": listing.title,
            "transaction_type": listing.transaction_type,
            "property_type": listing.property_type,
            "city": listing.city,
            "state": listing.state,
            "neighborhood": listing.neighborhood,
            "address": listing.address,
            "latitude": listing.latitude,
            "longitude": listing.longitude,
            "price_sale": listing.price_sale,
            "price_rent": listing.price_rent,
            "condo_fee": listing.condo_fee,
            "iptu": listing.iptu,
            "bedrooms": listing.bedrooms,
            "bathrooms": listing.bathrooms,
            "parking_spaces": listing.parking_spaces,
            "area_m2": listing.area_m2,
            "description": listing.description,
            "broker_name": listing.broker_name,
            "published_at": listing.published_at,
            "first_seen_at": now,
            "last_seen_at": now,
            "last_scraped_at": now,
            "is_active": True,
            "raw_payload": sanitize_json_value(listing.raw_payload),
        }
        statement = insert(Listing).values(**payload)
        update_columns = payload | {"first_seen_at": Listing.first_seen_at}
        result = session.execute(
            statement.on_conflict_do_update(
                constraint="uq_listings_source_external_id",
                set_=update_columns,
            )
        )
        if exists is None:
            inserted += 1
        else:
            updated += 1
    session.commit()
    return inserted, updated
