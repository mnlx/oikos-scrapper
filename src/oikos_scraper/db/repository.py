from __future__ import annotations

from decimal import Decimal
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from oikos_scraper.config import SourceDefinition
from oikos_scraper.db.models import BronzeListing, Listing, ListingArtifact, ListingIngestion, ScrapeRun, Source
from oikos_scraper.object_store import offering_hash
from oikos_scraper.raw_html_store import build_raw_html_store
from oikos_scraper.types import ListingArtifactBundle, ListingDraft, ParsedListingRecord, StoredObject


RAW_HTML_STORE = None


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


def get_raw_html_store():
    global RAW_HTML_STORE  # noqa: PLW0603
    if RAW_HTML_STORE is None:
        RAW_HTML_STORE = build_raw_html_store()
    return RAW_HTML_STORE


def persist_raw_html_payload(listing: ListingDraft) -> dict[str, Any]:
    payload = dict(listing.raw_payload)
    raw_html = payload.pop("raw_html", None)
    if not raw_html:
        return payload

    store = get_raw_html_store()
    if store is None:
        payload["raw_html"] = raw_html
        return payload

    uploaded = store.upload_listing_html(listing, raw_html)
    payload["raw_html_object"] = {
        "bucket": uploaded.bucket,
        "key": uploaded.key,
        "endpoint": uploaded.endpoint,
        "secure": uploaded.secure,
        "size": uploaded.size,
    }
    return payload


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


def create_scrape_run(
    session: Session,
    source_code: str,
    trigger_type: str,
    strategy: str,
    pipeline_stage: str = "scrape",
) -> ScrapeRun:
    run = ScrapeRun(
        started_at=datetime.now(UTC),
        trigger_type=trigger_type,
        status="running",
        source_code=source_code,
        strategy=strategy,
        pipeline_stage=pipeline_stage,
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
            "raw_payload": sanitize_json_value(persist_raw_html_payload(listing)),
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


def upsert_listing_ingestion(
    session: Session,
    *,
    scrape_run: ScrapeRun,
    source: Source,
    listing: ListingDraft,
    seed_url: str,
    strategy: str,
    image_urls: list[str],
    ingestion_payload: dict[str, Any],
) -> ListingIngestion:
    now = datetime.now(UTC)
    statement = insert(ListingIngestion).values(
        scrape_run_id=scrape_run.id,
        source_id=source.id,
        source_code=listing.source_code,
        external_id=listing.external_id,
        offering_hash=offering_hash(listing.source_code, listing.external_id),
        canonical_url=listing.canonical_url,
        seed_url=seed_url,
        strategy=strategy,
        city=listing.city,
        broker_name=listing.broker_name,
        image_urls=image_urls,
        ingestion_payload=sanitize_json_value(ingestion_payload),
        discovered_at=now,
        last_seen_at=now,
    ).on_conflict_do_update(
        constraint="uq_listing_ingestions_source_external_id",
        set_={
            "scrape_run_id": scrape_run.id,
            "offering_hash": offering_hash(listing.source_code, listing.external_id),
            "canonical_url": listing.canonical_url,
            "seed_url": seed_url,
            "strategy": strategy,
            "city": listing.city,
            "broker_name": listing.broker_name,
            "image_urls": sanitize_json_value(image_urls),
            "ingestion_payload": sanitize_json_value(ingestion_payload),
            "last_seen_at": now,
        },
    ).returning(ListingIngestion)
    ingestion = session.execute(statement).scalar_one()
    session.commit()
    return ingestion


def _artifact_rows(bundle: ListingArtifactBundle) -> list[tuple[str, StoredObject | None, str | None]]:
    rows: list[tuple[str, StoredObject | None, str | None]] = [
        ("html", bundle.html, None),
        ("screenshot", bundle.screenshot, None),
        ("json", bundle.metadata, None),
    ]
    rows.extend(("image", item, None) for item in bundle.images)
    return [row for row in rows if row[1] is not None]


def replace_listing_artifacts(
    session: Session,
    *,
    ingestion: ListingIngestion,
    bundle: ListingArtifactBundle,
    image_source_urls: list[str] | None = None,
) -> None:
    session.query(ListingArtifact).filter(ListingArtifact.ingestion_id == ingestion.id).delete()
    created_at = datetime.now(UTC)
    image_urls = image_source_urls or []
    image_index = 0
    for artifact_type, stored, source_url in _artifact_rows(bundle):
        if stored is None:
            continue
        if artifact_type == "image":
            source_url = image_urls[image_index] if image_index < len(image_urls) else None
            image_index += 1
        session.add(
            ListingArtifact(
                ingestion_id=ingestion.id,
                artifact_type=artifact_type,
                bucket=stored.bucket,
                object_key=stored.key,
                object_uri=stored.uri,
                content_type=stored.content_type,
                checksum_sha256=stored.checksum_sha256,
                size_bytes=stored.size,
                source_url=source_url,
                created_at=created_at,
            )
        )
    session.commit()


def list_ingestions(session: Session, source_codes: list[str] | None = None) -> list[ListingIngestion]:
    statement = select(ListingIngestion)
    if source_codes:
        statement = statement.where(ListingIngestion.source_code.in_(source_codes))
    return session.execute(statement.order_by(ListingIngestion.last_seen_at.desc())).scalars().all()


def list_artifacts_for_ingestion(session: Session, ingestion_id: int) -> list[ListingArtifact]:
    return session.execute(
        select(ListingArtifact).where(ListingArtifact.ingestion_id == ingestion_id).order_by(ListingArtifact.id.asc())
    ).scalars().all()


def upsert_bronze_listing(
    session: Session,
    *,
    source: Source,
    ingestion: ListingIngestion,
    record: ParsedListingRecord,
) -> BronzeListing:
    parsed_at = record.parsed_at or datetime.now(UTC)
    statement = insert(BronzeListing).values(
        ingestion_id=ingestion.id,
        source_id=source.id,
        source_code=record.source_code,
        external_id=record.external_id,
        offering_hash=record.offering_hash,
        canonical_url=record.canonical_url,
        title=record.title,
        transaction_type=record.transaction_type,
        property_type=record.property_type,
        city=record.city,
        state=record.state,
        neighborhood=record.neighborhood,
        address=record.address,
        latitude=record.latitude,
        longitude=record.longitude,
        price_sale=record.price_sale,
        price_rent=record.price_rent,
        condo_fee=record.condo_fee,
        iptu=record.iptu,
        bedrooms=record.bedrooms,
        bathrooms=record.bathrooms,
        parking_spaces=record.parking_spaces,
        area_m2=record.area_m2,
        description=record.description,
        broker_name=record.broker_name,
        published_at=record.published_at,
        image_uris=sanitize_json_value(record.image_uris),
        screenshot_uri=record.screenshot_uri,
        html_uri=record.html_uri,
        metadata_uri=record.metadata_uri,
        raw_payload=sanitize_json_value(record.raw_payload),
        parsed_at=parsed_at,
    ).on_conflict_do_update(
        constraint="uq_bronze_listings_source_external_id",
        set_={
            "ingestion_id": ingestion.id,
            "offering_hash": record.offering_hash,
            "canonical_url": record.canonical_url,
            "title": record.title,
            "transaction_type": record.transaction_type,
            "property_type": record.property_type,
            "city": record.city,
            "state": record.state,
            "neighborhood": record.neighborhood,
            "address": record.address,
            "latitude": record.latitude,
            "longitude": record.longitude,
            "price_sale": record.price_sale,
            "price_rent": record.price_rent,
            "condo_fee": record.condo_fee,
            "iptu": record.iptu,
            "bedrooms": record.bedrooms,
            "bathrooms": record.bathrooms,
            "parking_spaces": record.parking_spaces,
            "area_m2": record.area_m2,
            "description": record.description,
            "broker_name": record.broker_name,
            "published_at": record.published_at,
            "image_uris": sanitize_json_value(record.image_uris),
            "screenshot_uri": record.screenshot_uri,
            "html_uri": record.html_uri,
            "metadata_uri": record.metadata_uri,
            "raw_payload": sanitize_json_value(record.raw_payload),
            "parsed_at": parsed_at,
        },
    ).returning(BronzeListing)
    bronze = session.execute(statement).scalar_one()
    session.commit()
    return bronze
