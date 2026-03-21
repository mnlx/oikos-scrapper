from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(120), unique=True)
    name: Mapped[str] = mapped_column(String(255))
    base_url: Mapped[str] = mapped_column(String(500))
    active: Mapped[bool] = mapped_column(Boolean, default=True)


class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    trigger_type: Mapped[str] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(String(50))
    source_code: Mapped[str] = mapped_column(String(120))
    strategy: Mapped[str] = mapped_column(String(50))
    items_seen: Mapped[int] = mapped_column(Integer, default=0)
    items_inserted: Mapped[int] = mapped_column(Integer, default=0)
    items_updated: Mapped[int] = mapped_column(Integer, default=0)
    error_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    pipeline_stage: Mapped[str] = mapped_column(String(50), default="scrape")


class Listing(Base):
    __tablename__ = "listings"
    __table_args__ = (
        UniqueConstraint("source_id", "external_id", name="uq_listings_source_external_id"),
        UniqueConstraint("source_id", "canonical_url", name="uq_listings_source_canonical_url"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"))
    external_id: Mapped[str] = mapped_column(String(255))
    canonical_url: Mapped[str] = mapped_column(String(1000))
    title: Mapped[str] = mapped_column(String(500))
    transaction_type: Mapped[str] = mapped_column(String(30))
    property_type: Mapped[str] = mapped_column(String(30))
    city: Mapped[str] = mapped_column(String(120))
    state: Mapped[str] = mapped_column(String(2))
    neighborhood: Mapped[str | None] = mapped_column(String(255), nullable=True)
    address: Mapped[str | None] = mapped_column(String(500), nullable=True)
    latitude: Mapped[Decimal | None] = mapped_column(Numeric(10, 7), nullable=True)
    longitude: Mapped[Decimal | None] = mapped_column(Numeric(10, 7), nullable=True)
    price_sale: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    price_rent: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    condo_fee: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    iptu: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    bedrooms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bathrooms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parking_spaces: Mapped[int | None] = mapped_column(Integer, nullable=True)
    area_m2: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    broker_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    raw_payload: Mapped[dict] = mapped_column(JSONB)


class ListingIngestion(Base):
    __tablename__ = "raw_listing_ingestions"
    __table_args__ = (
        UniqueConstraint("source_id", "external_id", "page_url", name="uq_raw_listing_ingestions_source_external_page"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scrape_run_id: Mapped[int] = mapped_column(ForeignKey("scrape_runs.id"))
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"))
    source_code: Mapped[str] = mapped_column(String(120))
    external_id: Mapped[str] = mapped_column(String(255))
    offering_hash: Mapped[str] = mapped_column(String(64))
    canonical_url: Mapped[str] = mapped_column(String(1000))
    page_url: Mapped[str] = mapped_column(String(1000))
    seed_url: Mapped[str] = mapped_column(String(1000))
    parent_page_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    depth: Mapped[int] = mapped_column(Integer, default=0)
    strategy: Mapped[str] = mapped_column(String(50))
    city: Mapped[str | None] = mapped_column(String(120), nullable=True)
    broker_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    image_urls: Mapped[list] = mapped_column(JSONB, default=list)
    asset_links: Mapped[list] = mapped_column(JSONB, default=list)
    screenshot_uri: Mapped[str | None] = mapped_column(String(1200), nullable=True)
    ingestion_payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ListingArtifact(Base):
    __tablename__ = "raw_listing_artifacts"
    __table_args__ = (
        UniqueConstraint("ingestion_id", "artifact_type", "object_key", name="uq_raw_listing_artifacts_ingestion_object"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ingestion_id: Mapped[int] = mapped_column(ForeignKey("raw_listing_ingestions.id"))
    artifact_type: Mapped[str] = mapped_column(String(30))
    bucket: Mapped[str] = mapped_column(String(255))
    object_key: Mapped[str] = mapped_column(String(1000))
    object_uri: Mapped[str] = mapped_column(String(1200))
    content_type: Mapped[str] = mapped_column(String(255))
    checksum_sha256: Mapped[str] = mapped_column(String(64))
    size_bytes: Mapped[int] = mapped_column(Integer)
    source_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class BronzeListing(Base):
    __tablename__ = "raw_listings"
    __table_args__ = (
        UniqueConstraint("source_id", "external_id", name="uq_raw_listings_source_external_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ingestion_id: Mapped[int] = mapped_column(ForeignKey("raw_listing_ingestions.id"), unique=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"))
    source_code: Mapped[str] = mapped_column(String(120))
    external_id: Mapped[str] = mapped_column(String(255))
    offering_hash: Mapped[str] = mapped_column(String(64))
    canonical_url: Mapped[str] = mapped_column(String(1000))
    title: Mapped[str] = mapped_column(String(500))
    transaction_type: Mapped[str] = mapped_column(String(30))
    property_type: Mapped[str] = mapped_column(String(30))
    city: Mapped[str] = mapped_column(String(120))
    state: Mapped[str] = mapped_column(String(2))
    neighborhood: Mapped[str | None] = mapped_column(String(255), nullable=True)
    address: Mapped[str | None] = mapped_column(String(500), nullable=True)
    latitude: Mapped[Decimal | None] = mapped_column(Numeric(10, 7), nullable=True)
    longitude: Mapped[Decimal | None] = mapped_column(Numeric(10, 7), nullable=True)
    price_sale: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    price_rent: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    condo_fee: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    iptu: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    bedrooms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bathrooms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parking_spaces: Mapped[int | None] = mapped_column(Integer, nullable=True)
    area_m2: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    broker_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    image_uris: Mapped[list] = mapped_column(JSONB, default=list)
    asset_links: Mapped[list] = mapped_column(JSONB, default=list)
    screenshot_uri: Mapped[str | None] = mapped_column(String(1200), nullable=True)
    html_uri: Mapped[str | None] = mapped_column(String(1200), nullable=True)
    metadata_uri: Mapped[str | None] = mapped_column(String(1200), nullable=True)
    raw_payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    parsed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ListingAsset(Base):
    __tablename__ = "raw_listing_assets"
    __table_args__ = (
        UniqueConstraint("source_id", "external_id", "asset_url", name="uq_raw_listing_assets_source_external_url"),
        Index("ix_raw_listing_assets_source_external", "source_code", "external_id"),
        Index("ix_raw_listing_assets_ingestion", "ingestion_id"),
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"))
    ingestion_id: Mapped[int] = mapped_column(ForeignKey("raw_listing_ingestions.id"))
    source_code: Mapped[str] = mapped_column(String(120))
    external_id: Mapped[str] = mapped_column(String(255))
    asset_id: Mapped[int] = mapped_column(Integer)
    asset_type: Mapped[str] = mapped_column(String(50))
    asset_url: Mapped[str] = mapped_column(String(1200))
    asset_uri: Mapped[str] = mapped_column(String(1200))
    content_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    checksum_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_scrapped: Mapped[bool] = mapped_column(Boolean, default=False)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    scrapped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class NeighborhoodSignal(Base):
    __tablename__ = "neighborhood_signals"
    __table_args__ = (
        Index("ix_neighborhood_signals_city_neighborhood", "city", "neighborhood"),
        Index("ix_neighborhood_signals_category_reference_date", "signal_category", "reference_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    city: Mapped[str] = mapped_column(String(120), nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False, default="SC")
    neighborhood: Mapped[str | None] = mapped_column(String(255), nullable=True)
    geographic_scope: Mapped[str] = mapped_column(String(30), nullable=False, default="city")
    signal_category: Mapped[str] = mapped_column(String(50), nullable=False)
    signal_code: Mapped[str] = mapped_column(String(120), nullable=False)
    signal_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_type: Mapped[str] = mapped_column(String(30), nullable=False, default="report")
    publisher: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_url: Mapped[str] = mapped_column(String(1200), nullable=False)
    reference_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    value_numeric: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    value_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    unit: Mapped[str | None] = mapped_column(String(50), nullable=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    metadata_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class NeighborhoodFile(Base):
    __tablename__ = "neighborhood_files"
    __table_args__ = (
        UniqueConstraint("source_code", "source_url", name="uq_neighborhood_files_source_url"),
        Index("ix_neighborhood_files_city_neighborhood", "city", "neighborhood"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_code: Mapped[str] = mapped_column(String(120), nullable=False)
    source_name: Mapped[str] = mapped_column(String(255), nullable=False)
    base_url: Mapped[str] = mapped_column(String(500), nullable=False)
    source_url: Mapped[str] = mapped_column(String(1200), nullable=False)
    city: Mapped[str | None] = mapped_column(String(120), nullable=True)
    state: Mapped[str] = mapped_column(String(2), nullable=False, default="SC")
    neighborhood: Mapped[str | None] = mapped_column(String(255), nullable=True)
    signal_category: Mapped[str | None] = mapped_column(String(50), nullable=True)
    geographic_scope: Mapped[str] = mapped_column(String(30), nullable=False, default="city")
    source_type: Mapped[str] = mapped_column(String(30), nullable=False, default="report")
    publisher: Mapped[str | None] = mapped_column(String(255), nullable=True)
    parser: Mapped[str | None] = mapped_column(String(50), nullable=True)
    content_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    html_uri: Mapped[str | None] = mapped_column(String(1200), nullable=True)
    json_uri: Mapped[str | None] = mapped_column(String(1200), nullable=True)
    screenshot_uri: Mapped[str | None] = mapped_column(String(1200), nullable=True)
    file_uri: Mapped[str | None] = mapped_column(String(1200), nullable=True)
    metadata_uri: Mapped[str | None] = mapped_column(String(1200), nullable=True)
    checksum_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parse_status: Mapped[str] = mapped_column(String(30), nullable=False, default="pending")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    reference_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    parsed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
