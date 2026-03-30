"""recreate int_listings_deduped table if dropped by dbt

Revision ID: 0016_recreate_int_listings_deduped
Revises: 0015_drop_asset_tables
Create Date: 2026-03-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision = "0016_int_listings_deduped"
down_revision = "0015_drop_asset_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # dbt may have dropped this physical table when creating a view with the same name.
    # Recreate it if missing.
    op.execute("""
        CREATE TABLE IF NOT EXISTS int_listings_deduped (
            id SERIAL PRIMARY KEY,
            ingestion_id INTEGER NOT NULL UNIQUE REFERENCES raw_listing_ingestions(id),
            source_id INTEGER NOT NULL REFERENCES sources(id),
            source_code VARCHAR(120) NOT NULL,
            external_id VARCHAR(255) NOT NULL,
            offering_hash VARCHAR(64) NOT NULL,
            canonical_url VARCHAR(1000) NOT NULL,
            title VARCHAR(500) NOT NULL,
            transaction_type VARCHAR(30) NOT NULL,
            property_type VARCHAR(30) NOT NULL,
            city VARCHAR(120) NOT NULL,
            state VARCHAR(2) NOT NULL,
            neighborhood VARCHAR(255),
            address VARCHAR(500),
            latitude NUMERIC(10,7),
            longitude NUMERIC(10,7),
            price_sale NUMERIC(14,2),
            price_rent NUMERIC(14,2),
            condo_fee NUMERIC(14,2),
            iptu NUMERIC(14,2),
            bedrooms INTEGER,
            bathrooms INTEGER,
            parking_spaces INTEGER,
            area_m2 NUMERIC(10,2),
            description TEXT,
            broker_name VARCHAR(255),
            published_at TIMESTAMPTZ,
            listing_created_at TIMESTAMPTZ,
            listing_updated_at TIMESTAMPTZ,
            image_uris JSONB NOT NULL DEFAULT '[]'::jsonb,
            asset_links JSONB NOT NULL DEFAULT '[]'::jsonb,
            screenshot_uri VARCHAR(1200),
            html_uri VARCHAR(1200),
            metadata_uri VARCHAR(1200),
            text_html TEXT,
            geocode_provider VARCHAR(50),
            geocode_query VARCHAR(1000),
            geocode_confidence NUMERIC(10,6),
            geocode_status VARCHAR(30),
            geocode_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            geocoded_at TIMESTAMPTZ,
            parsed_at TIMESTAMPTZ NOT NULL,
            price_enriched_at TIMESTAMPTZ,
            price_enrichment_source VARCHAR(30),
            CONSTRAINT uq_int_listings_deduped_source_external_id UNIQUE (source_id, external_id)
        )
    """)


def downgrade() -> None:
    pass
