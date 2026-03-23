"""add price enrichment tracking to raw_listings

Revision ID: 0008_price_enrichment
Revises: 0007_listing_dates
Create Date: 2026-03-21 12:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0008_price_enrichment"
down_revision = "0007_listing_dates"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("raw_listings", sa.Column("price_enriched_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("raw_listings", sa.Column("price_enrichment_source", sa.String(30), nullable=True))


def downgrade() -> None:
    op.drop_column("raw_listings", "price_enrichment_source")
    op.drop_column("raw_listings", "price_enriched_at")
