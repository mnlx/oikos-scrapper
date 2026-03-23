"""add llm_latitude and llm_longitude to mart_listing_llm_enriched

Revision ID: 0011_llm_lat_long
Revises: 0010_geocode_enrichment
Create Date: 2026-03-23 04:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0011_llm_lat_long"
down_revision = "0010_geocode_enrichment"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("mart_listing_llm_enriched", sa.Column("llm_latitude", sa.Numeric(10, 7), nullable=True))
    op.add_column("mart_listing_llm_enriched", sa.Column("llm_longitude", sa.Numeric(10, 7), nullable=True))


def downgrade() -> None:
    op.drop_column("mart_listing_llm_enriched", "llm_latitude")
    op.drop_column("mart_listing_llm_enriched", "llm_longitude")
