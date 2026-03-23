"""add geocode enrichment columns to raw_listings

Revision ID: 0010_geocode_enrichment
Revises: 0009_llm_enrichment
Create Date: 2026-03-23 18:30:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "0010_geocode_enrichment"
down_revision = "0009_llm_enrichment"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("raw_listings", sa.Column("geocode_provider", sa.String(length=50), nullable=True))
    op.add_column("raw_listings", sa.Column("geocode_query", sa.String(length=1000), nullable=True))
    op.add_column("raw_listings", sa.Column("geocode_confidence", sa.Numeric(10, 6), nullable=True))
    op.add_column("raw_listings", sa.Column("geocode_status", sa.String(length=30), nullable=True))
    op.add_column(
        "raw_listings",
        sa.Column(
            "geocode_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column("raw_listings", sa.Column("geocoded_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_raw_listings_geocoded_at", "raw_listings", ["geocoded_at"])
    op.create_index("ix_raw_listings_geocode_status", "raw_listings", ["geocode_status"])


def downgrade() -> None:
    op.drop_index("ix_raw_listings_geocode_status", table_name="raw_listings")
    op.drop_index("ix_raw_listings_geocoded_at", table_name="raw_listings")
    op.drop_column("raw_listings", "geocoded_at")
    op.drop_column("raw_listings", "geocode_payload")
    op.drop_column("raw_listings", "geocode_status")
    op.drop_column("raw_listings", "geocode_confidence")
    op.drop_column("raw_listings", "geocode_query")
    op.drop_column("raw_listings", "geocode_provider")
