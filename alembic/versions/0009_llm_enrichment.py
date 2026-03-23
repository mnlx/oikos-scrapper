"""add mart_listing_llm_enriched table

Revision ID: 0009_llm_enrichment
Revises: 0008_price_enrichment
Create Date: 2026-03-21 13:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "0009_llm_enrichment"
down_revision = "0008_price_enrichment"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "mart_listing_llm_enriched",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("offering_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("source_code", sa.String(120), nullable=False),
        sa.Column("external_id", sa.String(255), nullable=False),
        sa.Column("llm_price_sale", sa.Numeric(14, 2), nullable=True),
        sa.Column("llm_price_rent", sa.Numeric(14, 2), nullable=True),
        sa.Column("llm_condo_fee", sa.Numeric(14, 2), nullable=True),
        sa.Column("llm_iptu", sa.Numeric(14, 2), nullable=True),
        sa.Column("llm_address", sa.Text(), nullable=True),
        sa.Column("llm_neighborhood", sa.String(255), nullable=True),
        sa.Column("llm_city", sa.String(120), nullable=True),
        sa.Column("llm_bedrooms", sa.Integer(), nullable=True),
        sa.Column("llm_bathrooms", sa.Integer(), nullable=True),
        sa.Column("llm_parking_spaces", sa.Integer(), nullable=True),
        sa.Column("llm_area_m2", sa.Numeric(10, 2), nullable=True),
        sa.Column("llm_property_type", sa.String(50), nullable=True),
        sa.Column("llm_transaction_type", sa.String(30), nullable=True),
        sa.Column("llm_model", sa.String(120), nullable=False),
        sa.Column("enriched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("llm_input", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
    )
    op.create_index("ix_mart_listing_llm_enriched_offering_hash", "mart_listing_llm_enriched", ["offering_hash"])
    op.create_index("ix_mart_listing_llm_enriched_source_code", "mart_listing_llm_enriched", ["source_code"])


def downgrade() -> None:
    op.drop_table("mart_listing_llm_enriched")
