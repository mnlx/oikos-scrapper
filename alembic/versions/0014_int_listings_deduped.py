"""rename raw_listings to int_listings_deduped, drop raw_payload, add text_html

Revision ID: 0014_int_listings_deduped
Revises: 0013_downloaded_assets_registry
Create Date: 2026-03-29 12:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0014_int_listings_deduped"
down_revision = "0013_downloaded_assets_registry"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the dbt-managed view if it exists (will be replaced by the physical table)
    op.execute("DROP VIEW IF EXISTS int_listings_deduped CASCADE")

    # Rename the table
    op.rename_table("raw_listings", "int_listings_deduped")

    # Rename unique constraint to match the new table name
    op.execute(
        "ALTER TABLE int_listings_deduped "
        "RENAME CONSTRAINT uq_raw_listings_source_external_id "
        "TO uq_int_listings_deduped_source_external_id"
    )

    # Drop the raw_payload column (large JSONB, redundant with raw_listing_ingestions)
    op.drop_column("int_listings_deduped", "raw_payload")

    # Add text_html: plain text extracted from HTML, used for LLM and geocode enrichment
    op.add_column(
        "int_listings_deduped",
        sa.Column("text_html", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("int_listings_deduped", "text_html")

    op.add_column(
        "int_listings_deduped",
        sa.Column("raw_payload", sa.dialects.postgresql.JSONB(), nullable=False, server_default="{}"),
    )

    op.execute(
        "ALTER TABLE int_listings_deduped "
        "RENAME CONSTRAINT uq_int_listings_deduped_source_external_id "
        "TO uq_raw_listings_source_external_id"
    )

    op.rename_table("int_listings_deduped", "raw_listings")
