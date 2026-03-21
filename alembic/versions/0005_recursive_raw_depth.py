"""rename raw layer tables and add recursive crawl depth

Revision ID: 0005_recursive_raw_depth
Revises: 0004_neighborhood_files
Create Date: 2026-03-21 15:45:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0005_recursive_raw_depth"
down_revision = "0004_neighborhood_files"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("listing_ingestions", "raw_listing_ingestions")
    op.rename_table("listing_artifacts", "raw_listing_artifacts")
    op.rename_table("bronze_listings", "raw_listings")

    op.execute(
        "ALTER TABLE raw_listing_ingestions RENAME CONSTRAINT "
        "uq_listing_ingestions_source_external_id TO uq_raw_listing_ingestions_source_external_page_old"
    )
    op.execute(
        "ALTER TABLE raw_listing_artifacts RENAME CONSTRAINT "
        "uq_listing_artifacts_ingestion_object TO uq_raw_listing_artifacts_ingestion_object"
    )
    op.execute(
        "ALTER TABLE raw_listings RENAME CONSTRAINT "
        "uq_bronze_listings_source_external_id TO uq_raw_listings_source_external_id"
    )

    op.add_column("raw_listing_ingestions", sa.Column("page_url", sa.String(length=1000), nullable=True))
    op.add_column("raw_listing_ingestions", sa.Column("parent_page_url", sa.String(length=1000), nullable=True))
    op.add_column(
        "raw_listing_ingestions",
        sa.Column("depth", sa.Integer(), nullable=False, server_default="0"),
    )

    op.execute("UPDATE raw_listing_ingestions SET page_url = canonical_url WHERE page_url IS NULL")
    op.alter_column("raw_listing_ingestions", "page_url", nullable=False)

    op.drop_constraint(
        "uq_raw_listing_ingestions_source_external_page_old",
        "raw_listing_ingestions",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_raw_listing_ingestions_source_external_page",
        "raw_listing_ingestions",
        ["source_id", "external_id", "page_url"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_raw_listing_ingestions_source_external_page",
        "raw_listing_ingestions",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_listing_ingestions_source_external_id",
        "raw_listing_ingestions",
        ["source_id", "external_id"],
    )

    op.drop_column("raw_listing_ingestions", "depth")
    op.drop_column("raw_listing_ingestions", "parent_page_url")
    op.drop_column("raw_listing_ingestions", "page_url")

    op.execute(
        "ALTER TABLE raw_listing_artifacts RENAME CONSTRAINT "
        "uq_raw_listing_artifacts_ingestion_object TO uq_listing_artifacts_ingestion_object"
    )
    op.execute(
        "ALTER TABLE raw_listings RENAME CONSTRAINT "
        "uq_raw_listings_source_external_id TO uq_bronze_listings_source_external_id"
    )

    op.rename_table("raw_listings", "bronze_listings")
    op.rename_table("raw_listing_artifacts", "listing_artifacts")
    op.rename_table("raw_listing_ingestions", "listing_ingestions")
