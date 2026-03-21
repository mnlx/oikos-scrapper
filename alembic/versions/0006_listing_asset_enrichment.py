"""add listing asset enrichment phase support

Revision ID: 0006_listing_asset_enrichment
Revises: 0005_recursive_raw_depth
Create Date: 2026-03-21 19:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0006_listing_asset_enrichment"
down_revision = "0005_recursive_raw_depth"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "raw_listing_ingestions",
        sa.Column("asset_links", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
    )
    op.add_column(
        "raw_listing_ingestions",
        sa.Column("screenshot_uri", sa.String(length=1200), nullable=True),
    )
    op.add_column(
        "raw_listings",
        sa.Column("asset_links", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
    )

    op.execute("UPDATE raw_listing_ingestions SET asset_links = COALESCE(image_urls, '[]'::jsonb)")
    op.execute(
        """
        UPDATE raw_listing_ingestions AS i
        SET screenshot_uri = a.object_uri
        FROM raw_listing_artifacts AS a
        WHERE a.ingestion_id = i.id
          AND a.artifact_type = 'screenshot'
        """
    )
    op.execute(
        """
        UPDATE raw_listings AS r
        SET asset_links = COALESCE(i.asset_links, '[]'::jsonb)
        FROM raw_listing_ingestions AS i
        WHERE i.id = r.ingestion_id
        """
    )

    op.create_table(
        "raw_listing_assets",
        sa.Column("id", sa.String(length=255), primary_key=True),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("sources.id"), nullable=False),
        sa.Column("ingestion_id", sa.Integer(), sa.ForeignKey("raw_listing_ingestions.id"), nullable=False),
        sa.Column("source_code", sa.String(length=120), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("asset_type", sa.String(length=50), nullable=False),
        sa.Column("asset_url", sa.String(length=1200), nullable=False),
        sa.Column("asset_uri", sa.String(length=1200), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=True),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("is_scrapped", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("scrapped_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("source_id", "external_id", "asset_url", name="uq_raw_listing_assets_source_external_url"),
    )
    op.create_index("ix_raw_listing_assets_source_external", "raw_listing_assets", ["source_code", "external_id"])
    op.create_index("ix_raw_listing_assets_ingestion", "raw_listing_assets", ["ingestion_id"])


def downgrade() -> None:
    op.drop_index("ix_raw_listing_assets_ingestion", table_name="raw_listing_assets")
    op.drop_index("ix_raw_listing_assets_source_external", table_name="raw_listing_assets")
    op.drop_table("raw_listing_assets")
    op.drop_column("raw_listings", "asset_links")
    op.drop_column("raw_listing_ingestions", "screenshot_uri")
    op.drop_column("raw_listing_ingestions", "asset_links")
