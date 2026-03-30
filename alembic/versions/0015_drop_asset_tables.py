"""drop raw_listing_artifacts, raw_listing_assets, downloaded_assets

Revision ID: 0015_drop_asset_tables
Revises: 0014_int_listings_deduped
Create Date: 2026-03-29
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision = "0015_drop_asset_tables"
down_revision = "0014_int_listings_deduped"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP VIEW IF EXISTS int_listing_media CASCADE")
    op.execute("DROP TABLE IF EXISTS mart_listing_media CASCADE")
    op.drop_table("raw_listing_artifacts")
    op.drop_table("raw_listing_assets")
    op.drop_table("downloaded_assets")


def downgrade() -> None:
    op.create_table(
        "downloaded_assets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("asset_url", sa.String(length=1200), nullable=False),
        sa.Column("asset_url_normalized", sa.String(length=1200), nullable=False),
        sa.Column("asset_uri", sa.String(length=1200), nullable=True),
        sa.Column("content_type", sa.String(length=255), nullable=True),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("download_status", sa.String(length=30), nullable=False),
        sa.Column("first_downloaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_attempted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_reused_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.UniqueConstraint("asset_url_normalized", name="uq_downloaded_assets_asset_url_normalized"),
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
        sa.Column("is_scrapped", sa.Boolean(), nullable=False, default=False),
        sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("scrapped_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("source_id", "external_id", "asset_url", name="uq_raw_listing_assets_source_external_url"),
    )
    op.create_table(
        "raw_listing_artifacts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("ingestion_id", sa.Integer(), sa.ForeignKey("raw_listing_ingestions.id"), nullable=False),
        sa.Column("artifact_type", sa.String(length=30), nullable=False),
        sa.Column("bucket", sa.String(length=255), nullable=False),
        sa.Column("object_key", sa.String(length=1000), nullable=False),
        sa.Column("object_uri", sa.String(length=1200), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=False),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("source_url", sa.String(length=1000), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("ingestion_id", "artifact_type", "object_key", name="uq_raw_listing_artifacts_ingestion_object"),
    )
