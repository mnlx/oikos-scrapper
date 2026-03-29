"""add listing dates and neighborhood asset enrichment tables

Revision ID: 0007_listing_dates
Revises: 0006_listing_asset_enrichment
Create Date: 2026-03-21 22:45:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0007_listing_dates"
down_revision = "0006_listing_asset_enrichment"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table_name in ("listings", "raw_listing_ingestions", "raw_listings"):
        op.add_column(table_name, sa.Column("listing_created_at", sa.DateTime(timezone=True), nullable=True))
        op.add_column(table_name, sa.Column("listing_updated_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("raw_listing_ingestions", sa.Column("published_at", sa.DateTime(timezone=True), nullable=True))

    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema()
                  AND c.relname = 'raw_neighborhood_files'
                  AND c.relkind IN ('v', 'm')
            ) THEN
                EXECUTE 'DROP VIEW raw_neighborhood_files CASCADE';
            END IF;
        END $$;
        """
    )
    op.rename_table("neighborhood_files", "raw_neighborhood_files")
    op.execute(
        "ALTER TABLE raw_neighborhood_files RENAME CONSTRAINT uq_neighborhood_files_source_url TO uq_raw_neighborhood_files_source_url"
    )
    op.execute(
        "ALTER INDEX ix_neighborhood_files_city_neighborhood RENAME TO ix_raw_neighborhood_files_city_neighborhood"
    )

    op.create_table(
        "raw_neighborhood_artifacts",
        sa.Column("id", sa.String(length=255), primary_key=True),
        sa.Column("neighborhood_file_id", sa.Integer(), sa.ForeignKey("raw_neighborhood_files.id"), nullable=False),
        sa.Column("source_code", sa.String(length=120), nullable=False),
        sa.Column("source_url", sa.String(length=1200), nullable=False),
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
        sa.UniqueConstraint("neighborhood_file_id", "asset_url", name="uq_raw_neighborhood_artifacts_file_url"),
    )
    op.create_index("ix_raw_neighborhood_artifacts_source_code", "raw_neighborhood_artifacts", ["source_code"])
    op.create_index("ix_raw_neighborhood_artifacts_file_id", "raw_neighborhood_artifacts", ["neighborhood_file_id"])


def downgrade() -> None:
    op.drop_index("ix_raw_neighborhood_artifacts_file_id", table_name="raw_neighborhood_artifacts")
    op.drop_index("ix_raw_neighborhood_artifacts_source_code", table_name="raw_neighborhood_artifacts")
    op.drop_table("raw_neighborhood_artifacts")

    op.execute(
        "ALTER INDEX ix_raw_neighborhood_files_city_neighborhood RENAME TO ix_neighborhood_files_city_neighborhood"
    )
    op.execute(
        "ALTER TABLE raw_neighborhood_files RENAME CONSTRAINT uq_raw_neighborhood_files_source_url TO uq_neighborhood_files_source_url"
    )
    op.rename_table("raw_neighborhood_files", "neighborhood_files")

    op.drop_column("raw_listing_ingestions", "published_at")
    for table_name in ("raw_listings", "raw_listing_ingestions", "listings"):
        op.drop_column(table_name, "listing_updated_at")
        op.drop_column(table_name, "listing_created_at")
