"""add downloaded assets registry

Revision ID: 0013_downloaded_assets_registry
Revises: 0012_sources_block_flag
Create Date: 2026-03-29 00:30:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0013_downloaded_assets_registry"
down_revision = "0012_sources_block_flag"
branch_labels = None
depends_on = None


def upgrade() -> None:
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


def downgrade() -> None:
    op.drop_table("downloaded_assets")
