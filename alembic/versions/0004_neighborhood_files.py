"""add neighborhood files table

Revision ID: 0004_neighborhood_files
Revises: 0003_neighborhood_signals
Create Date: 2026-03-21 17:05:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0004_neighborhood_files"
down_revision = "0003_neighborhood_signals"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "neighborhood_files",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_code", sa.String(length=120), nullable=False),
        sa.Column("source_name", sa.String(length=255), nullable=False),
        sa.Column("base_url", sa.String(length=500), nullable=False),
        sa.Column("source_url", sa.String(length=1200), nullable=False),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("state", sa.String(length=2), nullable=False, server_default="SC"),
        sa.Column("neighborhood", sa.String(length=255), nullable=True),
        sa.Column("signal_category", sa.String(length=50), nullable=True),
        sa.Column("geographic_scope", sa.String(length=30), nullable=False, server_default="city"),
        sa.Column("source_type", sa.String(length=30), nullable=False, server_default="report"),
        sa.Column("publisher", sa.String(length=255), nullable=True),
        sa.Column("parser", sa.String(length=50), nullable=True),
        sa.Column("content_type", sa.String(length=255), nullable=True),
        sa.Column("html_uri", sa.String(length=1200), nullable=True),
        sa.Column("json_uri", sa.String(length=1200), nullable=True),
        sa.Column("screenshot_uri", sa.String(length=1200), nullable=True),
        sa.Column("file_uri", sa.String(length=1200), nullable=True),
        sa.Column("metadata_uri", sa.String(length=1200), nullable=True),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("parse_status", sa.String(length=30), nullable=False, server_default="pending"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("reference_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "metadata_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("parsed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("source_code", "source_url", name="uq_neighborhood_files_source_url"),
    )
    op.create_index(
        "ix_neighborhood_files_city_neighborhood",
        "neighborhood_files",
        ["city", "neighborhood"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_neighborhood_files_city_neighborhood", table_name="neighborhood_files")
    op.drop_table("neighborhood_files")
