"""add neighborhood signals table

Revision ID: 0003_neighborhood_signals
Revises: 0002_medallion_pipeline
Create Date: 2026-03-21 16:20:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0003_neighborhood_signals"
down_revision = "0002_medallion_pipeline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "neighborhood_signals",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("city", sa.String(length=120), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False, server_default="SC"),
        sa.Column("neighborhood", sa.String(length=255), nullable=True),
        sa.Column("geographic_scope", sa.String(length=30), nullable=False, server_default="city"),
        sa.Column("signal_category", sa.String(length=50), nullable=False),
        sa.Column("signal_code", sa.String(length=120), nullable=False),
        sa.Column("signal_name", sa.String(length=255), nullable=False),
        sa.Column("source_name", sa.String(length=255), nullable=False),
        sa.Column("source_type", sa.String(length=30), nullable=False, server_default="report"),
        sa.Column("publisher", sa.String(length=255), nullable=True),
        sa.Column("source_url", sa.String(length=1200), nullable=False),
        sa.Column("reference_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("period_start", sa.Date(), nullable=True),
        sa.Column("period_end", sa.Date(), nullable=True),
        sa.Column("value_numeric", sa.Numeric(18, 4), nullable=True),
        sa.Column("value_text", sa.Text(), nullable=True),
        sa.Column("unit", sa.String(length=50), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "metadata_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_neighborhood_signals_city_neighborhood",
        "neighborhood_signals",
        ["city", "neighborhood"],
        unique=False,
    )
    op.create_index(
        "ix_neighborhood_signals_category_reference_date",
        "neighborhood_signals",
        ["signal_category", "reference_date"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_neighborhood_signals_category_reference_date", table_name="neighborhood_signals")
    op.drop_index("ix_neighborhood_signals_city_neighborhood", table_name="neighborhood_signals")
    op.drop_table("neighborhood_signals")
