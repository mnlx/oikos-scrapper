"""add block flag to sources

Revision ID: 0012_sources_block_flag
Revises: 0011_llm_lat_long
Create Date: 2026-03-29 00:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0012_sources_block_flag"
down_revision = "0011_llm_lat_long"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("sources", sa.Column("block", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.execute(sa.text("UPDATE sources SET block = true WHERE code IN ('olx', 'quintoandar')"))
    op.alter_column("sources", "block", server_default=None)


def downgrade() -> None:
    op.drop_column("sources", "block")
