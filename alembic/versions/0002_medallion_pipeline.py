"""medallion pipeline schema

Revision ID: 0002_medallion_pipeline
Revises: 0001_initial
Create Date: 2026-03-21 00:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0002_medallion_pipeline"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scrape_runs",
        sa.Column("pipeline_stage", sa.String(length=50), nullable=False, server_default="scrape"),
    )

    op.create_table(
        "listing_ingestions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scrape_run_id", sa.Integer(), sa.ForeignKey("scrape_runs.id"), nullable=False),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("sources.id"), nullable=False),
        sa.Column("source_code", sa.String(length=120), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("offering_hash", sa.String(length=64), nullable=False),
        sa.Column("canonical_url", sa.String(length=1000), nullable=False),
        sa.Column("seed_url", sa.String(length=1000), nullable=False),
        sa.Column("strategy", sa.String(length=50), nullable=False),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("broker_name", sa.String(length=255), nullable=True),
        sa.Column("image_urls", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("ingestion_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("source_id", "external_id", name="uq_listing_ingestions_source_external_id"),
    )

    op.create_table(
        "listing_artifacts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("ingestion_id", sa.Integer(), sa.ForeignKey("listing_ingestions.id"), nullable=False),
        sa.Column("artifact_type", sa.String(length=30), nullable=False),
        sa.Column("bucket", sa.String(length=255), nullable=False),
        sa.Column("object_key", sa.String(length=1000), nullable=False),
        sa.Column("object_uri", sa.String(length=1200), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=False),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("source_url", sa.String(length=1000), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("ingestion_id", "artifact_type", "object_key", name="uq_listing_artifacts_ingestion_object"),
    )

    op.create_table(
        "bronze_listings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("ingestion_id", sa.Integer(), sa.ForeignKey("listing_ingestions.id"), nullable=False, unique=True),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("sources.id"), nullable=False),
        sa.Column("source_code", sa.String(length=120), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("offering_hash", sa.String(length=64), nullable=False),
        sa.Column("canonical_url", sa.String(length=1000), nullable=False),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("transaction_type", sa.String(length=30), nullable=False),
        sa.Column("property_type", sa.String(length=30), nullable=False),
        sa.Column("city", sa.String(length=120), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("neighborhood", sa.String(length=255), nullable=True),
        sa.Column("address", sa.String(length=500), nullable=True),
        sa.Column("latitude", sa.Numeric(10, 7), nullable=True),
        sa.Column("longitude", sa.Numeric(10, 7), nullable=True),
        sa.Column("price_sale", sa.Numeric(14, 2), nullable=True),
        sa.Column("price_rent", sa.Numeric(14, 2), nullable=True),
        sa.Column("condo_fee", sa.Numeric(14, 2), nullable=True),
        sa.Column("iptu", sa.Numeric(14, 2), nullable=True),
        sa.Column("bedrooms", sa.Integer(), nullable=True),
        sa.Column("bathrooms", sa.Integer(), nullable=True),
        sa.Column("parking_spaces", sa.Integer(), nullable=True),
        sa.Column("area_m2", sa.Numeric(10, 2), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("broker_name", sa.String(length=255), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("image_uris", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("screenshot_uri", sa.String(length=1200), nullable=True),
        sa.Column("html_uri", sa.String(length=1200), nullable=True),
        sa.Column("metadata_uri", sa.String(length=1200), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("parsed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("source_id", "external_id", name="uq_bronze_listings_source_external_id"),
    )


def downgrade() -> None:
    op.drop_table("bronze_listings")
    op.drop_table("listing_artifacts")
    op.drop_table("listing_ingestions")
    op.drop_column("scrape_runs", "pipeline_stage")
