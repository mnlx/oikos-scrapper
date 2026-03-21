select
  id as raw_listing_asset_id,
  source_id,
  ingestion_id,
  source_code,
  external_id,
  asset_id,
  asset_type,
  asset_url,
  asset_uri,
  content_type,
  checksum_sha256,
  size_bytes,
  is_scrapped,
  discovered_at,
  scrapped_at
from {{ source('app', 'raw_listing_assets') }}
