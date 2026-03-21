select
  id as artifact_id,
  ingestion_id,
  artifact_type,
  bucket,
  object_key,
  object_uri,
  content_type,
  checksum_sha256,
  size_bytes,
  source_url,
  created_at
from {{ source('app', 'listing_artifacts') }}
