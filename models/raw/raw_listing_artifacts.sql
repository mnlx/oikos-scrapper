select
  a.id as artifact_id,
  a.ingestion_id,
  i.page_url,
  i.depth,
  a.artifact_type,
  a.bucket,
  a.object_key,
  a.object_uri,
  a.content_type,
  a.checksum_sha256,
  a.size_bytes,
  a.source_url,
  a.created_at
from {{ source('app', 'raw_listing_artifacts') }} as a
left join {{ source('app', 'raw_listing_ingestions') }} as i
  on a.ingestion_id = i.id
