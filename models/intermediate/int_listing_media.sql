with base as (
  select
    ingestion_id,
    artifact_type,
    object_uri,
    content_type,
    source_url,
    row_number() over (
      partition by ingestion_id, artifact_type
      order by created_at asc, artifact_id asc
    ) as media_rank
  from {{ ref('stg_listing_artifacts') }}
),
images as (
  select
    ingestion_id,
    jsonb_agg(object_uri order by media_rank) as image_uris
  from base
  where artifact_type = 'image'
  group by 1
),
screenshots as (
  select
    ingestion_id,
    max(object_uri) as screenshot_uri
  from base
  where artifact_type = 'screenshot'
  group by 1
),
html as (
  select
    ingestion_id,
    max(object_uri) as html_uri
  from base
  where artifact_type = 'html'
  group by 1
),
metadata as (
  select
    ingestion_id,
    max(object_uri) as metadata_uri
  from base
  where artifact_type = 'json'
  group by 1
)
select
  b.ingestion_id,
  coalesce(i.image_uris, '[]'::jsonb) as image_uris,
  s.screenshot_uri,
  h.html_uri,
  m.metadata_uri
from {{ ref('stg_bronze_listings') }} as b
left join images as i using (ingestion_id)
left join screenshots as s using (ingestion_id)
left join html as h using (ingestion_id)
left join metadata as m using (ingestion_id)
