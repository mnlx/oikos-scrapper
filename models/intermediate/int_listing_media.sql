with base as (
  select
    ingestion_id,
    asset_type,
    asset_uri,
    content_type,
    row_number() over (
      partition by ingestion_id, asset_type
      order by asset_id asc, raw_listing_asset_id asc
    ) as media_rank
  from (
    select
      id as raw_listing_asset_id,
      ingestion_id,
      asset_id,
      asset_type,
      asset_uri,
      content_type,
      is_scrapped
    from {{ source('app', 'raw_listing_assets') }}
  ) as raw_listing_assets
  where is_scrapped
),
images as (
  select
    ingestion_id,
    jsonb_agg(asset_uri order by media_rank) as image_uris
  from base
  where asset_type = 'image'
  group by 1
),
asset_counts as (
  select
    ingestion_id,
    count(*) as scrapped_asset_count
  from base
  group by 1
)
select
  b.ingestion_id,
  coalesce(i.image_uris, '[]'::jsonb) as image_uris,
  coalesce(a.scrapped_asset_count, 0) as scrapped_asset_count
from {{ source('app', 'raw_listings') }} as b
left join images as i using (ingestion_id)
left join asset_counts as a using (ingestion_id)
