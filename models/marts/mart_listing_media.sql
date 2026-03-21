with assets as (
  select
    l.offering_hash,
    a.source_code,
    a.asset_type as media_type,
    a.asset_uri as media_uri
  from {{ ref('raw_listing_assets') }} as a
  join {{ ref('raw_listings') }} as l
    on a.ingestion_id = l.ingestion_id
  where a.is_scrapped
)
select
  offering_hash,
  source_code,
  media_type,
  media_uri
from assets

union all

select
  offering_hash,
  source_code,
  'screenshot' as media_type,
  screenshot_uri as media_uri
from {{ ref('mart_listing_details') }}
where screenshot_uri is not null
