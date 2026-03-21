with roots as (
  select
    i.ingestion_id,
    i.offering_hash,
    i.depth as root_depth
  from {{ ref('raw_listing_ingestions') }} as i
  where i.depth = 0
),
crawl_summary as (
  select
    external_id,
    source_code,
    max(depth) as max_depth_crawled,
    count(*) as crawled_pages
  from {{ ref('raw_listing_ingestions') }}
  group by 1, 2
)
select
  b.offering_hash,
  b.source_code,
  b.source_name,
  b.external_id,
  b.canonical_url,
  b.title,
  b.transaction_type,
  b.property_type,
  b.city,
  b.state,
  b.neighborhood,
  b.address,
  b.latitude,
  b.longitude,
  b.price_sale,
  b.price_rent,
  b.condo_fee,
  b.iptu,
  b.bedrooms,
  b.bathrooms,
  b.parking_spaces,
  b.area_m2,
  b.description,
  b.broker_name,
  b.published_at,
  coalesce(m.image_uris, b.image_uris, '[]'::jsonb) as image_uris,
  b.asset_links,
  b.screenshot_uri,
  b.html_uri,
  b.metadata_uri,
  coalesce(m.scrapped_asset_count, 0) as scrapped_asset_count,
  coalesce(cs.max_depth_crawled, roots.root_depth, 0) as max_depth_crawled,
  coalesce(cs.crawled_pages, 1) as crawled_pages,
  b.raw_payload,
  b.parsed_at
from {{ ref('raw_listings') }} as b
left join {{ ref('int_listing_media') }} as m
  on b.ingestion_id = m.ingestion_id
left join roots
  on b.ingestion_id = roots.ingestion_id
left join crawl_summary as cs
  on b.external_id = cs.external_id
 and b.source_code = cs.source_code
