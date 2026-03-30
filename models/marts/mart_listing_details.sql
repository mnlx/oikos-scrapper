with roots as (
  select
    i.id as ingestion_id,
    i.offering_hash,
    i.depth as root_depth
  from {{ source('app', 'raw_listing_ingestions') }} as i
  where i.depth = 0
),
source_lookup as (
  select
    id as source_id,
    name as source_name
  from {{ source('app', 'sources') }}
),
crawl_summary as (
  select
    external_id,
    source_code,
    max(depth) as max_depth_crawled,
    count(*) as crawled_pages
  from {{ source('app', 'raw_listing_ingestions') }}
  group by 1, 2
)
select
  b.offering_hash,
  b.source_code,
  coalesce(sl.source_name, b.source_code) as source_name,
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
  b.geocode_provider,
  b.geocode_query,
  b.geocode_confidence,
  b.geocode_status,
  b.geocode_payload,
  b.geocoded_at,
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
  b.listing_created_at,
  b.listing_updated_at,
  coalesce(b.image_uris, '[]'::jsonb) as image_uris,
  b.asset_links,
  b.screenshot_uri,
  b.html_uri,
  b.metadata_uri,
  b.text_html,
  coalesce(cs.max_depth_crawled, roots.root_depth, 0) as max_depth_crawled,
  coalesce(cs.crawled_pages, 1) as crawled_pages,
  b.parsed_at
from {{ source('app', 'int_listings_deduped') }} as b
left join source_lookup as sl
  on b.source_id = sl.source_id
left join roots
  on b.ingestion_id = roots.ingestion_id
left join crawl_summary as cs
  on b.external_id = cs.external_id
 and b.source_code = cs.source_code
