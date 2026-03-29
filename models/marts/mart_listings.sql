{{ config(materialized='table') }}

-- Deduplicated listings table for the API.
-- One row per offering_hash (unique listing), with image aggregation
-- from actually-scraped assets and temporal tracking from all ingestions.
with scraped_images as (
  -- Aggregate scraped image URLs per offering hash, across all ingestions.
  -- asset_url is the original HTTP URL (browser-safe), ordered by asset_id for stability.
  select
    i.offering_hash,
    jsonb_agg(a.asset_url order by a.asset_id)           as image_uris,
    count(*)                                             as image_count
  from {{ source('app', 'raw_listing_assets') }} as a
  join {{ source('app', 'raw_listing_ingestions') }} as i
    on a.ingestion_id = i.id
  where a.asset_type = 'image'
    and a.is_scrapped
    and i.offering_hash is not null
    and a.asset_url is not null
  group by 1
)

select
  -- Stable integer ID derived from offering_hash for API compatibility.
  abs(hashtext(d.offering_hash))                         as id,
  d.offering_hash,
  d.source_id,
  d.source_code,
  d.source_name,
  d.external_id,
  d.canonical_url,
  d.title,
  d.transaction_type,
  d.property_type,
  d.city,
  d.state,
  d.neighborhood,
  d.address,
  d.latitude,
  d.longitude,
  d.geocode_provider,
  d.geocode_query,
  d.geocode_confidence,
  d.geocode_status,
  d.geocode_payload,
  d.geocoded_at,
  d.price_sale,
  d.price_rent,
  d.condo_fee,
  d.iptu,
  d.bedrooms,
  d.bathrooms,
  d.parking_spaces,
  d.area_m2,
  d.description,
  d.broker_name,
  d.published_at,
  d.listing_created_at,
  d.listing_updated_at,
  -- Prefer scraped asset URLs; fall back to raw image URIs from listing parse
  coalesce(si.image_uris, d.raw_image_uris, '[]'::jsonb) as image_uris,
  coalesce(si.image_count, 0)                            as image_count,
  d.has_asset_links,
  d.screenshot_uri,
  d.raw_payload,
  d.first_seen_at,
  d.last_seen_at,
  d.last_scraped_at,
  d.is_active
from {{ ref('int_listings_deduped') }} as d
left join scraped_images as si using (offering_hash)
