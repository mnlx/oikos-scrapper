-- int_listings_deduped is now a physical table written by the Python pipeline.
-- This model enriches it with first/last seen timestamps from ingestion history
-- and source name from the sources table.

with source_lookup as (
  select
    id as source_id,
    name as source_name
  from {{ source('app', 'sources') }}
),

offering_history as (
  select
    offering_hash,
    min(discovered_at)                                   as first_seen_at,
    max(last_seen_at)                                    as last_seen_at,
    bool_or(
      asset_links is not null
      and jsonb_array_length(asset_links) > 0
    )                                                    as has_asset_links
  from {{ source('app', 'raw_listing_ingestions') }}
  where offering_hash is not null
  group by 1
)

select
  d.offering_hash,
  d.source_id,
  d.source_code,
  coalesce(sl.source_name, d.source_code)               as source_name,
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
  d.text_html,
  d.broker_name,
  d.published_at,
  d.listing_created_at,
  d.listing_updated_at,
  d.image_uris                                          as raw_image_uris,
  d.screenshot_uri,
  oh.first_seen_at,
  oh.last_seen_at,
  oh.last_seen_at                                       as last_scraped_at,
  oh.has_asset_links,
  oh.last_seen_at >= now() - interval '45 days'         as is_active
from {{ source('app', 'int_listings_deduped') }} as d
join offering_history as oh using (offering_hash)
left join source_lookup as sl
  on d.source_id = sl.source_id
