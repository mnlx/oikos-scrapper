-- Latest parsed record per offering_hash, with first/last seen tracked
-- across all ingestion events.
with latest_listing as (
  select distinct on (offering_hash)
    *
  from {{ source('app', 'raw_listings') }}
  where offering_hash is not null
  order by offering_hash, parsed_at desc nulls last, ingestion_id desc
),
source_lookup as (
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
  ll.offering_hash,
  ll.source_id,
  ll.source_code,
  coalesce(sl.source_name, ll.source_code)               as source_name,
  ll.external_id,
  ll.canonical_url,
  ll.title,
  ll.transaction_type,
  ll.property_type,
  ll.city,
  ll.state,
  ll.neighborhood,
  ll.address,
  ll.latitude,
  ll.longitude,
  ll.geocode_provider,
  ll.geocode_query,
  ll.geocode_confidence,
  ll.geocode_status,
  ll.geocode_payload,
  ll.geocoded_at,
  ll.price_sale,
  ll.price_rent,
  ll.condo_fee,
  ll.iptu,
  ll.bedrooms,
  ll.bathrooms,
  ll.parking_spaces,
  ll.area_m2,
  ll.description,
  ll.broker_name,
  ll.published_at,
  ll.listing_created_at,
  ll.listing_updated_at,
  ll.image_uris                                          as raw_image_uris,
  ll.screenshot_uri,
  ll.raw_payload,
  oh.first_seen_at,
  oh.last_seen_at,
  oh.last_seen_at                                        as last_scraped_at,
  oh.has_asset_links,
  oh.last_seen_at >= now() - interval '45 days'          as is_active
from latest_listing as ll
join offering_history as oh using (offering_hash)
left join source_lookup as sl
  on ll.source_id = sl.source_id
