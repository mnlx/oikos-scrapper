with expanded as (
  select
    offering_hash,
    source_code,
    screenshot_uri,
    jsonb_array_elements_text(image_uris) as image_uri
  from {{ ref('api_listing_details') }}
)
select
  offering_hash,
  source_code,
  'image' as media_type,
  image_uri as media_uri
from expanded

union all

select
  offering_hash,
  source_code,
  'screenshot' as media_type,
  screenshot_uri as media_uri
from {{ ref('api_listing_details') }}
where screenshot_uri is not null
