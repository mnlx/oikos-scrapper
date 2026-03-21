select *
from {{ ref('api_listing_details') }}
where canonical_url !~ '^https?://'
