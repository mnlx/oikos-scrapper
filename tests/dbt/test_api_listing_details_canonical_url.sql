select *
from {{ ref('mart_listing_details') }}
where canonical_url !~ '^https?://'
