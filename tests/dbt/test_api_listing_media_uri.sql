select *
from {{ ref('mart_listing_media') }}
where media_uri !~ '^s3://'
