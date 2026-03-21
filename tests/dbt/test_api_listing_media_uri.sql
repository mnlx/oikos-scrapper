select *
from {{ ref('api_listing_media') }}
where media_uri !~ '^s3://'
