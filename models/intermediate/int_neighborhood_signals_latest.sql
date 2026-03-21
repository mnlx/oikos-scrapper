select distinct on (signal_code, city, coalesce(neighborhood, ''))
  neighborhood_signal_id,
  city,
  state,
  neighborhood,
  geographic_scope,
  signal_category,
  signal_code,
  signal_name,
  source_name,
  source_type,
  publisher,
  source_url,
  reference_date,
  period_start,
  period_end,
  value_numeric,
  value_text,
  unit,
  priority,
  metadata_json,
  collected_at
from {{ ref('raw_neighborhood_signals') }}
order by signal_code, city, coalesce(neighborhood, ''), collected_at desc, neighborhood_signal_id desc
