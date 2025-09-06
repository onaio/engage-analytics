with unnested_careteams as (
  select
    id,
    status,
    jsonb_array_elements(participant::jsonb) as participant,
    _airbyte_emitted_at
  from {{ ref('stg_careteams') }}
)
select
  id,
  status,
  split_part(participant -> 'member' ->> 'reference', '/', 1) as participant_type,
  split_part(participant -> 'member' ->> 'reference', '/', 2) as participant_id,
  _airbyte_emitted_at
from unnested_careteams
