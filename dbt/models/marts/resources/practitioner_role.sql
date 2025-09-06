{{ config(materialized='incremental', unique_key='id') }}

select
  spr.id,
  spr.active,
  spr.resourceType as resourcetype,
  -- first coding entry
  (spr.code::jsonb -> 'coding' -> 0 ->> 'code')    as coding_code,
  (spr.code::jsonb -> 'coding' -> 0 ->> 'display') as coding_display,
  {{ find_resource_id('spr.organization') }}  as organization_id,
  {{ find_resource_id('spr.practitioner') }}  as practitioner_id,
  (spr.meta::jsonb ->> 'lastUpdated') as meta_lastupdated,
  -- include the next line only if you actually want this column:
  -- (spr.meta::jsonb ->> 'source')       as meta_source,
  spr._airbyte_emitted_at
from {{ ref('stg_practitioner_roles') }} spr
{% if is_incremental() %}
where spr._airbyte_emitted_at > (
  select coalesce(max(_airbyte_emitted_at), '1900-01-01')
  from {{ this }}
)
{% endif %}
