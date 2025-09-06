{{ config(materialized='incremental', unique_key='id') }}

select
  s.id,
  {{ find_resource_id('s.organization') }}                 as organization_id,
  {{ find_resource_id('(s.location::jsonb -> 0)') }}       as location_id,   -- first array entry
  s._airbyte_emitted_at
from {{ ref('stg_organization_affiliations') }} s
{% if is_incremental() %}
where s._airbyte_emitted_at > (
  select coalesce(max(_airbyte_emitted_at),'1900-01-01') from {{ this }}
)
{% endif %}
