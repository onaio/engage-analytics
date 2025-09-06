{{ config(materialized='incremental', unique_key='id') }}

with
codes as (
  {{ resource_level_2_extraction(
        'stg_organizations',
        ['type']
  ) }}
)

select
  o.id,
  o.name,
  o.active,
  c.*
from {{ ref('stg_organizations') }} as o
left join codes c on c.resource_id = o.id 
{% if is_incremental() %}
where o._airbyte_emitted_at > (
  select coalesce(max(_airbyte_emitted_at),'1900-01-01') from {{ this }}
)
{% endif %}
