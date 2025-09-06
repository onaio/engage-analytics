{{ config(materialized='incremental', unique_key='id') }}

{% set level_1_keys = ['id','meta','description','partOf','resourceType','status','physicalType','name'] %}
{% set nested_keys   = ['alias','identifier'] %}

with previous_extraction as (
  {{ resource_level_1_extraction(level_1_keys, nested_keys) }},
  _airbyte_extracted_at as _airbyte_emitted_at
  from {{ source('engage_dataset','location') }}
)
select
  *,
  partof::jsonb -> 'display'   as parent_name,
  {{ find_resource_id('partof') }} as parent_id,
  partof::jsonb -> 'reference' as parent_reference
from previous_extraction
{% if is_incremental() %}
where _airbyte_emitted_at > (select coalesce(max(_airbyte_emitted_at),'1900-01-01') from {{ this }})
{% endif %}
