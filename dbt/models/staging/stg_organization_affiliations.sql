{{ config(materialized='incremental', unique_key='id') }}

{% set level_1_keys = ['id','meta','active','resourceType','organization','identifier'] %}
{% set nested_keys   = ['location'] %}

{{ resource_level_1_extraction(level_1_keys, nested_keys) }},
_airbyte_extracted_at as _airbyte_emitted_at
from {{ source('engage_dataset','organization_affiliation') }}

{% if is_incremental() %}
where _airbyte_extracted_at > (select coalesce(max(_airbyte_emitted_at),'1900-01-01') from {{ this }})
{% endif %}
