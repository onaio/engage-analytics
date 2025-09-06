{{ config(materialized='incremental', unique_key='id') }}

{% set level_1_keys = ['id','meta','active','identifier','resourceType'] %}
{% set nested_keys = ['name','telecom'] %}

{{ resource_level_1_extraction(level_1_keys, nested_keys) }},
_airbyte_extracted_at as _airbyte_emitted_at
from {{ source('engage_dataset','practitioner') }}

{% if is_incremental() %}
  where _airbyte_extracted_at > (select max(_airbyte_emitted_at) from {{ this }})
{% endif %}
