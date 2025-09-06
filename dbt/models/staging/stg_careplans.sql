{{ config(materialized='incremental', unique_key='id') }}

{% set level_1_keys = ['id','meta','subject','title','author','description','resourceType','intent','created','period','identifier','status','activity','instantiatesCanonical'] %}
{% set nested_keys   = [] %}

{{ resource_level_1_extraction(level_1_keys, nested_keys) }},
_airbyte_extracted_at as _airbyte_emitted_at
from {{ source('engage_dataset','care_plan') }}

{% if is_incremental() %}
where _airbyte_extracted_at > (select coalesce(max(_airbyte_emitted_at),'1900-01-01') from {{ this }})
{% endif %}
