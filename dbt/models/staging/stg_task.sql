{{ config(materialized='incremental', unique_key='id') }}

{% set level_1_keys = [
  'id','meta','authoredOn','basedOn','statusReason','reasonReference','executionPeriod',
  'for','code','owner','description','resourceType','intent','priority','lastModified',
  'requester','status','reasonCode'
] %}
{% set nested_keys = [] %}

{{ resource_level_1_extraction(level_1_keys, nested_keys) }},
_airbyte_extracted_at as _airbyte_emitted_at
from {{ source('engage_dataset','task') }}

{% if is_incremental() %}
  where _airbyte_extracted_at > (select max(_airbyte_emitted_at) from {{ this }})
{% endif %}
