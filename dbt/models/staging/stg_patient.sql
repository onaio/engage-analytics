{{ config(materialized='incremental', unique_key='id') }}

{% set level_1_keys = [
  'id','meta','gender','resourceType','birthDate','managingOrganization',
  'deceasedBoolean','active','text','identifier'
] %}
{% set nested_keys = [] %}

{{ resource_level_1_extraction(level_1_keys, nested_keys) }},
((resource::jsonb -> 'telecom')::jsonb->>0)::jsonb ->> 'value'  as telecom_value,
((resource::jsonb -> 'generalPractitioner')::jsonb->>0)::jsonb ->> 'reference' as generalPractitioner_reference,
((resource::jsonb -> 'name')::jsonb->>0)::jsonb ->> 'given' as name_given,
((resource::jsonb -> 'name')::jsonb->>0)::jsonb ->> 'family' as name_family,
((resource::jsonb -> 'identifier')::jsonb->>0)::jsonb -> 'period' ->> 'start' as period_start,
_airbyte_extracted_at as _airbyte_emitted_at
from {{ source('engage_dataset','patient') }}

{% if is_incremental() %}
  where _airbyte_extracted_at > (select max(_airbyte_emitted_at) from {{ this }})
{% endif %}
