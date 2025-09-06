
{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

{% set level_1_keys=['id', 'meta', 'name', 'active', 'identifier', 'resourceType']%}

{% set nested_keys=['type']%} 
    
    {{resource_level_1_extraction(level_1_keys, nested_keys)}},
     _airbyte_extracted_at as _airbyte_emitted_at 

    from {{ source("engage_dataset", "organization") }}


{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
   WHERE _airbyte_extracted_at > (SELECT max(_airbyte_emitted_at) FROM {{ this }})
{% endif %}
