with base as (
    select
        id as fhir_id,
        resource,
        resource ->> 'id'            as resource_id,
        resource ->> 'status'        as status,
        resource ->> 'questionnaire' as questionnaire_canonical,

        
    -- ref_text looks like 'Patient/123' -> return '123'
    (regexp_replace(resource -> 'subject'   ->> 'reference', '^\w+/', ''))
 as patient_id,
        
    -- ref_text looks like 'Patient/123' -> return '123'
    (regexp_replace(resource -> 'encounter' ->> 'reference', '^\w+/', ''))
 as encounter_id,
        
    -- ref_text looks like 'Patient/123' -> return '123'
    (regexp_replace(resource -> 'author'    ->> 'reference', '^\w+/', ''))
 as author_id,

        
  (nullif(resource ->> 'authored', '')::timestamptz)
 as authored_ts,
        
  (nullif("lastUpdated", '')::timestamptz)
           as lastUpdated_ts,
        _airbyte_extracted_at
    from "data_warehouse"."airbyte"."questionnaire_response"
)
select * from base