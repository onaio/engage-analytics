select
    id as fhir_id,
    resource,
    resource ->> 'id'                                 as resource_id,
    resource ->> 'status'                             as status,
    resource -> 'class' ->> 'code'                    as class_code,
    resource -> 'type'  -> 0 -> 'coding' -> 0 ->> 'code' as type_code,
    resource -> 'type'  -> 0 -> 'coding' -> 0 ->> 'display' as type_display,
    
    -- ref_text looks like 'Patient/123' -> return '123'
    (regexp_replace(resource -> 'subject' ->> 'reference', '^\w+/', ''))
 as patient_id,
    
    -- ref_text looks like 'Patient/123' -> return '123'
    (regexp_replace(resource -> 'participant' -> 0 -> 'individual' ->> 'reference', '^\w+/', ''))
 as practitioner_id,
    
    -- Handle ISO strings from FHIR meta/fields safely
    (resource ->> 'period' ->> 'start'::timestamptz)
 as period_start,
    
    -- Handle ISO strings from FHIR meta/fields safely
    (resource ->> 'period' ->> 'end'::timestamptz)
   as period_end,
    
    -- Handle ISO strings from FHIR meta/fields safely
    (lastUpdated::timestamptz)
                       as lastUpdated_ts,
    _airbyte_extracted_at
from "data_warehouse"."airbyte"."encounter"